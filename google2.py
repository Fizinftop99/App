import io
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Union, List, Optional

from aiocache import cached
from aiogram.types.mixins import Downloadable
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import (MediaFileUpload, HttpRequest,
                                  MediaIoBaseDownload)

from ..utils.yaml import get_cfg
from ..decorators.run_in_executor import run_in_executor
from ..exceptions import GoogleExplorerException
from ..exceptions.set_config_exception import SetConfigException
from ..models.file import File
from ..models.google_drive_methods import Methods

logger = logging.getLogger(__name__)
cfg = get_cfg('google')

GOOGLE_SERVICE_NAME = 'drive'
GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'
GOOGLE_DRIVE_API_VERSION = 'v3'
FOLDER_MIME = 'application/vnd.google-apps.folder'
folder_pattern = re.compile(r'folders\/(?P<folder_id>[-a-zA-Z0-9_]*)[?$]?')


class GoogleExplorer:
    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(
            filename=Path(cfg.get('json')).absolute(),
            scopes=[GOOGLE_DRIVE_SCOPE],
        )
        self.service = build(serviceName=GOOGLE_SERVICE_NAME,
                             version=GOOGLE_DRIVE_API_VERSION,
                             credentials=credentials,
                             cache_discovery=False)
        self.files = self.service.files()
        self.revisions = self.service.revisions()
        self.fields = ', '.join(File.__fields__)
        self.page_size = 100

    def _prepare_request(self, method_name, params: dict) -> HttpRequest:
        method = getattr(self.files, method_name)
        return method(**params)

    def _prepare_params(self, conditions: List[str]) -> dict:
        return {
            'pageSize': self.page_size,
            'fields': f"nextPageToken, files({self.fields})",
            'q': ' and '.join(conditions),
        }

    @staticmethod
    def _get_parents(parents: Union[list, str]):
        mod = {}
        if isinstance(parents, list):
            mod['parents'] = parents
        elif isinstance(parents, str):
            mod['parents'] = [parents]
        return mod

    @run_in_executor
    def _execute(self, request: HttpRequest):
        """
        Async executor

        :param request:
        :return: awaitable (coro)
        """
        return request.execute()

    async def _get_files(self, conditions: List[str]):
        """
        Get files by conditions

        :param conditions: list of conditions
        :return: list of File objects
        """
        # first execute
        params = self._prepare_params(conditions)
        request = self._prepare_request(Methods.LIST, params)
        results = await self._execute(request)
        files: list = results.get('files', [])
        if not files:
            return []

        # get pages
        while next_page := results.get('nextPageToken'):
            params.update(pageToken=next_page)
            request = self._prepare_request(Methods.LIST, params)
            results = await self._execute(request)
            files += results.get('files', [])

        # convert files
        return [File(**file_data) for file_data in files]

    async def get_folders(self,
                          id: str = None,  # noqa
                          parent: str = None,
                          name: str = None):
        conditions = [f"mimeType = '{FOLDER_MIME}'"]

        if id is not None:
            conditions.append(f"id = '{id}'")

        if parent is not None:
            conditions.append(f"'{parent}' in parents", )

        if name is not None:
            conditions.append(f"name = '{name}'")

        return await self._get_files(conditions)

    @cached(ttl=60 * 60 * 24, noself=True)
    async def get_folder(self,
                         id: str = None,  # noqa
                         parent: str = None,
                         name: str = None,
                         create: bool = False):
        logger.info(f"Getting folder id=`{id}`, "
                    f"name=`{name}`, parent=`{parent}`...")
        folders = await self.get_folders(id=id, parent=parent, name=name)
        if not folders:
            if create and id is None:
                return await self.create_folder(name=name, parents=parent)
            return None

        if len(folders) == 1:
            return folders[0]

        # todo add admin notification about double names!
        raise GoogleExplorerException(
            f'Дубликат папки `{name}` для родителя `{parent}`'
        )

    async def upload_file(self, file_path: Union[str, Path], name=None,
                          parents=None, description=None):
        """
        Upload file to GDrive

        :param file_path:
        :param name: custom name
        :param parents: parent id or parents id list
        :param description: file description
        :return:
        """
        logger.info(f"Uploading file name=`{name}`, parents=`{parents}`...")
        # get path
        if isinstance(file_path, str):
            file_path = Path(file_path).absolute()

        # define meta
        file_metadata = {'name': name or file_path.name}
        if parents is not None:
            file_metadata.update(self._get_parents(parents))
        if description is not None:
            file_metadata['description'] = description

        # prepare file
        media = MediaFileUpload(file_path, resumable=True)

        # prepare request and execute
        params = dict(body=file_metadata, media_body=media, fields=self.fields)
        request = self._prepare_request(Methods.CREATE, params)
        file_data = await self._execute(request)
        return File(**file_data)

    async def create_folder(self, name, parents=None):
        """
        Create GDrive folder

        :param name: name of folder to create
        :param parents: parent id or parents id list
        :return:
        """
        logger.info(f"Creating folder `{name}` for parents `{parents}`...")
        # define meta
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
        }
        if parents is not None:
            file_metadata.update(self._get_parents(parents))

        params = dict(body=file_metadata, fields=self.fields)
        request = self._prepare_request(Methods.CREATE, params)
        folder_data = await self._execute(request)
        if not folder_data:
            raise GoogleExplorerException("Can't create folder")

        return File(**folder_data)

    async def get_photos_date_folder(self, dt: datetime):
        """
        Get folder File object for requested date
        :param dt:
        :return:
        """
        photos_folder_url = cfg.get('photos_folder_url')
        if photos_folder_url is None:
            raise SetConfigException('Please set `photos_folder_url` in config')
        photos_root = self.get_id_from_url(photos_folder_url)

        year_folder = await self.get_folder(parent=photos_root,
                                            name=str(dt.year),
                                            create=True)
        month_folder = await self.get_folder(parent=year_folder.id,
                                             name=dt.strftime("%B"),
                                             create=True)
        day_folder = await self.get_folder(parent=month_folder.id,
                                           name=str(dt.day),
                                           create=True)
        return day_folder

    async def save_from_telegram(self,
                                 obj: Downloadable,
                                 folder: Union[File, str],
                                 name: str,
                                 description=None,
                                 rewrite=True):
        if isinstance(folder, File):
            folder_id = folder.id
        else:
            folder_id = folder

        tmp = NamedTemporaryFile('w+t', delete=False)
        try:
            # download from telegram to tmp file
            await obj.download(tmp.name)

            # Windows issue fix
            #   we need to close the file for open it in the next step
            tmp.seek(0)
            tmp.close()

            # upload temp file to Google Drive
            if rewrite:
                return await self.update_or_create_file(
                    file_path=tmp.name,
                    name=name,
                    parent=folder_id,
                    description=description,
                )
            else:
                return await self.upload_file(
                    file_path=tmp.name,
                    name=name,
                    parents=folder_id,
                    description=description,
                )

        finally:
            # close and remove temp file from system
            tmp.close()
            os.unlink(tmp.name)

    async def download_file(self, file_id, path):
        return await self._download_file(file_id, path)

    @run_in_executor
    def _download_file(self, file_id, path):
        request = self.files.export_media(fileId=file_id)
        fh = io.FileIO(path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        return done

    async def update_description(self, file_id, description):
        params = {'body': {'description': description}, 'fileId': file_id}
        request = self._prepare_request(Methods.UPDATE, params)
        return await self._execute(request)

    async def update_file(self, file_id, file_path, description=None):
        media_body = MediaFileUpload(file_path, resumable=True)
        params = {
            'media_body': media_body,
            'fileId': file_id,
            'fields': self.fields,
        }
        if description is not None:
            params['body'] = {'description': description}
        request = self._prepare_request(Methods.UPDATE, params)
        file_data = await self._execute(request)
        return File(**file_data)

    async def get_file(self, name, parent):
        conditions = [
            f"name = '{name}'",
            f"'{parent}' in parents",
        ]
        files = await self._get_files(conditions)
        if files:
            return files[0]

    async def update_or_create_file(self,
                                    file_path,
                                    name,
                                    parent,
                                    description=None):
        conditions = [
            f"name = '{name}'",
            f"'{parent}' in parents",
        ]
        files = await self._get_files(conditions)
        if files:
            file = files[0]
            new_file = await self.update_file(file_id=file.id,
                                              file_path=file_path,
                                              description=description)
            if file.headRevisionId is not None:
                await self.keep_revision(file.id, file.headRevisionId)
            return new_file
        return await self.upload_file(file_path=file_path,
                                      name=name,
                                      parents=parent,
                                      description=description)

    async def keep_revision(self, file_id, revision_id):
        params = {
            'fileId': file_id,
            'revisionId': revision_id,
            'body': {"keepForever": True},
        }
        request = self.revisions.update(**params)
        await self._execute(request)

    @staticmethod
    def get_id_from_url(url: str) -> Optional[str]:

        result = re.search(folder_pattern, url)
        if result is None:
            return None
        return result.group('folder_id')


explorer = GoogleExplorer()
