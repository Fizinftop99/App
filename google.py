from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LocalWebserverAuth()


def create_and_upload_file(file_name='text.txt', file_content='hey'):
    try:
        drive = GoogleDrive(gauth)
        my_file = drive.CreateFile({'title': f'{file_name}'})
        my_file.SetContentString(file_content)
        my_file.Upload()
        return 'upload'

    except Exception as ex:
        return 'trouble'


def main():
    print(create_and_upload_file())


if __name__ == '__main__':
    main()
