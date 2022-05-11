from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LocalWebserverAuth()

ID = '1BhyRHm1O4jHE7ICRzlLlYvThZ_nd_YTY'

def create_and_upload_file(file_name='text.txt', file_content='hey'):
    try:
        drive = GoogleDrive(gauth)

        # upload_file_list = ['2.csv']
        # for upload_file in upload_file_list:
        #     gfile = drive.CreateFile({'parents': [{'id': ID}]})
        #     # Read file and set it as the content of this instance.
        #     gfile.SetContentFile(upload_file)
        #     gfile.Upload()  # Upload the file.

        files = drive.ListFile({'q': f"'{ID}' in parents"}).GetList()

        return files

    except Exception as ex:
        return 'trouble'


def main():
    # print(create_and_upload_file()[0]['selfLink'])

    for key, val in enumerate(create_and_upload_file()[0]):
        print(key, val)


if __name__ == '__main__':
    main()

