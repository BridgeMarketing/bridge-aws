import json
import os
import re
import pytest
import time

from botocore import exceptions as boto_exceptions
from s3 import S3
from tempfile import NamedTemporaryFile, TemporaryDirectory, mkdtemp

TEST_BUCKET = 'test-bucket'
JSON_FILE = 'example.json'
JSON_FILE_CONTENT = {'a': 'value'}
SUB_FOLDER = 'sub-folder/'
SUB_FOLDER_FILE = 'sub-folder/inner_file.txt'
S3_URI = f's3://{TEST_BUCKET}/{SUB_FOLDER}'


class TestS3():

    @pytest.fixture
    def s3_test_setup(self, s3_conn: S3):
        s3_conn.create_bucket(TEST_BUCKET)
        s3_conn.bucket = TEST_BUCKET
        s3_conn.write_json(
            target=JSON_FILE,
            content=JSON_FILE_CONTENT,
            bucket=TEST_BUCKET
        )
        s3_conn.write_to_file(
            filename=SUB_FOLDER_FILE,
            content=b'You want content? Here\'s some content'
        )
        yield

    def test_init(self, aws_creds):
        client = S3(
            aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_key=os.environ['AWS_SECRET_ACCESS_KEY']
        )

    def test_repr(self, s3_conn: S3):
        assert s3_conn.__repr__().startswith(
            '<s3.s3.S3 object at'
        ) and s3_conn.__repr__().endswith(
            'bucket= cwd=>'
        )

    def test_base_s3_uri(self, s3_conn: S3, s3_test_setup):

        try:
            s3_conn.base_s3_uri = S3_URI
        except Exception as exc:
            assert False, f'Encountered Exception while trying to set base uri to: {S3_URI}, {s3_conn.list_folder_contents(delimiter="")=}'
        assert s3_conn.base_s3_uri == S3_URI

    def test_bucket(self, s3_conn: S3, s3_test_setup):
        assert s3_conn.bucket == TEST_BUCKET
        with pytest.raises(boto_exceptions.ClientError):
            s3_conn.bucket = 'non-existant-bucket'
        NEW_BUCKET = 'new-bucket'
        s3_conn.create_bucket(NEW_BUCKET)
        try:
            s3_conn.bucket = NEW_BUCKET
        except Exception as exc:
            assert False, f'While setting bucket property to {NEW_BUCKET} the following exception was raised:\n{exc}'

    def test_check_bucket(self, s3_conn: S3, s3_test_setup):
        response = s3_conn.check_bucket()
        assert response and response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode') == 200

    def test_check_file(self, s3_conn: S3, s3_test_setup):
        response = s3_conn.check_file(JSON_FILE)
        assert response and response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode') == 200
        with pytest.raises(Exception):
            s3_conn.check_file(SUB_FOLDER)

    def test_check_folder(self, s3_conn: S3, s3_test_setup):
        response = s3_conn.check_folder(SUB_FOLDER)
        assert response and response.get('found_in')
        with pytest.raises(Exception):
            s3_conn.check_folder(JSON_FILE)

    def test_check_object(self, s3_conn: S3, s3_test_setup):
        response = s3_conn.check_object(JSON_FILE)
        assert response and response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode') == 200

    def test_compose_s3_uri(self):
        uri = S3.compose_s3_uri(
            bucket=TEST_BUCKET,
            key=JSON_FILE
        )
        assert uri == f's3://{TEST_BUCKET}/{JSON_FILE}'

    def test_compose_s3_url(self):
        url = S3.compose_s3_url(
            bucket=TEST_BUCKET,
            key=JSON_FILE
        )
        assert url == f'https://{TEST_BUCKET}.s3.amazonaws.com/{JSON_FILE}'

    def test_copy_file(self, s3_conn: S3, s3_test_setup):
        copy_from = 'example.json'
        copy_to = 'folder_test/example.copy.json'
        s3_conn.copy_file(
            copy_from=copy_from,
            copy_to=copy_to
        )
        found_objects = [
            thing.get('Key')
            for thing in
            s3_conn.list_objects('', delimiter='').get('Contents')
        ]
        assert copy_to in found_objects
        original = s3_conn.read_json(target=copy_from)
        copied = s3_conn.read_json(target=copy_to)
        assert original == copied

    def test_copy_folder(self, s3_conn: S3, s3_test_setup):
        other_sub = f'other-{SUB_FOLDER}'
        result = s3_conn.copy_folder(
            copy_from=SUB_FOLDER,
            copy_to=other_sub
        )
        assert sorted(
                [
                    content.split('/')[-1]
                    for content in
                    s3_conn.list_folder_contents(
                        path=SUB_FOLDER, delimiter=''
                    )
                ]
            ) == sorted(
                [
                    content.split('/')[-1]
                    for content in
                    s3_conn.list_folder_contents(
                        path=other_sub, delimiter=''
                    )
                ]
            )
        # NOTE: if the complexity of the subfolders increases
        # this may need to change, perhaps cut the prefix instead?

    def test_create_bucket(self, s3_conn: S3):
        bucket_name = 'created-bucket'
        s3_conn.create_bucket(bucket_name)
        assert bucket_name in s3_conn.list_buckets()
        bucket_name = 'created-bucket-the-second'
        s3_conn.create_bucket(
            bucket_name, location='loco', access_control='private'
        )
        assert bucket_name in s3_conn.list_buckets()

    def test_cwd(self, s3_conn: S3, s3_test_setup):
        assert s3_conn.cwd == ''
        with pytest.raises(Exception):
            s3_conn.cwd = 'notavalidpath/'
        s3_conn.cwd = SUB_FOLDER
        assert s3_conn.cwd == SUB_FOLDER

    def test_decompose_s3_uri(self, s3_conn: S3):
        with pytest.raises(Exception):
            s3_conn.decompose_s3_uri('badlink')
        bucket, path = s3_conn.decompose_s3_uri(S3_URI)
        assert bucket and path
        assert bucket == TEST_BUCKET
        assert path == SUB_FOLDER

    def test_delete_bucket(self, s3_conn: S3, s3_test_setup):
        s3_conn.create_bucket('delete-this')
        s3_conn.bucket = 'delete-this'
        response = s3_conn.delete_bucket()
        assert response.get(
            'ResponseMetadata', {}
        ).get('HTTPStatusCode') == 204
        with pytest.raises(boto_exceptions.ClientError):
            s3_conn.delete_bucket('delete-this')

    def test_delete_file(self, s3_conn: S3, s3_test_setup):
        response = s3_conn.delete_file(JSON_FILE)
        assert response and response.get(
            'ResponseMetadata', {}
        ).get('HTTPStatusCode') == 204

    def test_delete_folder(self, s3_conn: S3, s3_test_setup):
        starting_files = s3_conn.list_folder_contents(
            path=SUB_FOLDER,
            delimiter=''
        )
        response = s3_conn.delete_folder(target=SUB_FOLDER)
        assert len(starting_files) == len(response)
        for resp in response:
            assert resp.get(
                'ResponseMetadata', {}
            ).get('HTTPStatusCode') == 204

    def test_download_folder(self, s3_conn: S3, s3_test_setup):
        with pytest.raises(Exception):
            s3_conn.download_folder(JSON_FILE)
        with TemporaryDirectory(dir='./') as temp_dir:
            s3_conn.download_folder(
                target=SUB_FOLDER,
                local_path=temp_dir + '/'
            )
            assert os.path.exists(temp_dir + '/' + SUB_FOLDER_FILE)
            s3_conn.download_folder(
                target=SUB_FOLDER,
                local_path=temp_dir + '/inner-temp/'
            )
            assert os.path.exists(
                temp_dir + '/inner-temp/' + SUB_FOLDER_FILE
            )

    def test_download_to_file(self, s3_conn: S3, s3_test_setup):
        with NamedTemporaryFile() as temp_file:
            s3_conn.download_to_file(
                s3_target=SUB_FOLDER_FILE,
                local_target=temp_file.name
            )
            assert os.path.exists(temp_file.name)

    def test_download_to_filelike(self, s3_conn: S3, s3_test_setup):
        with NamedTemporaryFile() as temp_file:
            s3_conn.download_to_filelike(
                s3_target=SUB_FOLDER_FILE,
                filelike=temp_file
            )
            temp_file.seek(0)
            assert len(temp_file.readlines())

    def test_get_file_link(self, s3_conn: S3, s3_test_setup):
        assert s3_conn.compose_s3_uri(
            bucket=TEST_BUCKET, key=JSON_FILE
        ) == s3_conn.get_file_link(key=JSON_FILE)
        assert s3_conn.compose_s3_url(
            bucket=s3_conn.bucket, key=JSON_FILE
        ) == s3_conn.get_file_link(key=JSON_FILE, get_url=True)
        with pytest.raises(Exception):
            s3_conn.get_file_link(key='badkey')

    def test_is_file(self):
        assert not S3.is_file('a_folder/')
        assert S3.is_file('not_a_folder')

    def test_is_valid_s3_link(self, s3_conn: S3, s3_test_setup):
        assert s3_conn.is_valid_s3_link(S3_URI)
        assert not s3_conn.is_valid_s3_link('baduri')

    def test_list_buckets(self, s3_conn: S3, s3_test_setup):
        buckets = s3_conn.list_buckets()
        assert buckets == [TEST_BUCKET]
        s3_conn.create_bucket('another')
        s3_conn.create_bucket('and-another')
        buckets = s3_conn.list_buckets()
        assert sorted(buckets) == sorted(
            [TEST_BUCKET, 'another', 'and-another']
        )

    def test_list_files(self, s3_conn: S3, s3_test_setup):
        files = s3_conn.list_files('')
        assert files == [JSON_FILE]
        files = s3_conn.list_files(path=SUB_FOLDER)
        assert files == [SUB_FOLDER_FILE]

    def test_list_folder_contents(
        self,
        s3_conn: S3,
        s3_test_setup
    ):
        files = s3_conn.list_folder_contents('')
        assert sorted(files) == sorted([JSON_FILE, SUB_FOLDER])
        files = s3_conn.list_folder_contents(path=SUB_FOLDER)
        assert files == [SUB_FOLDER_FILE]

    def test_list_folders(self, s3_conn: S3, s3_test_setup):
        folders = s3_conn.list_folders('')
        assert folders == [SUB_FOLDER]
        folders = s3_conn.list_folders(SUB_FOLDER)
        assert folders == []

    def test_list_objects(self, s3_conn: S3, s3_test_setup):
        objs = s3_conn.list_objects()
        assert objs
        assert sorted(
            [
                obj.get('Key')
                for obj in
                objs.get('Contents', [])
            ]
        ) == sorted(
            [
                JSON_FILE, SUB_FOLDER_FILE
            ]
        )
        objs = s3_conn.list_objects(prefix=SUB_FOLDER)
        assert sorted(
            [
                obj.get('Key')
                for obj in
                objs.get('Contents', [])
            ]
        ) == sorted([ SUB_FOLDER_FILE])
        s3_conn._bucket = ''
        with pytest.raises(Exception):
            s3_conn.list_objects(
                prefix=SUB_FOLDER
            )

    def test_read_json(self, s3_conn: S3, s3_test_setup):
        json_content = s3_conn.read_json(JSON_FILE)
        assert json_content == JSON_FILE_CONTENT

    def test_upload_file(self, s3_conn: S3, s3_test_setup):
        with NamedTemporaryFile() as temp_file:
            temp_file_content = {'a': 'dictionry'}
            temp_file.write(json.dumps(temp_file_content).encode('utf-8'))
            temp_file.flush()
            s3_conn.upload_file(temp_file.name, local_path=temp_file.name)
            did_it_work = s3_conn.read_json(temp_file.name)
            assert temp_file_content == did_it_work

    def test_upload_folder(self, s3_conn: S3, s3_test_setup):
        with TemporaryDirectory() as temp_dir:
            folder_upload = 'folder_up/'
            temp_filename = f'{temp_dir}/this_should_not_exist.txt'
            temp_filename_contents = 'This really shouldn\'t exist'
            temp_filename_also = f'{temp_dir}/this_should_not_exist_either.txt'
            temp_filename_also_contents = 'This really shouldn\'t exist either'
            with open(temp_filename, 'w') as temp_file:
                temp_file.write(temp_filename_contents)
            with open(temp_filename_also, 'w') as temp_file:
                temp_file.write(temp_filename_also_contents)
            s3_conn.upload_folder(
                target=folder_upload,
                local_path=temp_dir
            )
            contents = s3_conn.list_folder_contents('/tmp', delimiter='')
            assert len(contents) == 2, contents
            assert temp_filename in contents
            assert temp_filename_also in contents
        with pytest.raises(FileNotFoundError):
            s3_conn.upload_folder(target='', local_path='notvalid/')
        with pytest.raises(Exception):
            s3_conn.upload_folder(
                target='', local_path='./tests/conftest.py'
            )

    def test_wait_for_bucket(self, s3_conn: S3, s3_test_setup):
        s3_conn.create_bucket('new-bucket')
        assert s3_conn.wait_for_bucket('new-bucket')

    def test_waiters(self, s3_conn: S3):
        assert sorted(
            [
                'bucket_exists',
                'bucket_not_exists',
                'object_exists',
                'object_not_exists'
            ]
        ) == sorted(s3_conn.waiters)

    def test_write_json(self, s3_conn: S3, s3_test_setup):
        json_content = {
            'some': 'json'
        }
        new_json = 'new_json.json'
        response = s3_conn.write_json(
            target=new_json,
            content=json_content
        )
        assert response
        assert json_content == s3_conn.read_json(target=new_json)

    def test_write_to_file(self, s3_conn: S3, s3_test_setup):
        file_content = json.dumps({'cont': 'ent'})
        filename = 'newfile.json'
        response = s3_conn.write_to_file(
            filename=filename, content=file_content
        )
        assert response
        assert file_content == json.dumps(s3_conn.read_json(filename))
