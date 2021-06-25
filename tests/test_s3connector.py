import json
from sys import prefix
import pytest

# from botocore.client import 
from botocore import exceptions as boto_exceptions
from s3_connector_lib import S3Connector

TEST_BUCKET = 'test-bucket'
JSON_FILE = 'example.json'
JSON_FILE_CONTENT = {'a': 'value'}
SUB_FOLDER = 'sub-folder/'
SUB_FOLDER_FILE = 'sub-folder/inner_file.txt'
S3_URI = f's3://{TEST_BUCKET}/{SUB_FOLDER}'


class TestS3Connector():

    @pytest.fixture
    def s3_test_setup(self, s3_conn: S3Connector):
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

    def test_base_s3_uri(self, s3_conn: S3Connector, s3_test_setup):

        try:
            s3_conn.base_s3_uri = S3_URI
        except Exception as exc:
            assert False, f'Encountered Exception while trying to set base uri to: {S3_URI}, {s3_conn.list_folder_contents(delimiter="")=}'

    def test_bucket(self, s3_conn: S3Connector, s3_test_setup):
        assert s3_conn.bucket == TEST_BUCKET
        with pytest.raises(boto_exceptions.ClientError):
            s3_conn.bucket = 'non-existant-bucket'
        NEW_BUCKET = 'new-bucket'
        s3_conn.create_bucket(NEW_BUCKET)
        try:
            s3_conn.bucket = NEW_BUCKET
        except Exception as exc:
            assert False, f'While setting bucket property to {NEW_BUCKET} the following exception was raised:\n{exc}'

    def test_check_bucket(self, s3_conn: S3Connector, s3_test_setup):
        response = s3_conn.check_bucket()
        assert response and response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode') == 200

    def test_check_file(self, s3_conn: S3Connector, s3_test_setup):
        response = s3_conn.check_file(JSON_FILE)
        assert response and response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode') == 200

    def test_check_folder(self, s3_conn: S3Connector, s3_test_setup):
        response = s3_conn.check_folder(SUB_FOLDER)
        assert response and response.get('found_in')

    def test_check_object(self, s3_conn: S3Connector, s3_test_setup):
        response = s3_conn.check_object(JSON_FILE)
        assert response and response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode') == 200

    def test_compose_s3_uri(self):
        uri = S3Connector.compose_s3_uri(
            bucket=TEST_BUCKET,
            key=JSON_FILE
        )
        assert uri == f's3://{TEST_BUCKET}/{JSON_FILE}'

    def test_compose_s3_url(self):
        url = S3Connector.compose_s3_url(
            bucket=TEST_BUCKET,
            key=JSON_FILE
        )
        assert url == f'https://{TEST_BUCKET}.s3.amazonaws.com/{JSON_FILE}'

    def test_copy_file(self, s3_conn: S3Connector, s3_test_setup):
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

    def test_copy_folder(self, s3_conn: S3Connector, s3_test_setup):
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

    def test_create_bucket(self, s3_conn):
        bucket_name = 'created-bucket'
        s3_conn.create_bucket(bucket_name)
        assert bucket_name in s3_conn.list_buckets()

    def test_cwd(self, s3_conn: S3Connector, s3_test_setup):
        assert s3_conn.cwd == ''
        with pytest.raises(Exception):
            s3_conn.cwd = 'notavalidpath'
        s3_conn.cwd = SUB_FOLDER
        assert s3_conn.cwd == SUB_FOLDER

    def test_decompose_s3_uri(self, s3_conn: S3Connector):
        with pytest.raises(Exception):
            s3_conn.decompose_s3_uri('badlink')
        bucket, path = s3_conn.decompose_s3_uri(S3_URI)
        assert bucket and path
        assert bucket == TEST_BUCKET
        assert path == SUB_FOLDER

    def test_delete_bucket(self, s3_conn: S3Connector, s3_test_setup):
        s3_conn.create_bucket('delete-this')
        response = s3_conn.delete_bucket('delete-this')
        assert response.get(
            'ResponseMetadata', {}
        ).get('HTTPStatusCode') == 204
        with pytest.raises(boto_exceptions.ClientError):
            s3_conn.delete_bucket('delete-this')

    def test_delete_file(self, s3_conn: S3Connector, s3_test_setup):
        response = s3_conn.delete_file(JSON_FILE)
        assert response and response.get(
            'ResponseMetadata', {}
        ).get('HTTPStatusCode') == 204

    def test_delete_folder(self, s3_conn: S3Connector, s3_test_setup):
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

    @pytest.mark.skip('Not implemented')
    def test_download_folder(self, s3_conn: S3Connector, s3_test_setup):
        pass

    @pytest.mark.skip('Not implemented')
    def test_download_to_file(self, s3_conn: S3Connector, s3_test_setup):
        pass

    @pytest.mark.skip('Not implemented')
    def test_download_to_filelike(self, s3_conn: S3Connector, s3_test_setup):
        pass

    def test_get_file_link(self, s3_conn: S3Connector, s3_test_setup):
        assert s3_conn.compose_s3_uri(
            bucket=TEST_BUCKET, key=JSON_FILE
        ) == s3_conn.get_file_link(key=JSON_FILE)
        assert s3_conn.compose_s3_url(
            bucket=s3_conn.bucket, key=JSON_FILE
        ) == s3_conn.get_file_link(key=JSON_FILE, get_url=True)

    def test_is_file(self):
        assert not S3Connector.is_file('a_folder/')
        assert S3Connector.is_file('not_a_folder')

    def test_is_valid_s3_link(self, s3_conn: S3Connector, s3_test_setup):
        assert s3_conn.is_valid_s3_link(S3_URI)
        assert not s3_conn.is_valid_s3_link('baduri')

    def test_list_buckets(self, s3_conn: S3Connector, s3_test_setup):
        buckets = s3_conn.list_buckets()
        assert buckets == [TEST_BUCKET]
        s3_conn.create_bucket('another')
        s3_conn.create_bucket('and-another')
        buckets = s3_conn.list_buckets()
        assert sorted(buckets) == sorted(
            [TEST_BUCKET, 'another', 'and-another']
        )

    def test_list_files(self, s3_conn: S3Connector, s3_test_setup):
        files = s3_conn.list_files('')
        assert files == [JSON_FILE]
        files = s3_conn.list_files(path=SUB_FOLDER)
        assert files == [SUB_FOLDER_FILE]

    def test_list_folder_contents(
        self,
        s3_conn: S3Connector,
        s3_test_setup
    ):
        files = s3_conn.list_folder_contents('')
        assert sorted(files) == sorted([JSON_FILE, SUB_FOLDER])
        files = s3_conn.list_folder_contents(path=SUB_FOLDER)
        assert files == [SUB_FOLDER_FILE]

    def test_list_folders(self, s3_conn: S3Connector, s3_test_setup):
        folders = s3_conn.list_folders('')
        assert folders == [SUB_FOLDER]
        folders = s3_conn.list_folders(SUB_FOLDER)
        assert folders == []

    def test_list_objects(self, s3_conn: S3Connector, s3_test_setup):
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

    def test_read_json(self, s3_conn: S3Connector, s3_test_setup):
        json_content = s3_conn.read_json(JSON_FILE)
        assert json_content == JSON_FILE_CONTENT

    @pytest.mark.skip('Not implemented')
    def test_upload_file(self, s3_conn, s3_test_setup):
        pass

    @pytest.mark.skip('Not implemented')
    def test_upload_folder(self, s3_conn, s3_test_setup):
        pass

    def test_wait_for_bucket(self, s3_conn: S3Connector, s3_test_setup):
        s3_conn.create_bucket('new-bucket')
        assert s3_conn.wait_for_bucket('new-bucket')

    def test_waiters(self, s3_conn: S3Connector):
        assert sorted(
            [
                'bucket_exists',
                'bucket_not_exists',
                'object_exists',
                'object_not_exists'
            ]
        ) == sorted(s3_conn.waiters)

    def test_write_json(self, s3_conn: S3Connector, s3_test_setup):
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

    def test_write_to_file(self, s3_conn: S3Connector, s3_test_setup):
        file_content = json.dumps({'cont': 'ent'})
        filename = 'newfile.json'
        response = s3_conn.write_to_file(
            filename=filename, content=file_content
        )
        assert response
        assert file_content == json.dumps(s3_conn.read_json(filename))
