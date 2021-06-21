# TODO: docstring
import json
from typing import Union
import boto3

class S3Connector():
    # TODO: docstring

    def __init__(
        self,
        s3_uri_base: str = '',
        aws_secret_key: str = '',
        aws_access_key: str = ''
    ) -> None:
        # TODO: docstring
        if aws_access_key and aws_secret_key:
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            # use default configs at ~/.aws/config
            self.s3 = boto3.client('s3')
        self.events = self.s3.meta.events
        self.base_uri = s3_uri_base
        self._bucket, self._path_prefix = self.decompose_s3_uri(s3_uri_base)
        self.default_delimiter = '/'

    def __repr__(self) -> str:
        base = super().__repr__()[1:-1]
        # base = base[1:-1]
        return f'<{base} bucket={self.bucket} cwd={self.cwd}>'

    @property
    def waiters(self) -> list[str]:
        # TODO: docstring
        return self.s3.waiter_names

    @property
    def bucket(self) -> str:
        # TODO: docstring
        return self._bucket

    @bucket.setter
    def bucket(self, bucket_name: str) -> None:
        # TODO: docstring
        self.s3.head_bucket(Bucket=bucket_name)
        # head_bucket throws an exception if we don't have
        # access, or it doesn't exist, so if no exception
        # was thrown we can move forward.
        # TODO: doc exceptions raised
        self._bucket = bucket_name

    @property
    def cwd(self) -> str:
        return self._path_prefix

    @cwd.setter
    def cwd(self, path: str) -> None:
        # TODO: docstring
        # NOTE: this requires we have a bucket set
        result = self.s3.head_object(
            Key=path,
            Bucket=self.bucket
        )
        print(result)
        if result:
            self._path_prefix = path
        else:
            raise Exception('Path not found in current bucket') # TODO: file not found exception?

    @property
    def base_s3_uri(self):
        return self.compose_s3_uri(
            bucket=self.bucket,
            key=self.cwd
        )

    @base_s3_uri.setter
    def base_s3_uri(self, s3_uri: str = '', bucket: str = '', path: str = '') -> None:
        if s3_uri and not (bucket and path):
            self.bucket, self.cwd = self.decompose_s3_uri(s3_uri)
        elif (bucket and path) and not s3_uri:
            self.bucket, self.cwd = bucket, path
        else:
            raise Exception('Must provide either an s3_uri, or ')

    def create_bucket(
        self,
        name: str,
        location: str = '',
        access_control: str = '',
        **kwargs
    ) -> dict:
        # TODO: docstring
        if location:
            kwargs['CreateBucketConfiguration'] = {
                'LocationConstraint': location
            }
        if access_control:
            kwargs['ACL'] = access_control
        return self.s3.create_bucket(
            Bucket=name,
            **kwargs
        )

    def delete_bucket(self, bucket: str = ''):
        # TODO: docstring
        response = self.s3.delete_bucket(
            Bucket=bucket or self.bucket
        )
        if not bucket:
            # we just deleted our current bucket
            # so let's remove that, set the hidden
            # field so it doesn't try to do the check
            self._bucket = ''

    def wait_for_bucket(
        self,
        bucket_name: str,
        expected_owner: str = None,
        waiter_delay: int = None,
        waiter_max_attempts: int = None
    ) -> bool:
        # TODO: docstring
        self.s3.get_waiter(
            'bucket_exists'
        ).wait(
            Bucket=bucket_name,
            ExpectedBucketOwner=expected_owner,
            WaiterConfig={
                'Delay': waiter_delay,
                'MaxAttempts': waiter_max_attempts
            }
        )
        # wait blocks until success so if no exception was raised,
        # then we have success!
        return True

    def list_buckets(self) -> list[str]:
        # TODO: add filtering
        # TODO: docstring
        return [
            bucket.get('Name')
            for bucket in self.s3.list_buckets().get('Buckets')
        ]

    def list_folders(
        self,
        path: str,
        bucket: str = '',
        filters: dict = {
            'starts_with': '',
            # TODO: add contains
            'ends_with': ''
        }
    ) -> list[dict]:
        # TODO: docstring
        aws_objects = self.list_objects(
            bucket=bucket or self.bucket,
            prefix=path
        ).get('Contents', [])
        folders = []
        for aws_object in aws_objects:
            # 'folders' in aws are only folders if they end with /
            if aws_object.get('Key', '')[-1] == '/':
                if (
                    filters.get('starts_with')
                    and not aws_object.get('Key', '').starts_with(
                        filters.get('starts_with')
                    )
                ):
                    # this does not match the filter, so look at
                    # the next object
                    continue
                if (
                    filters.get('ends_with')
                    and not aws_object.get('Key', '')[:-1].ends_with(
                        filters.get('ends_with')
                    )
                ):
                    # this does not match the filter, so look at
                    # the next object
                    continue
                folders.append(aws_object)
        return folders

    def list_files(
        self,
        path: str,
        filters: dict = {
            'starts_with': '',
            # TODO: add contains
            'ends_with': ''
        },
        csv_only: bool = False,
        json_only: bool = False,
        bucket: str = ''
    ) -> list[dict]:
        # TODO: docstring
        aws_objects = self.list_objects(
            bucket=bucket or self.bucket,
            prefix=path
        ).get('Contents', [])
        files = []
        for aws_object in aws_objects:
            print(f'checking: {aws_object.get("Key", "NO KEY!!!")}')
            if not aws_object.get('Key', '').endswith('/'):
                obj_key = aws_object.get('Key', '')
                if csv_only and not obj_key.endswith('.csv'):
                    continue
                if json_only and not obj_key.endswith('.json'):
                    continue
                if (
                    filters.get('starts_with')
                    and not obj_key.startswith(
                        filters.get('starts_with')
                    )
                ):
                    continue
                if (
                    filters.get('ends_with')
                    and not obj_key.endswith(
                        filters.get('ends_with')
                    )
                ):
                    continue
                # TODO: using a generator here might be better
                files.append(aws_object)
        return files

    def list_folder_contents(
        self,
        path: str,
        bucket: str = '',
        filters: dict ={
            'starts_with': '',
            # TODO: add contains
            'ends_with': ''
        },
        continuation_token:str = '',
        max_object: int = None, # TODO: pagination!!!!
        delimiter: str = ''
    ) -> list[str]:
        # TODO: docstring
        aws_objects = self.list_objects(
            bucket=bucket or self.bucket,
            prefix=path
        )
        print(aws_objects.keys())
        contents = []
        for aws_obj in aws_objects.get('Contents', []):
            if (
                filters.get('starts_with')
                and not aws_obj.get('Key', '').startswith(
                    filters.get('starts_with')
                )
            ):
                continue
            if (
                filters.get('ends_with')
                and not aws_obj.get('Key', '').endswith(
                    filters.get('ends_with')
                )
            ):
                continue
            contents.append(aws_obj.get('Key'))
            # TODO: could use a generator instead
        return contents

    def list_objects(
        self,
        bucket: str = None,
        delimiter: str = None,
        continuation_token: str = None,
        max_keys: int = None,
        prefix: str = None
    ) -> dict:
        # TODO: docstring
        kwargs = {
            'Bucket': bucket or self.bucket
        }
        # the key will exist, we just need to know
        # it is populated
        if not kwargs['Bucket']:
            raise Exception(
                'Must set, or provide a bucket'
            ) # TODO: choose more appropriate exception
        for arg, val in [
            ('Delimiter', delimiter),
            ('ContinuationToken', continuation_token),
            ('MaxKeys', max_keys),
            ('Prefix', prefix)
        ]:
            if val:
                kwargs[arg] = val
        return self.s3.list_objects_v2(
            **kwargs
        )

    def check_bucket(self, bucket_name: str = '') -> dict:
        # TODO: docstring
        return self.s3.head_bucket(
            Bucket=bucket_name or self.bucket
        )

    def check_file(
        self,
        path: str,
        bucket: str = ''
    ) -> dict:
        # TODO: docstring
        if not path.endswith('/'):
            return self.check_object(obj_name=path, bucket=bucket)
        return {
            'error': 'Not a file'
        } # TODO: this should be standardized

    def check_folder(
        self,
        path: str,
        bucket: str = ''
    ) -> dict:
        # TODO: docstring
        if path.endswith('/'):
            return self.check_object(obj_name=path, bucket=bucket)
        return {
            'error': 'Not a folder'
        } # TODO: this should be standardized

    def check_object(
        self,
        obj_name: str,
        bucket: str = '',
    ) -> dict:
        # TODO: docstring
        return self.s3.head_object(
            Bucket=bucket or self.bucket,
            Key=obj_name
        )

    def write_to_file(
        self,
        filename: str,
        content: bytes,
        bucket: str = '',
        **kwargs
    ) -> dict:
        # TODO: docstring
        return self.s3.put_object(
            Body=content,
            Bucket=bucket or self.bucket,
            Key=filename,
            **kwargs
        )

    def write_json(
        self,
        target: str,
        content: Union[dict, str],
        bucket: str = ''
    ) -> bool:
        # TODO: docstring
        # if a dict is provided dump the content, otherwise assume valid json
        json_content = json.dumps(content) if type(content) is dict else content
        return self.write_to_file(
            filename=target,
            content=content,
            bucket=bucket
        )

    def read_json(
        self,
        target: str,
        bucket: str = ''
    ) -> dict:
        # TODO: docstring
        content = self.s3.get_object(
            Bucket=bucket or self.bucket,
            Key=target
        ).get('Body')
        loaded = json.loads(content.read()) # throws JSONDecodeError
        return loaded

    def download_to_file(
        self,
        s3_target: str,
        local_target: str,
        bucket: str = '',
    ) -> int:
        # TODO: docstring
        self.s3.download_file(
            Bucket=bucket or self.bucket,
            Key=s3_target,
            Filename=local_target,
        )
        return True

    def download_to_filelike(
        self,
        s3_target: str,
        filelike: object,
        bucket: str = ''
    ) -> bytes: # TODO: I don't think bytes is actually correct here...
        # TODO: docstring
        self.s3.download_fileobj(
            Bucket=bucket or self.bucket,
            Key=s3_target,
            Fileobj=filelike # the s3 client function will do the writing
        )
        return True

    def download_folder(
        self,
        target: str,
        bucket: str = None
    ) -> int:
        # TODO: docstring
        # TODO: implement this
        # NOTE: may need to list contents of a prefix, and ensure the delimiter
        # is used, hopefully that will 
        raise NotImplemented('This has not been added yet.')

    def upload_file(
        self,
        target: str,
        local_path: str,
        bucket: str = ''
    ) -> bool:
        # TODO: docstring
        self.s3.upload_file(
            Filename=local_path,
            Bucket=bucket or self.bucket,
            Key=target
        )
        return True

    def upload_folder(
        self,
        target: str,
        content: bytes,
        bucket: str = ''
    ) -> str:
        # TODO: docstring
        # TODO: implement this
        raise NotImplemented('This has not been added yet.')

    def copy_file(
        self,
        copy_from: str,
        copy_to: str,
        bucket_from: str = '',
        bucket_to: str = '',
    ) -> bool:
        # TODO: docstring
        return self.s3.copy_object(
            CopySource={
                'Bucket': bucket_from or self.bucket,
                'Key': copy_from
                # This could be extended to include a VersionID for
                # copying a specific version of the file
                # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object
            },
            Bucket=bucket_to or self.bucket,
            Key=copy_to
        )

    def copy_folder(
        self,
        copy_from: str,
        copy_to: str,
        bucket_from: str = '',
        bucket_to: str = '',
    ) -> bool:
        # TODO: docstring
        # TODO: implement this
        raise NotImplemented('This has not been added yet.')

    def delete_file(
        self,
        target: str,
        bucket: str = ''
    ) -> bool:
        # TODO: docstring
        return self.s3.delete_object(
            Bucket=bucket or self.bucket,
            Key=target
        )

    def delete_folder(
        self,
        target: str,
        bucket: str = '',
        allow_recursive: bool = True
    ) -> bool:
        # TODO: docstring
        # TODO: implement this
        raise NotImplemented('This has not been added yet.')
        # get all files in target folder
        # if allow_recursive, call this for each subfolder

    def is_valid_s3_link(self, s3_link: str) -> tuple[bool, dict]:
        # TODO: docstring
        if not s3_link.startswith('s3://'):
            return False
        bucket, s3_key = self.decompose_s3_uri(s3_link[5:]) # slice out s3://
        bucket_test = self.check_bucket(bucket)
        key_test = self.check_object(obj_name=s3_key, bucket=bucket)
        return (bucket_test is not None) and (key_test is not None)

    @staticmethod
    def decompose_s3_uri(s3_link: str) -> tuple[str, str]:
        # TODO: docstring
        if not s3_link:
            # guard against empty string
            return '', ''
        # remove whitespace
        s3_link = s3_link.strip()
        if not s3_link.startswith('s3://'):
            raise Exception(f'{s3_link} is not a valid s3 link')
        bucket = s3_link[5:].split('/')[0]
        s3key = '/'.join(s3_link[5:].split('/')[1:])
        return bucket, s3key

    @staticmethod
    def compose_s3_uri(bucket: str, key: str) -> str:
        # TODO: docstring
        # return 's3://' + os.path.join(bucket, key) # TODO: do we actually need path.join? isn't it always / on s3?
        return f's3://{bucket}/{key}'

    @staticmethod
    def compose_s3_url(bucket: str, key: str) -> str:
        # TODO: docstring
        return f'https://{bucket}.s3.amazonaws.com/{key}' # TODO: check this formatting

    @staticmethod
    def is_file(key: str) -> bool:
        # TODO: docstring
        return not key.endswith('/')

    def walk(self, root: str, bucket: str = '') -> dict:
        # TODO: docstring
        # TODO: implement this
        # NOTE: this may not actually be required, as in reality s3 is flat, there is
        # no folder structure to walk, if you leave off the delimiter it will pull 
        # everything with the provided prefix
        raise NotImplemented('This has not been added yet.')

    def filter():
        # TODO: docstring
        # TODO: implement this
        # NOTE: not sure I want this.. depends on how it's implemented
        # but it could be annoying, but it will make it more DRY
        raise NotImplemented('This has not been added yet.')
