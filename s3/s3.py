import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Generator, Iterable, List, Tuple, Union

import boto3


class S3:
    def __init__(
        self, s3_uri_base: str = "", aws_secret_key: str = "", aws_access_key: str = ""
    ) -> None:
        """An S3Connector provides many convenience functions for making use of s3

        Args:
            s3_uri_base (str, optional): an s3 link (pattern: s3://{bucket}/{key})
                This link will be decomposed into a bucket and key and will set them
                to be default values. Defaults to ''.
            aws_secret_key (str, optional): your aws_secret_key credential, if
                provided, aws_access_key must also be provided, if not provided
                uses default configs (~/.aws/credentials). Defaults to ''.
            aws_access_key (str, optional): your aws access key credential, if
                provided aws_secret_key must also be provided, if not provided
                uses default configs (~/.aws/credentials). Defaults to ''.
        """
        if aws_access_key and aws_secret_key:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
            )
        else:
            # use default configs at ~/.aws/config
            self.s3 = boto3.client("s3")
        self.events = self.s3.meta.events
        self.base_uri = s3_uri_base
        self._bucket, self._path_prefix = self.decompose_s3_uri(s3_uri_base)
        self.default_delimiter = "/"

    def __repr__(self) -> str:
        """A default string representation for an S3Connector

        Returns:
            str: simply adds the bucket and path to default object output
        """
        base = super().__repr__()[1:-1]
        return f"<{base} bucket={self.bucket} cwd={self.cwd}>"

    @property
    def waiters(self) -> List[str]:
        """A list of "waiters" which provide details on asynchronous tasks

        Returns:
            List[str]: the names of the available waiters
        """
        return self.s3.waiter_names

    @property
    def bucket(self) -> str:
        """the current default bucket

        Returns:
            str: the name of the current default bucket
        """
        return self._bucket

    @bucket.setter
    def bucket(self, bucket_name: str) -> None:
        """before setting the new default bucket, checks if it can be accessed

        Args:
            bucket_name (str): the name of the bucket to check, then assign (if valid)

        Raises:
            S3.Client.exceptions.NoSuchBucket
            botocore.exceptions.ClientError
        """
        # head_bucket throws an exception if we don't have
        # access, or it doesn't exist, so if no exception
        # was thrown we can move forward.
        # NOTE: the head_bucket check was removed because it caused
        # NOTE: some issues with building on the pipeline
        self._bucket = bucket_name

    @property
    def cwd(self) -> str:
        """the default current working directory

        Returns:
            str: the prefix for the "working directory"
        """
        return self._path_prefix

    @cwd.setter
    def cwd(self, path: str) -> None:
        """sets the new current working directory

        Args:
            path (str): the s3 key prefix to set as the default working directory

        Raises:
            Exception: The prefix could not be accessed
        """
        # NOTE: this requires we have a bucket set
        if path in self.check_folder(path).get("found_in"):
            self._path_prefix = path
        else:
            raise Exception("Path not found in current bucket")

    @property
    def base_s3_uri(self):
        """get the bucket and cwd as an s3 uri

        Returns:
            str: the s3 uri that matches your default parameters (bucket and cwd)
        """
        return self.compose_s3_uri(bucket=self.bucket, key=self.cwd)

    @base_s3_uri.setter
    def base_s3_uri(self, s3_uri: str) -> None:
        """sets the default values based on an s3 uri, or bucket and path (key prefix)

        Args:
            s3_uri (str, optional): s3 uri to decompose into bucket and cwd, should end
                with a folder.
        """
        self.bucket, self.cwd = self.decompose_s3_uri(s3_uri)

    def create_bucket(
        self, name: str, location: str = "", access_control: str = "", **kwargs
    ) -> dict:
        """Create a new bucket with the provided values. Can provide any keyword
            arguments for the s3 client's create bucket call and it will be passed
            through.

        Args:
            name (str): the name of the bucket to create
            location (str, optional): the region to create this bucket in, one of:
                "
                    'af-south-1', 'ap-east-1', 'ap-northeast-1', 'ap-northeast-2',
                    'ap-northeast-3', 'ap-south-1', 'ap-southeast-1', 'ap-southeast-2',
                    'ca-central-1', 'cn-north-1', 'cn-northwest-1', 'EU',
                    'eu-central-1', 'eu-north-1', 'eu-south-1', 'eu-west-1',
                    'eu-west-2', 'eu-west-3', 'me-south-1', 'sa-east-1', 'us-east-2',
                    'us-gov-east-1', 'us-gov-west-1', 'us-west-1', 'us-west-2'
                "
                Defaults to ''.
            access_control (str, optional): the access control setting to use, one of:
                "
                    'private', 'public-read', 'public-read-write', 'authenticated-read'
                "
                Defaults to ''.

        Returns:
            dict: {'Location': 'string, where it was created'}

        Raises:
            S3.Client.exceptions.BucketAlreadyExists
            S3.Client.exceptions.BucketAlreadyOwnedByYou
        """
        if location:
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": location}
        if access_control:
            kwargs["ACL"] = access_control
        return self.s3.create_bucket(Bucket=name, **kwargs)

    def delete_bucket(self, bucket: str = ""):
        """delete the specified bucket (name), or the current default bucket,
            if no args are provided.

        Args:
            bucket (str, optional): The name of the bucket to delete. Defaults to ''.
        Returns:
            dict: {
                'ResponseMetadata': {
                    'HTTPStatusCode': int (204 on success),
                    'HTTPHeaders': dict,
                    'RetryAttempts': int
                }
            }
        Raises:
            botocore.errorfactory.NoSuchBucket: the bucket you are trying to delete
                doesn't exist (this is a ClientError)
        """
        response = self.s3.delete_bucket(Bucket=bucket or self.bucket)
        if not bucket or bucket == self.bucket:
            # we just deleted our current bucket
            # so let's remove that, set the hidden
            # field so it doesn't try to do the check
            self._bucket = ""
        return response

    def wait_for_bucket(
        self,
        bucket_name: str,
        expected_owner: str = "",
        waiter_delay: int = None,
        waiter_max_attempts: int = None,
    ) -> bool:
        """A blocking function that waits for a bucket to be created

        Args:
            bucket_name (str): The name of the bucket to wait for
            expected_owner (str, optional): The expected owner of the bucket.
                Defaults to ''.
            waiter_delay (int, optional): time (seconds) to wait between polls.
                Defaults to None.
            waiter_max_attempts (int, optional): Will not poll more times than this.
                Defaults to None.

        Returns:
            bool: True if successful

        Raises:
            TODO: specify the various exception this can raise
        """
        self.s3.get_waiter("bucket_exists").wait(
            Bucket=bucket_name,
            ExpectedBucketOwner=expected_owner,
            WaiterConfig={"Delay": waiter_delay, "MaxAttempts": waiter_max_attempts},
        )
        # wait blocks until success so if no exception was raised,
        # then we have success!
        return True

    def list_buckets(self) -> List[str]:
        """Gets a list of bucket names that you have access to

        Returns:
            List[str]: the names of buckets that are visible to you
        """
        # TODO: add filtering, there can be a large number of buckets
        return [bucket.get("Name") for bucket in self.s3.list_buckets().get("Buckets")]

    def list_folders(
        self,
        path: str,
        bucket: str = "",
        filters: dict = {
            "starts_with": "",
            # TODO: add contains
            "ends_with": "",
        },
        delimiter: str = "/",
    ) -> List[str]:
        """List all folders in bucket (or default bucket) under path (key prefix)

        Args:
            path (str): the key prefix to list folders under (all folders will have this prefix)
            bucket (str, optional): The name of the bucket to look in. Defaults to ''.
            filters (dict, optional): filters to apply the the search result.
                Defaults to { 'starts_with': '', 'ends_with': '' }.
            delimiter (str, optional): delimiter to use, should typically be / but can
                be whatever you decide, use '' to list subfolders as well.
                Defaults to '/'.
        Returns:
            List[str]: The folders in bucket under path
        """
        aws_objects = self.s3.list_objects_v2(
            Bucket=bucket or self.bucket,
            Prefix=path,
            Delimiter=delimiter,
        )
        folders = []
        for aws_object in aws_objects.get("CommonPrefixes", []):
            # 'folders' in aws are only folders if they end with /
            folder: str = aws_object.get("Prefix", "")
            if filters.get("starts_with") and (
                not folder.startswith(filters["starts_with"])
                or not folder.startswith(path + filters["starts_with"])
            ):
                # this does not match the filter, so look at
                # the next object
                continue
            if filters.get("ends_with") and not (
                folder[:-1].endswith(filters["ends_with"])
                or folder.endswith(filters["ends_with"])
            ):
                # this does not match the filter, so look at
                # the next object
                continue
            folders.append(folder)
        return folders

    def list_files(
        self,
        path: str,
        filters: dict = {
            "starts_with": "",
            # TODO: add contains
            "ends_with": "",
        },
        bucket: str = "",
        delimiter: str = "/",
    ) -> Generator[str, None, None]:
        """List the files in bucket under path

        Args:
            path (str): the path (key prefix) that the files are under
            filters (dict, optional): filters to apply to the resulting list.
                Defaults to { 'starts_with': '', 'ends_with': '' }.
            bucket (str, optional): name of the bucket to look in,
                '' uses default bucket. Defaults to ''.
            delimiter (str, optional): key delimiter to use, use ''
                to list subfolder contents as well. Defaults to '/'.
        Returns:
            List[str]: the files in bucket under path
        """
        aws_objects = self.list_objects(
            bucket=bucket or self.bucket, prefix=path, delimiter=delimiter
        )
        for aws_object in aws_objects:
            if not aws_object.endswith("/"):
                if filters.get("starts_with") and not (
                    aws_object.startswith(filters.get["starts_with"])
                    or path + aws_object.startswith(filters.get["starts_with"])
                ):
                    continue
                if filters.get("ends_with") and not aws_object.endswith(
                    filters.get("ends_with")
                ):
                    continue
                yield aws_object

    def list_json_files(
        self, path: str, filters: dict = {}, delimiter: str = "/", bucket: str = ""
    ) -> Generator[str, None, None]:
        """lists json files under path in bucket

        Args:
            path (str): all returned s3 keys will have this prefix
            filters (dict, optional): filter the values to return based on startswith and endswith. Defaults to {}.
            delimiter (str, optional): limits the depth to look, to list all use ''. Defaults to '/'.
            bucket (str, optional): the bucket to search in. Defaults to '' (Which uses the defaul bucket).

        Yields:
            Generator[str]: the keys matching provided filters, all will have the `.json` extension
        """
        for obj in self.list_objects(bucket=bucket, prefix=path, delimiter=delimiter):
            if obj.endswith(".json"):
                if filters.get("starts_with") and not (
                    obj.startswith(filters["starts_with"])
                    or obj.startswith(path + filters["starts_with"])
                ):
                    continue
                if filters.get("ends_with") and not (
                    obj.endswith(filters["ends_with"])
                    or obj.endswith(filters["ends_with"] + ".json")
                ):
                    continue
                yield obj

    def list_csv_files(
        self, path: str, filters: dict = {}, delimiter: str = "/", bucket: str = ""
    ) -> Generator[str, None, None]:
        """lists csv files under path in bucket

        Args:
            path (str): all returned s3 keys will have this prefix
            filters (dict, optional): filter the values to return based on startswith and endswith. Defaults to {}.
            delimiter (str, optional): limits the depth to look, to list all use ''. Defaults to '/'.
            bucket (str, optional): the bucket to search in. Defaults to '' (Which uses the defaul bucket).

        Yields:
            Generator[str]: the keys matching provided filters,
                all will have the `.csv` extension
        """
        for obj in self.list_objects(bucket=bucket, prefix=path, delimiter=delimiter):
            if obj.endswith(".csv"):
                if filters.get("starts_with") and not (
                    obj.startswith(filters["starts_with"])
                    or obj.startswith(path + filters["starts_with"])
                ):
                    continue
                if filters.get("ends_with") and not (
                    obj.endswith(filters["ends_with"])
                    or obj.endswith(filters["ends_with"] + ".csv")
                ):
                    continue
                yield obj

    def list_folder_contents(
        self,
        path: str = "",  # root of the bucket
        bucket: str = "",
        filters: dict = {
            "starts_with": "",
            # TODO: add contains
            "ends_with": "",
        },
        max_object: int = 1000,  # default max value
        delimiter: str = "/",  # default delimeter
    ) -> Generator[str, None, List[str]]:
        """List all folder contents (files and folders)

        Args:
            path (str, optional): the path to list contents of. Defaults to ''.
            filters (dict, optional): Filters to apply to search results. Defaults to { 'starts_with': '', 'ends_with': '' }.
            max_object (int, optional): max objects to return. Defaults to 1000.
        Returns:
            List[str]: The contents of the specified folder in bucket
        """
        folders = self.list_folders(
            bucket=bucket or self.bucket,
            path=path,
            delimiter=delimiter,
        )
        files = self.list_files(
            path=path, filters=filters, bucket=bucket, delimiter=delimiter
        )
        contents = []
        # get the file objects from contents
        for folder in folders:
            if filters.get("starts_with") and not folder.startswith(
                filters.get("starts_with")
            ):
                continue
            if filters.get("ends_with") and not folder.endswith(
                filters.get("ends_with")
            ):
                continue
            yield folder
            contents.append(folder)

        for file in files:
            yield file
            contents.append(file)

        return contents

    def list_objects(
        self,
        bucket: str = "",
        delimiter: str = "/",
        prefix: str = "",
        max_keys: int = 1000,
    ) -> Generator[str, None, None]:
        """Lists objects in bucket with prefix, delimiter allows "layers" (akin to a directory structure) if memory is a concern default max_keys is 1000, lower this to decrease memory usage, but increase network calls.

        Args:
            bucket (str, optional): The name of the bucket to look in. Defaults to None.
            delimiter (str, optional): delimiter allows for controlling depth, typically a / is used since this gets represented as a directory structure. Defaults to None.
            max_keys (int, optional): the maximum number of keys. Defaults to None.
            prefix (str, optional): all returned objects will have this prefix (like having a base folder to search from). Defaults to None.

        Raises:
            Exception: Either a default bucket must be set or a bucket name must be specified

        Returns:
            Generator: this method yields strings, which are keys for s3 objects
        """
        kwargs = {"Bucket": bucket or self.bucket}
        # the key will exist, we just need to know
        # it is populated, if it resolves false, then
        # no default bucket has been defined
        if not kwargs["Bucket"]:
            raise Exception("Must set, or provide a bucket")
        continuation_token = ""
        while True:
            for arg, val in [
                ("Delimiter", delimiter),
                ("ContinuationToken", continuation_token),
                ("Prefix", prefix),
                ("MaxKeys", max_keys),
            ]:
                if val:
                    kwargs[arg] = val
            objs = self.s3.list_objects_v2(**kwargs)
            for obj in objs.get("Contents", []):
                yield obj.get("Key")
            if not objs.get("IsTruncated"):
                break
            continuation_token = objs.get("NextContinuationToken")

    def check_bucket(self, bucket_name: str = "") -> dict:
        """poll the specified bucket (or default bucket)

        Args:
            bucket_name (str, optional): the name of the bucket to check.
                Defaults to '' which will use the default bucket (self.bucket).

        Returns:
            dict: {
                'ResponseMetadata': {
                    'RequestId': str,
                    'HostId': str,
                    'HTTPStatusCode': int (200 on success),
                    'HTTPHeaders': dict,
                    'x-amz-request-id': str,
                    'date': str,
                    'x-amz-bucket-region': str,
                    'content-type': str,
                    'server': str
                },
                'RetryAttempts': int
            }

        Raises:

        """
        return self.s3.head_bucket(Bucket=bucket_name or self.bucket)

    def check_file(self, path: str, bucket: str = "") -> dict:
        """Polls the object specified by 'path' in bucket

        Args:
            path (str): the key for the object (file) to poll
            bucket (str, optional): The name of the bucket to look in. Defaults to ''.

        Returns:
            dict: {
                'DeleteMarker': bool,
                'AcceptRanges': str,
                'Expiration': str,
                'Restore': str,
                'ArchiveStatus': str one of: 'ARCHIVE_ACCESS', 'DEEP_ARCHIVE_ACCESS',
                'LastModified': datetime,
                'ContentLength': int,
                'ETag': str,
                'MissingMeta': int,
                'VersionId': str,
                'CacheControl': str,
                'ContentDisposition': str,
                'ContentEncoding': str,
                'ContentLanguage': str,
                'ContentType': str,
                'Expires': datetime,
                'WebsiteRedirectLocation': str,
                'ServerSideEncryption': str one of: 'AES256', 'aws:kms',
                'Metadata': {
                    str: str
                },
                'SSECustomerAlgorithm': 'str,
                'SSECustomerKeyMD5': 'str,
                'SSEKMSKeyId': 'str,
                'BucketKeyEnabled': bool,
                'StorageClass': str one of: 'STANDARD', 'REDUCED_REDUNDANCY',
                    'STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING', 'GLACIER',
                    'DEEP_ARCHIVE', 'OUTPOSTS',
                'RequestCharged': str,
                'ReplicationStatus': str one of: 'COMPLETE', 'PENDING',
                    'FAILED', 'REPLICA',
                'PartsCount': int,
                'ObjectLockMode': str one of: 'GOVERNANCE', 'COMPLIANCE',
                'ObjectLockRetainUntilDate': datetime,
                'ObjectLockLegalHoldStatus': str one of: 'ON', 'OFF'
            }
        """
        if not path.endswith("/"):
            return self.check_object(obj_name=path, bucket=bucket)
        raise Exception("Not a file")

    def check_folder(self, path: str, bucket: str = "") -> dict:
        """checks if a folder exists, returning basic info about it

        Args:
            path (str): the s3 key to check
            bucket (str, optional): the bucket to look in.
                Defaults to '', which tries to use the default bucket.

        Returns:
            dict: {
                'found_in': [str, ...]
            }
        """
        if path.endswith("/"):
            return {
                "found_in": [
                    path
                    for prefix in self.list_folder_contents(bucket=bucket, delimiter="")
                    if path in prefix
                ]
            }
        raise Exception("Not a folder")

    def check_object(
        self,
        obj_name: str,
        bucket: str = "",
    ) -> dict:
        """heads an object, getting some basic information about it

        Args:
            obj_name (str): the key/path of the object to check
            bucket (str, optional): the bucket to look int.
                Defaults to '' which uses the default bucket.

        Returns:
            dict: {
                'DeleteMarker': bool,
                'AcceptRanges': str,
                'Expiration': str,
                'Restore': str,
                'ArchiveStatus': str one of: 'ARCHIVE_ACCESS', 'DEEP_ARCHIVE_ACCESS',
                'LastModified': datetime,
                'ContentLength': int,
                'ETag': str,
                'MissingMeta': int,
                'VersionId': str,
                'CacheControl': str,
                'ContentDisposition': str,
                'ContentEncoding': str,
                'ContentLanguage': str,
                'ContentType': str,
                'Expires': datetime,
                'WebsiteRedirectLocation': str,
                'ServerSideEncryption': str one of: 'AES256', 'aws:kms',
                'Metadata': {
                    str: str
                },
                'SSECustomerAlgorithm': 'str,
                'SSECustomerKeyMD5': 'str,
                'SSEKMSKeyId': 'str,
                'BucketKeyEnabled': bool,
                'StorageClass': str one of: 'STANDARD', 'REDUCED_REDUNDANCY',
                    'STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING', 'GLACIER',
                    'DEEP_ARCHIVE', 'OUTPOSTS',
                'RequestCharged': str,
                'ReplicationStatus': str one of: 'COMPLETE', 'PENDING',
                    'FAILED', 'REPLICA',
                'PartsCount': int,
                'ObjectLockMode': str one of: 'GOVERNANCE', 'COMPLIANCE',
                'ObjectLockRetainUntilDate': datetime,
                'ObjectLockLegalHoldStatus': str one of: 'ON', 'OFF'
            }
        """
        return self.s3.head_object(Bucket=bucket or self.bucket, Key=obj_name)

    def get_object(self, key: str = "", bucket: str = "", **kwargs) -> dict:
        if "Bucket" in kwargs:
            bucket = kwargs["Bucket"]
            del kwargs["Bucket"]
        if "Key" in kwargs:
            key = kwargs["Key"]
            del kwargs["Key"]
        if not key:
            raise TypeError("Either 'key' or 'Key' must be provided.")
        return self.s3.get_object(Bucket=bucket or self.bucket, Key=key, **kwargs)

    def write_to_file(
        self, filename: str, content: bytes, bucket: str = "", **kwargs
    ) -> dict:
        """writes content to an s3 location specified by filename in bucket

        Args:
            filename (str): the file to write to, if it exists it will be overwritten,
                must be in the form of an absolute path
            content (bytes): the content to write to the file
            bucket (str, optional): the bucket to write the file in. Defaults to '',
                which uses the default bucket.

        Returns:
            dict: {
                'Expiration': str,
                'ETag': str,
                'ServerSideEncryption': str one of: 'AES256', 'aws:kms',
                'VersionId': str,
                'SSECustomerAlgorithm': str,
                'SSECustomerKeyMD5': str,
                'SSEKMSKeyId': str,
                'SSEKMSEncryptionContext': str,
                'BucketKeyEnabled': bool,
                'RequestCharged': str
            }
        """
        return self.s3.put_object(
            Body=content, Bucket=bucket or self.bucket, Key=filename, **kwargs
        )

    def read_from_file(
        self,
        filename: str,
        bucket: str = "",
    ) -> bytes:
        """reads the content of a file and yields the contents or returns a string

        Args:
            filename (str): the filename (must include full path)
            bucket (str, optional): the bucket to write to. Defaults to '', uses the
                default bucket.

        Returns:
            str: reads the file contents as a string and returns that
        """
        result = self.s3.get_object(Bucket=bucket or self.bucket, Key=filename).get(
            "Body"
        )
        full_return = b""
        for chunk in result.iter_chunks():
            full_return += chunk
        return full_return

    def read_stream_from_file(
        self, filename: str, bucket: str = ""
    ) -> Generator[bytes, None, None]:
        """reads the content of a file and yields contents

        Args:
            filename (str): the filename (must include full path)
            bucket (str, optional): the bucket to write to. Defaults to '', uses the
                default bucket.

        Returns:
            generator: reads the file contents as a string and returns that
        """
        result = self.s3.get_object(Bucket=bucket or self.bucket, Key=filename).get(
            "Body"
        )
        for chunk in result.iter_chunks():
            yield chunk

    def read_stream_lines_from_file(
        self, filename: str, bucket: str = ""
    ) -> Generator[bytes, None, None]:
        """reads the content of a file and yields lines

        Args:
            filename (str): the filename (must include full path)
            bucket (str, optional): the bucket to write to. Defaults to '', uses the
                default bucket.

        Returns:
            generator: reads the file contents as a string and returns that
        """
        result = self.s3.get_object(Bucket=bucket or self.bucket, Key=filename).get(
            "Body"
        )
        for line in result.iter_lines():
            yield line

    def read_first_line_from_file(self, filename: str, bucket: str = "") -> bytes:
        """reads the first line of a file and closes the stream

        Args:
            filename (str): the filename (must include full path)
            bucket (str, optional): the bucket to write to. Defaults to '', uses the
                default bucket.

        Returns:
            bytes: reads the file contents as bytes and returns that
        """
        result = self.s3.get_object(Bucket=bucket or self.bucket, Key=filename).get(
            "Body"
        )
        result_gen = result.iter_lines()
        first_line = result_gen.__next__()
        result_gen.close()
        return first_line

    def write_json(
        self, target: str, content: Union[dict, list, str], bucket: str = ""
    ) -> bool:
        """writes json from content to s3 file called target

        Args:
            target (str): the absolute path to the file to write to, will overwrite if
                exists
            content (Union[dict, list, str]): if a dict or a list is provided it will
                be dumped to json, otherwise it assumes the json is properly formatted
            bucket (str, optional): the bucket to write to. Defaults to '', uses the
                default bucket.

        Returns:
            dict: {
                'Expiration': str,
                'ETag': str,
                'ServerSideEncryption': str one of: 'AES256', 'aws:kms',
                'VersionId': str,
                'SSECustomerAlgorithm': str,
                'SSECustomerKeyMD5': str,
                'SSEKMSKeyId': str,
                'SSEKMSEncryptionContext': str,
                'BucketKeyEnabled': bool,
                'RequestCharged': str
            }
        """
        # if a dict is provided dump the content, otherwise assume valid json
        json_content = json.dumps(content) if not isinstance(content, str) else content
        return self.write_to_file(filename=target, content=json_content, bucket=bucket)

    def read_json(self, target: str, bucket: str = "") -> Union[dict, list]:
        """read the contents of s3 object at target into a dict

        Args:
            target (str): the (json) file to read from
            bucket (str, optional): the bucket the file is in. Defaults to ''.

        Returns:
            dict: the contents of target loaded via json.loads
        """
        content = self.read_from_file(filename=target, bucket=bucket)
        loaded = json.loads(content)  # throws JSONDecodeError
        return loaded

    def download_to_file(
        self,
        s3_target: str,
        local_target: str,
        bucket: str = "",
    ) -> int:
        """downloads the s3 object specified by s3_target in bucket to the (local) file defined by local_target

        Args:
            s3_target (str): the key for the s3 object to download
            local_target (str): the path to the local file to write to
            bucket (str, optional): the bucket to look in.
                Defaults to '', which uses the default bucket.

        Returns:
            bool: True if no exceptions were raised
        """
        self.s3.download_file(
            Bucket=bucket or self.bucket,
            Key=s3_target,
            Filename=local_target,
        )
        return True

    def download_to_files(
        self,
        download_to_from: Iterable[Tuple[str, str]],
        bucket: str = "",
        max_workers: int = 3,
    ):
        """download the list of files to the location specified by download_to_from[0]
            from the location specified by download_to_from[1]

        Args:
            download_to_from (Iterable[Tuple[str, str]]): an iterable of tuples in the
                form [(local file path, s3 key), ...]
            bucket (str, optional): the bucket to search in. Defaults to ''.
            max_workers (int, optional): the number of workers to use for threading.
                Defaults to 3.

        Returns:
            dict: a dictionary in the form {
                (local path, s3 key): True|False
            }
        """
        futures = []
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for local_file, s3_target in download_to_from:
                future = executor.submit(
                    self.download_to_file,
                    **{
                        "s3_target": s3_target,
                        "local_target": local_file,
                        "bucket": bucket,
                    },
                )
                future.id = (local_file, s3_target)
                futures.append(future)

            for future in as_completed(futures):
                results[future.id] = future.result()

        return results

    def download_to_filelike(
        self, s3_target: str, filelike: object, bucket: str = ""
    ) -> bool:
        """dowload the s3 object specified by s3_target into the filelike object
            (ie the result of calling open())

        Args:
            s3_target (str): the key of the object to download
            filelike (object): the object to write the contents of s3_target into
            bucket (str, optional): the bucket to look in.
                Defaults to '' which uses the defualt bucket.

        Returns:
            bool: True if no exceptions were raised
        """
        self.s3.download_fileobj(
            Bucket=bucket or self.bucket,
            Key=s3_target,
            Fileobj=filelike,  # the s3 client function will do the writing
        )
        return True

    def download_to_filelikes(
        self,
        download_to_from: Iterable[Tuple[object, str]],
        bucket: str = "",
        max_workers: int = 3,
    ):
        """downloads one or more files to file-like objects
            (ex the result of `open(...)`)

        Args:
            download_to_from (Iterable[Tuple[object, str]]): an iterable of tuples
                mapping s3 keys to filelike objects
            bucket (str, optional): the s3 bucket to search in.
                Defaults to '', which will try to use the default bucket.
            max_workers (int, optional): How many threads to be allowed. Defaults to 3.

        Returns:
            [type]: [description]
        """
        futures = []
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for filelike, s3_target in download_to_from:
                future = executor.submit(
                    self.download_to_filelike,
                    **{"s3_target": s3_target, "filelike": filelike, "bucket": bucket},
                )
                future.id = (filelike, s3_target)
                futures.append(future)

            for future in as_completed(futures):
                results[future.id] = future.result()

        return results

    def download_folder(self, target: str, local_path: str, bucket: str = "") -> bool:
        """Downloads each object with the prefix "target" to the local folder specified
            by local_path

        Args:
            target (str): the folder on s3 (key ending with /) to download from
            local_path (str): the local filesystem path to download to
            bucket (str, optional): the bucket to look in.
                Defaults to '', which uses the default bucket.

        Raises:
            Exception: if the target doesn't end with / (it's not a folder)

        Returns:
            bool: True if no exceptions were raised
        """
        if not target.endswith("/"):
            raise Exception("Target must be a folder")
        if not os.path.exists(local_path):
            os.makedirs(local_path, exist_ok=True)
        for s3_key in self.list_folder_contents(path=target, delimiter=""):
            if self.is_file(s3_key):
                *folders_in_path, _ = s3_key.split("/")
                os.makedirs(local_path + "/".join(folders_in_path), exist_ok=True)
                self.download_to_file(
                    s3_target=s3_key,
                    local_target=os.path.join(local_path, s3_key),
                    bucket=bucket,
                )
            else:
                os.makedirs(os.path.join(local_path, s3_key))
        return True

    def upload_file(
        self, target: str, local_path: str, bucket: str = "", extra_args: Dict = None
    ) -> bool:
        """upload the file at local_path to the s3 location target in bucket

        Args:
            target (str): the key for the file to upload to (will overwrite if exists)
            local_path (str): where the file is on the local filesystem
            bucket (str, optional): the bucket to look in.
                Defaults to '' which uses the default bucket.
            extra_args (dict): keys & values to pass as extra arguments when uploading
        (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#the-extraargs-parameter)

        Returns:
            bool: True if no exceptions were raised
        """
        self.s3.upload_file(
            Filename=local_path,
            Bucket=bucket or self.bucket,
            Key=target,
            ExtraArgs=extra_args if extra_args else {},
        )
        return True

    def upload_files(
        self,
        local_to_s3: List[Tuple[str, str]],
        bucket: str = "",
        max_workers: int = 3,
        extra_args: Dict = None,
    ) -> Tuple[List[str], List[str]]:
        """uploads one or more files to the specified locations

        Args:
            local_to_s3 (List[Tuple[str, str]]): a list of tuples mapping local files
                to desired s3 locations
            bucket (str, optional): the bucket to upload to.
                Defaults to '', which uses the default bucket.
            max_workers (int, optional): how many workers to allow for the threadpool.
                Defaults to 3.
            extra_args (dict): keys & values to pass as extra arguments when uploading each file. Same parameters will
            be used for all files.
        (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#the-extraargs-parameter)

        Returns:
            dict: the dictionary maps the upload to the result (boolean)
                in the form:
                    {
                        (from, to): True | False
                    }
        """
        futures = []
        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for local_file, s3_target in local_to_s3:
                future = executor.submit(
                    self.upload_file,
                    **{
                        "target": s3_target,
                        "local_path": local_file,
                        "bucket": bucket,
                        "extra_args": extra_args if extra_args else {},
                    },
                )
                future.id = (local_file, s3_target)
                futures.append(future)

            for future in as_completed(futures):
                results[future.id] = future.result()

        return results

    def upload_folder(
        self, target: str, local_path: str, bucket: str = "", extra_args: Dict = None
    ) -> bool:
        """Upload the contents of the folder at local_path to the s3 location target in bucket

        Args:
            target (str): the s3 key, ending in / to upload the files to
            local_path (str): where on the local filesystem the files to upload are
            bucket (str, optional): the bucket to look in. Defaults to ''.
            extra_args (dict): keys & values to pass as extra arguments when uploading each file. Same parameters will
            be used for all files.
        (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html#the-extraargs-parameter)

        Raises:
            FileNotFoundError: Cannot find the file on the local filesystem
            Exception: can't upload a folder to a file, target must end with /

        Returns:
            bool: True if no exceptions were raised
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Cannot locate {local_path}")
        if not target.endswith("/"):
            raise Exception("Must upload to a folder")
        for root, _, files in os.walk(local_path):
            for file in files:
                self.upload_file(
                    target=os.path.join(target, os.path.join(root, file)),
                    local_path=os.path.join(root, file),
                    bucket=bucket,
                    extra_args=extra_args,
                )
        return True

    def copy_file(
        self,
        copy_from: str,
        copy_to: str,
        bucket_from: str = "",
        bucket_to: str = "",
    ) -> dict:
        """Copy a file from one s3 location to another s3 location

        Args:
            copy_from (str): The s3 location to read from
            copy_to (str): The s3 location to write to
            bucket_from (str, optional): The bucket to read from.
                Defaults to '', which uses the default bucket.
            bucket_to (str, optional): The bucket to write to.
                Defaults to '', which uses the default bucket.

        Returns:
            dict: {
                'CopyObjectResult': {
                    'ETag': str,
                    'LastModified': datetime
                },
                'Expiration': str,
                'CopySourceVersionId': str,
                'VersionId': str,
                'ServerSideEncryption': str one of: 'AES256', 'aws:kms',
                'SSECustomerAlgorithm': str,
                'SSECustomerKeyMD5': str,
                'SSEKMSKeyId': str,
                'SSEKMSEncryptionContext': str,
                'BucketKeyEnabled': bool,
                'RequestCharged': str
            }
        """
        try:
            return self.s3.copy(
                CopySource={
                    "Bucket": bucket_from or self.bucket,
                    "Key": copy_from
                    # This could be extended to include a VersionID for
                    # copying a specific version of the file
                    # ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object
                },
                Bucket=bucket_to or self.bucket,
                Key=copy_to,
            )
        except Exception:
            raise FileNotFoundError(
                f"Failed to copy object. bucket_from={bucket_from or self.bucket} "
                f"{copy_from=} {bucket_to or self.bucket} {copy_to=}"
            )

    def move_file(
        self,
        move_from: str,
        move_to: str,
        bucket_from: str = "",
        bucket_to: str = "",
    ) -> str:
        """moves a file from one key to a new key (copy+delete old)

        Args:
            move_from (str): the object to be moved
            move_to (str): the new key/name/location
            bucket_from (str, optional): the bucket that stores `move_from`.
                Defaults to "", which uses the default bucket.
            bucket_to (str, optional): the bucket that will hold `move_to`.
                Defaults to "", which uses the default bucket.

        Raises:
            FileNotFoundError: something went wrong while copying the file

        Returns:
            str: the new key for the moved file
        """
        try:
            self.copy_file(
                copy_from=move_from,
                copy_to=move_to,
                bucket_from=bucket_from,
                bucket_to=bucket_to,
            )
            if self.key_exists(key=move_to, bucket=bucket_to):
                self.delete_file(target=move_from, bucket=bucket_from)
                return self.compose_s3_uri(bucket=bucket_to or self.bucket, key=move_to)
        except Exception as exc:
            raise FileNotFoundError(
                f"Failed to move object. bucket_from={bucket_from or self.bucket} "
                f"{move_from=} {bucket_to or self.bucket} {move_to=} \n{exc}"
            )

    def copy_files(
        self,
        files_from_to: Iterable[Tuple[str, str]],
        from_path_prefix: str = "",
        to_path_prefix: str = "",
        bucket_from: str = "",
        bucket_to: str = "",
    ) -> dict:
        """takes a list of tuples (or comparable data structures) in
        the form [(from, to), (from, to)] and copies each element of
        the list from the first value (s3 key) to the second value
        (s3_key).

        Args:
            files_from_to (Iterable[Iterable]): a collection that can
                be iterated over, where each element is able to unpack
                into to values.
            from_path_prefix (str, optional): Applies this prefix to
                each element's first value to create a full path.
                Defaults to ''.
            to_path_prefix (str, optional): Applies this prefix to each
                element's second value to create a full path.
                Defaults to ''.
            bucket_from (str, optional): the bucket to copy from.
                Defaults to ''.
            bucket_to (str, optional): The bucket to copy to.
                Defaults to ''.

        Returns:
            dict: the dictionary maps the copy to the result (boolean)
                in the form:
                    {
                        (from, to): True | False
                    }
        """
        futures = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for copy_from, copy_to in files_from_to:
                future = executor.submit(
                    self.copy_file,
                    **{
                        "copy_from": from_path_prefix + copy_from,
                        "copy_to": to_path_prefix + copy_to,
                        "bucket_from": bucket_from or self.bucket,
                        "bucket_to": bucket_to or self.bucket,
                    },
                )
                future.id = (copy_from, copy_to)
                futures.append(future)
            results = {}
            for future in as_completed(futures):
                results[future.id] = future.result()
            return results

    def move_files(
        self,
        files_from_to: Iterable[Tuple[str, str]],
        from_path_prefix: str = "",
        to_path_prefix: str = "",
        bucket_from: str = "",
        bucket_to: str = "",
    ) -> dict:
        """takes a list of tuples (or comparable data structures) in
        the form [(from, to), (from, to)] and copies each element of
        the list from the first value (s3 key) to the second value
        (s3_key). After each file is copied, the old file is removed.

        Args:
            files_from_to (Iterable[Iterable]): a collection that can
                be iterated over, where each element is able to unpack
                into to values.
            from_path_prefix (str, optional): Applies this prefix to
                each element's first value to create a full path.
                Defaults to ''.
            to_path_prefix (str, optional): Applies this prefix to each
                element's second value to create a full path.
                Defaults to ''.
            bucket_from (str, optional): the bucket to copy from.
                Defaults to ''.
            bucket_to (str, optional): The bucket to copy to.
                Defaults to ''.

        Returns:
            dict: the dictionary maps the copy to the result (boolean)
                in the form:
                    {
                        (from, to): True | False
                    }
        """
        futures = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            for copy_from, copy_to in files_from_to:
                future = executor.submit(
                    self.copy_file,
                    **{
                        "copy_from": from_path_prefix + copy_from,
                        "copy_to": to_path_prefix + copy_to,
                        "bucket_from": bucket_from or self.bucket,
                        "bucket_to": bucket_to or self.bucket,
                    },
                )
                future.id = (copy_from, copy_to)
                futures.append(future)
            results = {}
            for future in as_completed(futures):
                results[future.id] = future.result()
                self.delete_file(target=future.id[0], bucket=bucket_from or self.bucket)
            return results

    def copy_folder(
        self,
        copy_from: str,
        copy_to: str,
        bucket_from: str = "",
        bucket_to: str = "",
    ) -> List[dict]:
        """Copy a folder from one s3 location to another s3 location

        Args:
            copy_from (str): The s3 location to read from
            copy_to (str): The s3 location to write to
            bucket_from (str, optional): The bucket to read from.
                Defaults to '', which uses the default bucket.
            bucket_to (str, optional): The bucket to write to.
                Defaults to '', which uses the default bucket.

        Returns:
            dict: {
                'CopyObjectResult': {
                    'ETag': str,
                    'LastModified': datetime
                },
                'Expiration': str,
                'CopySourceVersionId': str,
                'VersionId': str,
                'ServerSideEncryption': str one of: 'AES256', 'aws:kms',
                'SSECustomerAlgorithm': str,
                'SSECustomerKeyMD5': str,
                'SSEKMSKeyId': str,
                'SSEKMSEncryptionContext': str,
                'BucketKeyEnabled': bool,
                'RequestCharged': str
            }
        """
        preamble_length = len(copy_from)
        copied_files = []
        for file in self.list_folder_contents(
            # list with empty delimiter reads through subfolders
            path=copy_from,
            bucket=bucket_from,
            delimiter="",
        ):
            copy_to = (
                f"{copy_to}/{file[preamble_length:]}"
                if not copy_to.endswith("/")
                else f"{copy_to}{file[preamble_length:]}"
            )
            copied_files.append(
                self.copy_file(
                    copy_from=file,
                    copy_to=copy_to,
                    bucket_from=bucket_from,
                    bucket_to=bucket_to,
                )
            )
        return copied_files

    def move_folder(
        self,
        move_from: str,
        move_to: str,
        bucket_from: str = "",
        bucket_to: str = "",
    ) -> str:
        self.copy_folder(
            copy_from=move_from,
            copy_to=move_to,
            bucket_from=bucket_from or self.bucket,
            bucket_to=bucket_to or self.bucket,
        )
        self.delete_folder(target=move_from, bucket=bucket_from or self.bucket)
        return self.list_folder_contents(path=move_to, bucket=bucket_to, delimiter="")

    def delete_file(self, target: str, bucket: str = "") -> dict:
        """Delete the file at s3 location target in bucket

        Args:
            target (str): the key to delete on s3
            bucket (str, optional): The bucket to look in.
                Defaults to '', which uses the default bucket.

        Returns:
            dict: {
                'ResponseMetadata': {
                    'RequestId': str,
                    'HostId': str,
                    'HTTPStatusCode': int (204 on success),
                    'HTTPHeaders': dict,
                    'CopyObjectResult': {
                        'ETag': str,
                        'LastModified': datetime
                    }
                }
        """
        return self.s3.delete_object(Bucket=bucket or self.bucket, Key=target)

    def delete_folder(
        self,
        target: str,
        bucket: str = "",
    ) -> dict:
        """Deletes the folder at target in bucket

        Args:
            target (str): [description]
            bucket (str, optional): [description]. Defaults to ''.

        Returns:
            dict: {
                'ResponseMetadata': {
                    'RequestId': str,
                    'HostId': str,
                    'HTTPStatusCode': int (200 on success),
                    'HTTPHeaders': dict,
                    'CopyObjectResult': {
                        'ETag': str,
                        'LastModified': datetime
                    }
                }
        """
        deleted_files = []
        for file in self.list_folder_contents(
            bucket=bucket or self.bucket, path=target, delimiter=""
        ):
            deleted_files.append(
                self.s3.delete_object(Bucket=bucket or self.bucket, Key=file)
            )
        return deleted_files

    def is_valid_s3_link(self, s3_link: str) -> bool:
        """Tests s3_link to see if it points to a valid bucket and key

        Args:
            s3_link (str): the s3 link in the form s3://{bucket}/{key}

        Returns:
            bool: bucket and key exist
        """
        try:
            bucket, s3_key = self.decompose_s3_uri(s3_link)
            bucket_test = self.check_bucket(bucket)
            key_test = (
                self.check_folder(path=s3_key, bucket=bucket)
                if s3_link.endswith("/")
                else self.check_file(path=s3_key, bucket=bucket)
            )
            return (bucket_test is not None) and (key_test is not None)
        except Exception:
            return False  # something failed validation

    def get_file_link(self, key: str, bucket: str = "", get_url: bool = False) -> str:
        """generates a link for a specific key in bucket

        Args:
            key (str): the file (s3 key)
            bucket (str, optional): the bucket.
                Defaults to '', which uses the default bucket.
            get_url (bool, optional): Return as URL format
                (ie https://... instead of s3://...). Defaults to False.

        Raises:
            Exception: Key and bucket failed existence check

        Returns:
            str: the s3 link (s3://...) or url (https://...) if get_url
        """
        s3_link = self.compose_s3_uri(bucket=bucket or self.bucket, key=key)
        if self.is_valid_s3_link(s3_link):
            return (
                self.compose_s3_url(bucket=bucket or self.bucket, key=key)
                if get_url
                else s3_link
            )
        raise Exception("Can not validate s3 link.")

    def get_presigned_url(
        self,
        operation: str,
        params: dict = {},
        expires: int = 3600,
        http_method: str = None,
    ) -> str:
        """generates a presigned url for a specific operation

        Args:
            operation (str): a client method to allow. ex `self.s3.put_object`
            params (dict, optional): the params to be used with the operation.
                Defaults to {}. reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
            expires (int, optional): how long in seconds the returned url will be valid for. Defaults to 3600.
            http_method (str, optional): the http method to be allowed. Defaults to None.

        Returns:
            str: the presigned url, valid for `expires` seconds
        """
        return self.s3.generate_presigned_url(
            ClientMethod=operation,
            Params=params,
            ExpiresIn=expires,
            HttpMethod=http_method,
        )

    def get_presigned_download_url(
        self, key: str, bucket: str = "", expires: int = 3600
    ) -> str:
        """generates a presigned url for downloading a file (s3.get_object)

        Args:
            key (str): the file-key to allow download for
            bucket (str, optional): the bucket to allow download from. Defaults to "",
                which uses the default bucket
            expires (int, optional): the number of seconds before the link expires.
                Defaults to 3600.

        Returns:
            str: the presigned url, valid for `expires` seconds
        """
        return self.get_presigned_url(
            "get_object",
            params={"Key": key, "Bucket": bucket or self.bucket},
            expires=expires,
            http_method="GET",
        )

    def get_presigned_post(
        self,
        key: str,
        bucket: str = "",
        fields: dict = {},
        conditions: List[str] = [],
        expires: int = 3600,
    ) -> dict:
        """generates a url and form fields for a presigned post operation
            ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.generate_presigned_post

        Args:
            key (str): the key to give the posted object
            bucket (str, optional): the name of the bucket to allow the post to.
                Defaults to '', which will try to use the default bucket.
            fields (dict, optional): prepopulated fields for the post request.
                Defaults to {}. allowed fields:
                    acl, Cache-Control, Content-Type, Content-Disposition,
                    Content-Encoding, Expires, success_action_redirect, redirect,
                    success_action_status, and x-amz-meta-
            conditions (List[str], optional): conditions to include in the policy.
                Defaults to []. Use dictionaries for key->value pairings, and lists for
                key to multiple values
            expires (int, optional): time in seconds this will be valid for. Defaults to 3600.

        Returns:
            dict: a dictionary with the url and fields for the presigned post request
        """
        return self.s3.generate_presigned_post(
            Bucket=bucket or self.bucket,
            Key=key,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=expires,
        )

    def key_exists(self, key: str, bucket: str = "") -> bool:
        """checks for the existence of a key in a bucket

        Args:
            key (str): the key to check (this looks like a file path)
            bucket (str, optional): the bucket to look in.
                Defaults to "", which will try to use the default bucket

        Returns:
            bool: True IFF head object does not raise an exception
        """
        try:
            self.s3.head_object(Bucket=bucket or self.bucket, Key=key)
        except Exception:
            return False
        return True

    @staticmethod
    def decompose_s3_uri(s3_link: str) -> Tuple[str, str]:
        """Take a properly formatted s3 link (s3://....) and break it into a bucket and key

        Args:
            s3_link (str): the s3 link to decompose

        Raises:
            Exception: the link is not properly formatted

        Returns:
            Tuple[str, str]: bucket, and key
        """
        if not s3_link:
            # guard against empty string
            return "", ""
        # remove whitespace
        s3_link = s3_link.strip()
        if not s3_link.startswith("s3://"):
            raise Exception(f"{s3_link} is not a valid s3 link")
        bucket = s3_link[5:].split("/")[0]
        s3key = "/".join(s3_link[5:].split("/")[1:])
        return bucket, s3key

    @staticmethod
    def compose_s3_uri(bucket: str, key: str) -> str:
        """creates an s3 link for provided bucket and key, does not verify existence

        Args:
            bucket (str): the bucket
            key (str): the key (s3 location/path)

        Returns:
            str: a formatted s3 link in the format: s3://{bucket}/{key}
        """
        return f"s3://{bucket}/{key}"

    @staticmethod
    def compose_s3_url(bucket: str, key: str) -> str:
        """generate an s3 url from a bucket and key
        # TODO: optional custom CDN, need to know how that behavior works
        Args:
            bucket (str): the bucket
            key (str): the key (s3 location)

        Returns:
            str: the formatted s3 url, should be navigable in a browser
        """
        return f"https://{bucket}.s3.amazonaws.com/{key}"  # TODO: check this formatting

    @staticmethod
    def is_file(key: str) -> bool:
        """tests whether a key represents a file or a folder

        Args:
            key (str): the key to check

        Returns:
            bool: True if this resource should be a file,
                False if this should point at a folder (ends with /)
        """
        return not key.endswith("/")
