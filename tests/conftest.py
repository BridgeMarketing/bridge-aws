import os

import pytest
from moto import mock_s3

from s3 import S3


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "did you think this was real?"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "also not real"


@pytest.fixture
def s3_conn(aws_creds):
    with mock_s3():
        conn = S3()
        yield conn
