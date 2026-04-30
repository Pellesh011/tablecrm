import aioboto3

from ..core.IS3Client import IS3Client
from ..core.IS3ServiceFactory import IS3ServiceFactory
from ..models.S3SettingsModel import S3SettingsModel
from .S3Client import S3Client


class S3ServiceFactory(IS3ServiceFactory):
    def __init__(self, s3_settings: S3SettingsModel):
        self.__s3_settings = s3_settings
        self._session = aioboto3.Session(
            aws_access_key_id=s3_settings.aws_access_key_id,
            aws_secret_access_key=s3_settings.aws_secret_access_key,
        )

    def __call__(self) -> IS3Client:
        return S3Client(s3_settings=self.__s3_settings)
