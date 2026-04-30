import io

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from ..core.IS3Client import IS3Client
from ..models.S3SettingsModel import S3SettingsModel


class S3Client(IS3Client):
    def __init__(self, s3_settings: S3SettingsModel):
        self.__s3_settings = s3_settings
        self.session = aioboto3.Session(
            aws_access_key_id=s3_settings.aws_access_key_id,
            aws_secret_access_key=s3_settings.aws_secret_access_key,
        )

    async def create_bucket_if_not_exists(self, bucket_name: str):
        async with self.session.client(
            "s3",
            endpoint_url=self.__s3_settings.endpoint_url,
            config=Config(
                signature_version="s3v4", s3={"addressing_style": "path"}  # 🔥 ВАЖНО
            ),
        ) as s3_client:
            try:
                await s3_client.create_bucket(Bucket=bucket_name)
            except ClientError as e:
                if e.response["Error"]["Code"] in (
                    "BucketAlreadyExists",
                    "BucketAlreadyOwnedByYou",
                ):
                    pass
                else:
                    raise

    async def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        await self.create_bucket_if_not_exists(bucket_name)
        async with self.session.client(
            "s3", endpoint_url=self.__s3_settings.endpoint_url
        ) as s3_client:
            await s3_client.upload_file(file_path, bucket_name, object_name)

    async def upload_file_object(
        self, bucket_name: str, file_key: str, file_bytes: bytes
    ):
        await self.create_bucket_if_not_exists(bucket_name)
        async with self.session.client(
            "s3", endpoint_url=self.__s3_settings.endpoint_url
        ) as s3_client:
            await s3_client.upload_fileobj(
                io.BytesIO(file_bytes), bucket_name, file_key
            )

    async def download_file(self, bucket_name: str, object_name: str, file_path: str):
        async with self.session.client(
            "s3", endpoint_url=self.__s3_settings.endpoint_url
        ) as s3_client:
            await s3_client.download_file(bucket_name, object_name, file_path)

    async def get_object(self, bucket_name: str, object_name: str) -> bytes:
        async with self.session.client(
            "s3", endpoint_url=self.__s3_settings.endpoint_url
        ) as s3_client:
            response = await s3_client.get_object(Bucket=bucket_name, Key=object_name)
            data = await response["Body"].read()
            return data

    async def get_link_object(self, bucket_name: str, file_key: str):
        async with self.session.client(
            "s3", endpoint_url=self.__s3_settings.endpoint_url
        ) as s3_client:
            url = await s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": file_key},
                ExpiresIn=None,
            )
            return url

    async def put_object(self, bucket_name: str, object_name: str, data: bytes):
        await self.create_bucket_if_not_exists(bucket_name)
        async with self.session.client(
            "s3", endpoint_url=self.__s3_settings.endpoint_url
        ) as s3_client:
            await s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
