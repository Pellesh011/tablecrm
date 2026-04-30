class IS3Client:

    async def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        raise NotImplementedError()

    async def upload_file_object(
        self, bucket_name: str, file_key: str, file_bytes: bytes
    ):
        raise NotImplementedError()

    async def create_bucket_if_not_exists(self, bucket_name: str):
        raise NotImplementedError()

    async def download_file(self, bucket_name: str, object_name: str, file_path: str):
        raise NotImplementedError()

    async def get_object(self, bucket_name: str, object_name: str) -> bytes:
        raise NotImplementedError()

    async def get_link_object(self, bucket_name: str, file_key: str):
        raise NotImplementedError()

    async def put_object(self, bucket_name: str, object_name: str, data: bytes):
        raise NotImplementedError()
