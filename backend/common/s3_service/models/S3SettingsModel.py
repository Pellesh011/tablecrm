from pydantic import BaseModel


class S3SettingsModel(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str
