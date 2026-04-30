from pydantic.main import BaseModel


class AmoInstallInfoModel(BaseModel):
    id: int
    referrer: str
    access_token: str
    group_id: int
