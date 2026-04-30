from pydantic import BaseModel


class AmoCRMAuthenticationResultModel(BaseModel):
    access_token: str
    refresh_token: str
    amo_domain: str
    expires_in: int
