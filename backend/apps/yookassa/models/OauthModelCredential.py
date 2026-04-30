from pydantic import BaseModel


class OauthModelCredential(BaseModel):
    client_id: str
    client_secret: str
    cashbox: int = None
