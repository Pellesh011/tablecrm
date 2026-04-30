from typing import Optional

from fastapi import Form
from fastapi.param_functions import Form
from fastapi.security import OAuth2PasswordRequestForm


class OAuthCustomRequestForm(OAuth2PasswordRequestForm):
    def __init__(
        self,
        custom_token: Optional[str] = Form(None),
        client_id: Optional[int] = Form(None),
        client_secret: Optional[str] = Form(None),
    ):
        self.custom_token = custom_token
        self.client_id = client_id
        self.client_secret = client_secret
