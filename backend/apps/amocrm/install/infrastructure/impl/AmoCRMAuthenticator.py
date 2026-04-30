import aiohttp
from apps.amocrm.install.infrastructure.core.IAmoCRMAuthenticationService import (
    IAmoCRMAuthenticationService,
)
from apps.amocrm.install.infrastructure.impl.models.AmoCRMAuthenticationResultModel import (
    AmoCRMAuthenticationResultModel,
)


class AmoCRMAuthenticator(IAmoCRMAuthenticationService):
    def __init__(
        self,
        http_client: aiohttp.ClientSession,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        amo_domain: str,
    ):
        self.http_client = http_client
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.amo_domain = amo_domain

    async def authenticate(self, code: str) -> AmoCRMAuthenticationResultModel:
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }

        async with self.http_client.post(
            f"https://{self.amo_domain}/oauth2/access_token", json=params
        ) as response:
            data = await response.json()
            if "access_token" in data:
                return AmoCRMAuthenticationResultModel(
                    access_token=data["access_token"],
                    refresh_token=data["refresh_token"],
                    amo_domain=self.amo_domain,
                    expires_in=int(data["expires_in"]),
                )
            else:
                print(data)
                raise ValueError("Authentication failed")
