import os

from apps.yookassa.functions.core.IGetOauthCredentialFunction import (
    IGetOauthCredentialFunction,
)


class GetOauthCredentialFunction(IGetOauthCredentialFunction):

    def __call__(self):
        return os.getenv("YOOKASSA_OAUTH_APP_CLIENT_ID"), os.getenv(
            "YOOKASSA_OAUTH_APP_CLIENT_SECRET"
        )
