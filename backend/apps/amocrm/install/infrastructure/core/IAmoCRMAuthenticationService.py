from apps.amocrm.install.infrastructure.impl.models.AmoCRMAuthenticationResultModel import (
    AmoCRMAuthenticationResultModel,
)


class IAmoCRMAuthenticationService:

    async def authenticate(self, code: str) -> AmoCRMAuthenticationResultModel:
        raise NotImplementedError()
