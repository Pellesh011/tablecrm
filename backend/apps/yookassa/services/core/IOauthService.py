class IOauthService:
    async def oauth_link(self, cashbox: int, warehouse: int, token: str):
        raise NotImplementedError

    async def revoke_token(self, cashbox: str, warehouse: int):
        raise NotImplementedError

    async def get_access_token(self, code: str, cashbox: int, warehouse: int):
        raise NotImplementedError

    async def get_install_oauth_by_user(self, cashbox: int):
        raise NotImplementedError

    async def validation_oauth(self, cashbox: int, warehouse: int):
        raise NotImplementedError
