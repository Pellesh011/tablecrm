class AmoCRMAuthenticationResult:
    def __init__(
        self, access_token: str, refresh_token: str, amo_domain: str, expires_in: int
    ):
        self.access_token = access_token
        self.expires_in = expires_in
        self.amo_domain = amo_domain
        self.refresh_token = refresh_token
