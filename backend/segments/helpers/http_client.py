import urllib.parse

import aiohttp


class HttpClient:
    """HTTP API клиент."""

    def __init__(self):
        """Инициализирует клиента."""
        self.session = aiohttp.ClientSession()

    async def _request(self, method, url, data=None, headers=None):
        """Отправляет запрос к API."""
        if headers is None:
            headers = {}
        elif not isinstance(headers, dict):
            raise ValueError("headers must be a dict")
        async with self.session.request(
            method, url, json=data, headers=headers, timeout=50
        ) as response:
            status_code = response.status
            try:
                data = await response.json()
                return status_code, data
            except Exception as e:
                print(e)
                return status_code, None

    async def get(self, url, headers=None):
        """Отправляет GET-запрос."""
        return await self._request("GET", url, headers=headers)

    async def post(self, url, data, headers=None):
        """Отправляет POST-запрос."""
        return await self._request("POST", url, data=data, headers=headers)

    async def patch(self, url, data=None, headers=None):
        """Отправляет PATCH-запрос."""
        return await self._request("PATCH", url, data=data, headers=headers)

    @staticmethod
    def _encode_url(url):
        """Кодирует URL, заменяя небезопасные символы на %xx."""
        return urllib.parse.quote(url, safe="")

    async def close(self):
        """Закрывает сессию клиента."""
        await self.session.close()

    async def __aenter__(self):
        """Вход в менеджер контекста."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из менеджера контекста."""
        await self.close()
