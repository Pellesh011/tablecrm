import base64
import socket
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

import aiohttp
from pydantic import BaseModel


class SearchTypes(str, Enum):
    ru = "SEARCH_TYPE_RU"


class RequestQuery(BaseModel):
    search_type: SearchTypes = SearchTypes.ru
    queryText: str


class RequestModel(BaseModel):
    query: RequestQuery
    userAgent: Optional[str]


class IImageResearcher(ABC):

    async def _request(
        self,
        url: str,
        headers: dict = None,
        json_payload: dict = None,
        data: str = None,
    ):

        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            if json_payload is not None:
                async with session.post(
                    url, json=json_payload, headers=headers
                ) as response:
                    payload = await response.json()
            else:
                async with session.get(
                    url, data=data or {}, headers=headers
                ) as response:
                    payload = await response.json()

        return payload

    @abstractmethod
    async def search(self, text_query: str):
        pass


class YandexImageResearcher(IImageResearcher):
    def __init__(self, api_token: str):
        self.__YANDEX_SEARCH_API_BASE_URL = (
            "https://searchapi.api.cloud.yandex.net/v2/image/search"
        )
        self.__token = api_token

    async def __image_to_bytes(self, url: str) -> Optional[bytes]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        except Exception as e:
            return None

    async def search(self, text_query: str) -> Optional[bytes]:
        """
        Ищет изображение по текстовому описанию с помощью yandex search api

        :param text_query: Текстовый запрос
        Returns:
            bytes: строка байт
        """
        query = RequestQuery(queryText=text_query)
        request = RequestModel(query=query)

        response = await self._request(
            self.__YANDEX_SEARCH_API_BASE_URL,
            json_payload=request.dict(exclude_none=True),
            headers={"Authorization": f"Bearer {self.__token}"},
        )

        raw_data = response.get("rawData")
        if not raw_data:
            return None

        xml_root = ET.fromstring(base64.b64decode(raw_data))
        img_url_elem = xml_root.find(".//image-link")
        return await self.__image_to_bytes(img_url_elem.text)
