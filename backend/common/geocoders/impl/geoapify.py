import asyncio
from typing import Union

import aiohttp
from common.geocoders.core.base_instance import BaseGeocoder
from common.geocoders.schemas import GeocoderSearchResponse
from common.geocoders.utils import AsyncLRU


class Geoapify(BaseGeocoder):
    _instance = None
    _session: Union[aiohttp.ClientSession, None] = None
    _lock = asyncio.Lock()

    def __new__(cls, api_key: Union[str, None] = None):
        if cls._instance is None:
            if not api_key:
                raise ValueError("Geoapify API key is required")
            cls._instance = super().__new__(cls)
            cls._instance.api_key = api_key
            cls._instance.autocomplete_url = (
                "https://api.geoapify.com/v1/geocode/autocomplete"
            )
            cls._instance.search_url = "https://api.geoapify.com/v1/geocode/search"
            cls._instance.autocomplete_cache = AsyncLRU()
            cls._instance.search_cache = AsyncLRU()
            # Для IP-геолокации не кэшируем None, чтобы восстановиться после временного сбоя
            cls._instance.ip_cache = AsyncLRU(cache_none=False)
        return cls._instance

    async def _get_session(self):
        async with self._lock:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def autocomplete(self, text: str, limit=5) -> Union[list[str], list]:
        return await self.autocomplete_cache.get(
            key=text, func=self._autocomplete, text=text, limit=limit
        )

    async def _autocomplete(self, text, limit=5) -> Union[list[str], list]:
        try:
            session = await self._get_session()
            params = {
                "text": text,
                "apiKey": self.api_key,
                "limit": limit,
                "lang": "ru",
            }
            async with session.get(self.autocomplete_url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return [f["properties"]["formatted"] for f in data.get("features", [])]
        except aiohttp.ClientError as e:
            return []

    async def validate_address(
        self, address: str, limit=1
    ) -> Union[GeocoderSearchResponse, None]:
        return await self.search_cache.get(
            address, func=self._validate_address, address=address, limit=limit
        )

    async def _validate_address(
        self, address: str, limit=1
    ) -> Union[GeocoderSearchResponse, None]:
        try:
            session = await self._get_session()
            params = {
                "text": address,
                "apiKey": self.api_key,
                "limit": limit,
                "lang": "ru",
            }
            async with session.get(self.search_url, params=params) as resp:
                resp.raise_for_status()
                resp = await resp.json()
                return self._parse_response(resp)
        except aiohttp.ClientError as e:
            return None

    def _parse_response(self, resp) -> Union[GeocoderSearchResponse, None]:
        features = resp.get("features")
        if features:
            properties = features[0].get("properties")
            return GeocoderSearchResponse(
                country=properties.get("country"),
                state=properties.get("state"),
                city=properties.get("city"),
                street=properties.get("street"),
                housenumber=properties.get("housenumber"),
                timezone=properties.get("timezone", {}).get("name"),
                postcode=properties.get("postcode"),
                latitude=properties.get("lat"),
                longitude=properties.get("lon"),
            )
        else:
            return None

    async def get_location_by_ip(self, ip: str) -> Union[GeocoderSearchResponse, None]:
        """Определение местоположения по IP адресу"""
        return await self.ip_cache.get(key=ip, func=self._get_location_by_ip, ip=ip)

    async def _get_location_by_ip(self, ip: str) -> Union[GeocoderSearchResponse, None]:
        """Внутренний метод для определения местоположения по IP"""
        try:
            session = await self._get_session()
            # Используем ip-api.com для определения по IP (бесплатный сервис)
            # Geoapify не поддерживает определение по IP напрямую
            url = f"http://ip-api.com/json/{ip}?lang=ru&fields=status,message,country,regionName,city,lat,lon,timezone"
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json()

                # ip-api.com возвращает другой формат
                if data.get("status") == "success":
                    return GeocoderSearchResponse(
                        country=data.get("country"),
                        state=data.get("regionName"),
                        city=data.get("city"),
                        street=None,
                        housenumber=None,
                        timezone=data.get("timezone"),
                        postcode=None,
                        latitude=data.get("lat"),
                        longitude=data.get("lon"),
                    )
                else:
                    return None
        except aiohttp.ClientError as e:
            return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
