from abc import ABC, abstractmethod
from typing import List, Union

from common.geocoders.schemas import GeocoderSearchResponse


class BaseGeocoder(ABC):
    @abstractmethod
    async def autocomplete(self, text: str, limit=5) -> Union[List[str], List]:
        pass

    @abstractmethod
    async def validate_address(
        self, address: str, limit=1
    ) -> Union[GeocoderSearchResponse, None]:
        pass

    async def get_location_by_ip(self, ip: str) -> Union[GeocoderSearchResponse, None]:
        """Определение местоположения по IP адресу"""
        # Реализация по умолчанию - переопределяется в наследниках
        return None
