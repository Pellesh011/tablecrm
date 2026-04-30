from typing import Union

from pydantic import BaseModel


class GeocoderSearchResponse(BaseModel):
    country: Union[str, None]
    state: Union[str, None]
    city: Union[str, None]
    street: Union[str, None]
    housenumber: Union[str, None]
    timezone: Union[str, None]
    postcode: Union[str, None]
    latitude: Union[float, None]
    longitude: Union[float, None]
