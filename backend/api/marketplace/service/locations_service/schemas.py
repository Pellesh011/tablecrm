from typing import List, Optional

from pydantic import BaseModel


class MarketplaceLocation(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description: Optional[str] = None
    distance: Optional[float] = None
    avg_rating: Optional[float] = None
    reviews_count: Optional[int] = 0


class LocationsListRequest(BaseModel):
    lat: Optional[float] = None
    lon: Optional[float] = None
    radius: Optional[float] = None
    address: Optional[str] = None
    city: Optional[str] = None
    page: int = 1
    size: int = 20


class LocationsListResponse(BaseModel):
    locations: List[MarketplaceLocation]
    count: int
    page: int
    size: int
