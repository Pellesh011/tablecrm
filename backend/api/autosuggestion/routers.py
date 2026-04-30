from api.autosuggestion.schemas import AutosuggestResponse
from common.geocoders.instance import geocoder
from common.geocoders.schemas import GeocoderSearchResponse
from fastapi import APIRouter

router = APIRouter(prefix="/autosuggestions", tags=["autosuggestions"])


@router.get("/geolocation", response_model=AutosuggestResponse)
async def autosuggest_location(query: str, limit: int = 5):
    suggestions = await geocoder.autocomplete(query, limit=limit)
    return AutosuggestResponse(suggestions=suggestions)


@router.get("/geolocation/validate", response_model=GeocoderSearchResponse)
async def validate_address(address: str):
    """Валидация адреса и получение координат"""
    result = await geocoder.validate_address(address, limit=1)
    if result:
        return result
    return GeocoderSearchResponse(
        country=None,
        state=None,
        city=None,
        street=None,
        housenumber=None,
        timezone=None,
        postcode=None,
        latitude=None,
        longitude=None,
    )
