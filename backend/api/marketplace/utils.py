from typing import Any, Dict, Optional

from api.marketplace.service.service import MarketplaceService


def safe_parse_json(value: Any) -> Optional[Dict[str, Any]]:
    try:
        if value is None:
            return None
        if isinstance(value, dict):
            return value  # already parsed
        import json

        return json.loads(value)
    except Exception:
        return None


async def get_marketplace_service() -> MarketplaceService:
    marketplace_service = MarketplaceService()
    await marketplace_service.connect()
    return marketplace_service
