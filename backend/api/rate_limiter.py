from common.redis_utils import get_redis_uri
from fastapi import Request
from slowapi import Limiter
from slowapi.util import get_remote_address


def get_cashbox_id_from_token(request: Request) -> str:
    token = request.query_params.get("token") or ""
    if token:
        return token[:16]
    return get_remote_address(request)


limiter = Limiter(
    key_func=get_cashbox_id_from_token,
    storage_uri=get_redis_uri(),
    default_limits=["200/minute"],
    in_memory_fallback_enabled=True,
)

HEAVY_ENDPOINTS_LIMITS = {
    "/analytics": "10/minute",
    "/analytics_cards": "5/minute",
    "/gross_profit_docs": "5/minute",
    "/reports": "5/minute",
    "/segment": "10/minute",
    "/chats": "100/minute",
    "/loyality_transactions": "50/minute",
}
