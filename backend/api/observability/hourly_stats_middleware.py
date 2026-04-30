import asyncio
import datetime
import json
import time
from typing import Optional

import redis.asyncio as aioredis
from common.redis_utils import get_redis_uri
from starlette.middleware.base import BaseHTTPMiddleware

_redis_client: Optional[aioredis.Redis] = None


async def _get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.from_url(get_redis_uri(), socket_connect_timeout=2)
    return _redis_client


class HourlyStatsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed = (time.perf_counter() - start) * 1000

        # Не ждём Redis — fire-and-forget
        asyncio.create_task(self._record_stats(request, response, elapsed))
        return response

    async def _record_stats(self, request, response, elapsed_ms):
        try:
            r = await _get_redis()
            hour_key = datetime.utcnow().strftime("hourly:%Y-%m-%d:%H")
            async with r.pipeline(transaction=False) as pipe:
                pipe.hincrby(hour_key, "requests", 1)
                pipe.hincrbyfloat(hour_key, "total_ms", round(elapsed_ms, 1))
                if response.status_code >= 500:
                    pipe.hincrby(hour_key, "errors_5xx", 1)
                elif response.status_code >= 400:
                    pipe.hincrby(hour_key, "errors_4xx", 1)
                if elapsed_ms > 2000:
                    pipe.hincrby(hour_key, "slow_requests", 1)
                    pipe.lpush(
                        "slow_requests_log",
                        json.dumps(
                            {
                                "path": request.url.path,
                                "ms": round(elapsed_ms, 1),
                                "ts": datetime.utcnow().isoformat(),
                            }
                        ),
                    )
                    pipe.ltrim("slow_requests_log", 0, 999)
                pipe.expire(hour_key, 86400 * 7)
                await pipe.execute()
        except Exception:
            pass
