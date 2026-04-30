import logging
import time
from datetime import datetime

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("api")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        response = None
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as exc:
            logger.error(
                "unhandled_exception",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "error": str(exc),
                    "type": type(exc).__name__,
                },
                exc_info=True,
            )
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            level = (
                logging.ERROR
                if status_code >= 500
                else (logging.WARNING if status_code >= 400 else logging.INFO)
            )
            logger.log(
                level,
                "request",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "status": status_code,
                    "duration_ms": round(duration_ms, 1),
                    "slow": duration_ms > 2000,
                    "ts": datetime.utcnow().isoformat(),
                },
            )
        return response
