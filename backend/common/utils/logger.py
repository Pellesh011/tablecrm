import logging
import time

logger = logging.getLogger("storage")
_last_quota_log = 0
_LOG_INTERVAL = 2 * 60 * 60


def log_quota_exceeded():
    global _last_quota_log
    now = time.time()
    if now - _last_quota_log > _LOG_INTERVAL:
        logger.error("хранилище заполнено")
        _last_quota_log = now
