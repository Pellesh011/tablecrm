"""
api/chats/max/max_polling_worker.py

Фоновый воркер для получения обновлений через long-polling Max Bot API.
Используется как альтернатива webhook (например, в dev-окружении или если
сервер недоступен из интернета).

Запуск:
    python3 -m api.chats.max.max_polling_worker
"""

import asyncio
import logging
import os
from typing import Dict, Optional, Tuple

from api.chats.avito.avito_factory import _decrypt_credential
from api.chats.max.max_client import MaxAPIError, MaxClient
from api.chats.max.max_handler import handle_update
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings
from common.utils.ioc.ioc import ioc
from database.db import channel_credentials, channels, database
from sqlalchemy import and_, select

logger = logging.getLogger(__name__)

POLL_TIMEOUT = int(os.getenv("MAX_POLL_TIMEOUT", "25"))
POLL_LIMIT = int(os.getenv("MAX_POLL_LIMIT", "100"))
POLL_ERROR_SLEEP = float(os.getenv("MAX_POLL_ERROR_SLEEP", "5"))
REFRESH_INTERVAL = int(os.getenv("MAX_POLL_REFRESH_INTERVAL", "60"))


_markers: Dict[str, Optional[int]] = {}
_tasks: Dict[str, asyncio.Task] = {}
_task_meta: Dict[str, Tuple[int, int]] = {}


async def _ensure_rabbitmq() -> None:
    """Инициализировать RabbitMQ-фабрику в процессе воркера."""
    try:
        ioc.get(IRabbitFactory)
        return
    except AttributeError:
        pass
    factory = RabbitFactory(
        settings=RabbitMqSettings(
            rabbitmq_host=os.getenv("RABBITMQ_HOST"),
            rabbitmq_user=os.getenv("RABBITMQ_USER"),
            rabbitmq_pass=os.getenv("RABBITMQ_PASS"),
            rabbitmq_port=int(os.getenv("RABBITMQ_PORT", "5672")),
            rabbitmq_vhost=os.getenv("RABBITMQ_VHOST") or "/",
        )
    )
    ioc.set(IRabbitFactory, await factory())
    logger.info("[MaxPolling] RabbitMQ factory initialized")


async def _fetch_max_credentials() -> Dict[str, Dict[str, int]]:
    """Загрузить активные токены Max-ботов из БД."""
    query = (
        select(
            [
                channel_credentials.c.channel_id,
                channel_credentials.c.cashbox_id,
                channel_credentials.c.api_key,
            ]
        )
        .select_from(
            channels.join(
                channel_credentials,
                channels.c.id == channel_credentials.c.channel_id,
            )
        )
        .where(
            and_(
                channels.c.type == "MAX",
                channels.c.is_active.is_(True),
                channel_credentials.c.is_active.is_(True),
            )
        )
    )
    rows = await database.fetch_all(query)
    result: Dict[str, Dict[str, int]] = {}
    for row in rows:
        encrypted_token = row["api_key"]
        if not encrypted_token:
            continue
        try:
            token = _decrypt_credential(encrypted_token)
        except Exception as exc:
            logger.warning("[MaxPolling] Failed to decrypt token: %s", exc)
            continue
        if token in result:
            logger.warning("[MaxPolling] Duplicate Max token for multiple channels")
            continue
        result[token] = {
            "channel_id": row["channel_id"],
            "cashbox_id": row["cashbox_id"],
        }
    return result


async def _poll_bot_forever(token: str, channel_id: int, cashbox_id: int) -> None:
    """Бесконечный цикл long-polling для одного бота."""
    client = MaxClient(token)

    try:
        await client.delete_webhook()
    except Exception as exc:
        logger.warning("[MaxPolling] delete_webhook for token %s: %s", token[:10], exc)

    while True:
        try:
            marker = _markers.get(token)
            data = await client.get_updates(
                marker=marker,
                timeout=POLL_TIMEOUT,
                limit=POLL_LIMIT,
            )
            updates = data.get("updates") or []
            new_marker = data.get("marker")
            if new_marker is not None:
                _markers[token] = new_marker

            logger.info(
                "[MaxPolling] %d updates for token %s…",
                len(updates),
                token[:10],
            )
            for upd in updates:
                try:
                    await handle_update(upd, channel_id, cashbox_id, token)
                except Exception as exc:
                    logger.exception(
                        "[MaxPolling] Error handling update %s: %s", upd, exc
                    )

        except MaxAPIError as exc:
            logger.warning("[MaxPolling] API error for token %s: %s", token[:10], exc)
            await asyncio.sleep(POLL_ERROR_SLEEP)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "[MaxPolling] Unexpected error for token %s: %s", token[:10], exc
            )
            await asyncio.sleep(POLL_ERROR_SLEEP)


async def _sync_tasks() -> None:
    """Синхронизировать активные задачи с данными из БД."""
    credentials = await _fetch_max_credentials()
    desired = set(credentials.keys())
    active = set(_tasks.keys())

    for token in active - desired:
        task = _tasks.pop(token)
        _task_meta.pop(token, None)
        task.cancel()
        logger.info("[MaxPolling] Stopped polling for removed token %s…", token[:10])

    for token, data in credentials.items():
        meta = (data["channel_id"], data["cashbox_id"])
        if token in _tasks and _task_meta.get(token) == meta:
            continue
        if token in _tasks:
            _tasks[token].cancel()
        _task_meta[token] = meta
        _tasks[token] = asyncio.create_task(
            _poll_bot_forever(token, data["channel_id"], data["cashbox_id"])
        )
        logger.info("[MaxPolling] Started polling for token %s…", token[:10])


async def run_polling_forever(manage_db: bool = True) -> None:
    """Главный цикл воркера."""
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    await _ensure_rabbitmq()
    if manage_db:
        await database.connect()
    try:
        while True:
            await _sync_tasks()
            await asyncio.sleep(REFRESH_INTERVAL)
    finally:
        for task in _tasks.values():
            task.cancel()
        if manage_db:
            await database.disconnect()


async def main() -> None:
    await run_polling_forever(manage_db=True)


if __name__ == "__main__":
    asyncio.run(main())
