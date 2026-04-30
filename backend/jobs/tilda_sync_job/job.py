"""
Джоб для периодической синхронизации фидов с Tilda
"""

import logging

from common.decorators import ensure_db_connection
from database.db import database, feeds
from sqlalchemy import and_, func, or_, select

logger = logging.getLogger(__name__)


async def get_feeds_to_sync():
    """
    Получает список фидов, которые нужно синхронизировать с Tilda.

    Возвращает фиды, у которых:
    - tilda_sync_enabled = True
    - есть все необходимые данные (tilda_url, tilda_username, tilda_password)
    - прошло время для синхронизации (tilda_sync_interval)
    """
    query = select(feeds.c.id).where(
        and_(
            feeds.c.tilda_sync_enabled == True,
            feeds.c.tilda_url.isnot(None),
            feeds.c.tilda_username.isnot(None),
            feeds.c.tilda_password.isnot(None),
            feeds.c.tilda_sync_interval.isnot(None),
            # Проверяем, что прошло время для синхронизации
            or_(
                feeds.c.updated_at.is_(None),
                # Если updated_at + interval < now(), то нужно синхронизировать
                func.now()
                >= feeds.c.updated_at
                + func.make_interval(0, 0, 0, 0, 0, feeds.c.tilda_sync_interval),
            ),
        )
    )
    rows = await database.fetch_all(query)
    return [row.id for row in rows]


@ensure_db_connection
async def sync_tilda_feeds():
    """
    Основная функция джоба для синхронизации фидов с Tilda.
    Находит все фиды, которые нужно синхронизировать, и отправляет их в Tilda.
    """
    try:
        logger.info("Tilda sync job tick")
        feed_ids = await get_feeds_to_sync()

        if not feed_ids:
            logger.info("No feeds to sync with Tilda")
            return

        logger.info(f"Found {len(feed_ids)} feeds to sync with Tilda: {feed_ids}")

        # Импортируем функцию синхронизации
        from api.feeds.tilda_sync import sync_feed_to_tilda_by_id

        for feed_id in feed_ids:
            try:
                logger.info(f"Syncing feed {feed_id} with Tilda...")
                result = await sync_feed_to_tilda_by_id(feed_id)

                if result.get("success"):
                    logger.info(f"Successfully synced feed {feed_id} with Tilda")
                else:
                    logger.error(
                        f"Failed to sync feed {feed_id} with Tilda: {result.get('error', 'Unknown error')}"
                    )
            except Exception as e:
                logger.error(
                    f"Error syncing feed {feed_id} with Tilda: {str(e)}", exc_info=True
                )

    except Exception as e:
        logger.error(f"Error in sync_tilda_feeds job: {str(e)}", exc_info=True)
