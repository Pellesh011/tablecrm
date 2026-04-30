import functools
import logging

from asyncpg.exceptions import InterfaceError
from database.db import database

logger = logging.getLogger(__name__)


def ensure_db_connection(func):
    """
    Декоратор для фоновых задач (Jobs).
    Гарантирует наличие подключения и обрабатывает ошибки закрытия пула.
    """

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            if not database.is_connected:
                await database.connect()

            return await func(*args, **kwargs)

        except InterfaceError as e:
            if "pool is closing" in str(e):
                logger.info(
                    f"Task '{func.__name__}' interrupted: Database pool is closing."
                )
                return
            logger.error(
                f"Database interface error in '{func.__name__}': {e}", exc_info=True
            )

        except Exception as e:
            logger.error(f"Critical error in job '{func.__name__}': {e}", exc_info=True)

    return wrapper
