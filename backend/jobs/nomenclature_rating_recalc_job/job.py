import logging

from api.nomenclature.utils import recalc_nomenclature_rating
from common.decorators import ensure_db_connection
from database.db import database, nomenclature
from sqlalchemy import select

logger = logging.getLogger(__name__)


@ensure_db_connection
async def recalc_all_nomenclature_ratings(batch_size: int = 500) -> None:
    """
    Полный фоновый пересчет рейтинга всех актуальных номенклатур.
    Запускается по расписанию (cron/interval)
    """
    logger.info("Старт полного фонового пересчета рейтинга номенклатур")
    last_id = 0
    processed = 0

    while True:
        rows = await database.fetch_all(
            select(nomenclature.c.id)
            .where(
                nomenclature.c.id > last_id,
                nomenclature.c.is_deleted.is_not(True),
            )
            .order_by(nomenclature.c.id.asc())
            .limit(batch_size)
        )

        if not rows:
            break

        for row in rows:
            nom_id = row.id
            try:
                await recalc_nomenclature_rating(nom_id)
                processed += 1
            except Exception:
                logger.error(
                    "Не удалось пересчитать рейтинг номенклатуры: nomenclature_id=%s",
                    nom_id,
                    exc_info=True,
                )
            last_id = nom_id

    logger.info(
        "Завершен полный пересчет рейтинга номенклатур, обработано: %s", processed
    )
