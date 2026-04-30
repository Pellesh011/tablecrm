import logging

from common.decorators import ensure_db_connection
from database.db import database, docs_warehouse
from sqlalchemy import select, text

logger = logging.getLogger(__name__)

RENUMBER_SQL = """
UPDATE docs_warehouse dw
SET number = sub.rn::text
FROM (
    SELECT id, number, ROW_NUMBER() OVER (ORDER BY id) AS rn
    FROM docs_warehouse
    WHERE cashbox = :cashbox_id AND is_deleted = false
) sub
WHERE dw.id = sub.id AND sub.number IS NULL
"""


@ensure_db_connection
async def renumber_docs_warehouse() -> None:
    """
    Перенумерация документов
    """
    cashbox_rows = await database.fetch_all(
        select(docs_warehouse.c.cashbox)
        .where(
            docs_warehouse.c.cashbox.is_not(None),
            docs_warehouse.c.is_deleted.is_not(True),
            docs_warehouse.c.number.is_(None),
        )
        .distinct()
    )

    if not cashbox_rows:
        return

    for row in cashbox_rows:
        query = text(RENUMBER_SQL).bindparams(cashbox_id=row["cashbox"])
        await database.execute(query)

    logger.info(
        "docs_warehouse renumber job completed for %s cashboxes",
        len(cashbox_rows),
    )
