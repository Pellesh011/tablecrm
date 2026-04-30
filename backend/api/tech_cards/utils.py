from database.db import users_cboxes_relation
from sqlalchemy.orm import Session

from .models import TechCardDB


async def _tech_cards_cashbox_scope_query(db: Session, cashbox_id: int):
    """
    Метод для получение тех карты только для определенного cashbox, чтобы чужие кассы
    не получали чужие данные.
    """
    return (
        db.query(TechCardDB)
        .join(users_cboxes_relation, TechCardDB.user_id == users_cboxes_relation.c.id)
        .filter(
            TechCardDB.status != "deleted",
            users_cboxes_relation.c.cashbox_id == cashbox_id,
        )
    )
