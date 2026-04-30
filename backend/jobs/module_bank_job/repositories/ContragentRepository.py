from database.db import async_session_maker, contragents
from sqlalchemy import and_


class ContragentRepository:

    async def get_contragent_by_inn(self, cashbox_id: int, inn: str):
        async with async_session_maker() as session:
            query = contragents.select().where(
                and_(contragents.c.inn == inn), contragents.c.cashbox == cashbox_id
            )
            result = await session.execute(query)
            contragent_db = result.fetchone()
        return contragent_db
