from database.db import database, entity_to_entity, payments
from sqlalchemy import and_, func, select


class PaymentsRepository:
    @staticmethod
    async def fetch_payments_map_by_ids(
        doc_ids: list[int],
    ) -> dict[tuple[int, int], dict]:
        if not doc_ids:
            return {}

        query = (
            select(
                entity_to_entity.c.from_id.label("doc_id"),
                entity_to_entity.c.cashbox_id,
                func.sum(payments.c.amount).label("total_amount"),
            )
            .join(payments, payments.c.id == entity_to_entity.c.to_id)
            .where(
                and_(
                    entity_to_entity.c.from_entity == 7,
                    entity_to_entity.c.to_entity == 5,
                    entity_to_entity.c.from_id.in_(doc_ids),
                    payments.c.status.is_(True),
                    payments.c.is_deleted.is_(False),
                )
            )
            .group_by(entity_to_entity.c.from_id, entity_to_entity.c.cashbox_id)
        )

        rows = await database.fetch_all(query)
        return {
            (row["doc_id"], row["cashbox_id"]): {"total_amount": row["total_amount"]}
            for row in rows
        }

    async def fetch_payments_map_by_id(
        self, doc_id: int
    ) -> dict[tuple[int, int], dict]:
        return await self.fetch_payments_map_by_ids([doc_id])
