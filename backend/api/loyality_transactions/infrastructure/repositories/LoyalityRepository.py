from database.db import database, entity_to_entity, loyality_transactions
from sqlalchemy import Integer, and_, case, cast, func, select


class LoyalityRepository:
    @staticmethod
    async def fetch_loyality_map_by_ids(
        doc_ids: list[int],
    ) -> dict[tuple[int, int], dict]:
        if not doc_ids:
            return {}

        query = (
            select(
                entity_to_entity.c.from_id.label("doc_id"),
                entity_to_entity.c.cashbox_id,
                func.sum(loyality_transactions.c.amount).label("total_amount"),
                func.count(
                    case((entity_to_entity.c.delinked.isnot(True), cast(1, Integer)))
                ).label("has_link"),
            )
            .join(
                loyality_transactions,
                loyality_transactions.c.id == entity_to_entity.c.to_id,
            )
            .where(
                and_(
                    entity_to_entity.c.from_entity == 7,
                    entity_to_entity.c.to_entity == 6,
                    entity_to_entity.c.from_id.in_(doc_ids),
                    loyality_transactions.c.status.is_(True),
                    loyality_transactions.c.is_deleted.is_(False),
                )
            )
            .group_by(entity_to_entity.c.from_id, entity_to_entity.c.cashbox_id)
        )

        rows = await database.fetch_all(query)

        return {
            (row["doc_id"], row["cashbox_id"]): {
                "total_amount": row["total_amount"],
                "has_link": row["has_link"],
            }
            for row in rows
        }

    async def fetch_loyality_map_by_id(
        self, doc_id: int
    ) -> dict[tuple[int, int], dict]:
        return await self.fetch_loyality_map_by_ids([doc_id])
