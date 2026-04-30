from database.db import database, docs_sales_delivery_info


class DeliveryRepository:
    @staticmethod
    async def fetch_delivery_info_by_ids(doc_ids: list[int]) -> dict[int, dict]:
        if not doc_ids:
            return {}

        query = docs_sales_delivery_info.select().where(
            docs_sales_delivery_info.c.docs_sales_id.in_(doc_ids)
        )
        rows = await database.fetch_all(query)

        delivery_map: dict[int, dict] = {}
        for row in rows:
            delivery_map[row["docs_sales_id"]] = {
                "address": row.get("address"),
                "delivery_date": (
                    row["delivery_date"].timestamp()
                    if row.get("delivery_date")
                    else None
                ),
                "recipient": row.get("recipient"),
                "note": row.get("note"),
            }
        return delivery_map

    async def fetch_delivery_info_by_id(self, doc_id: int) -> dict:
        delivery_map = await self.fetch_delivery_info_by_ids([doc_id])
        return delivery_map.get(doc_id, {})
