from collections import defaultdict

from database.db import database, docs_sales_goods, nomenclature, units
from sqlalchemy import Integer, cast, func, select


class GoodsRepository:
    @staticmethod
    async def fetch_goods_by_docs_ids(doc_ids: list[int]) -> dict[int, list[dict]]:
        if not doc_ids:
            return {}

        list_col = [
            c for c in docs_sales_goods.c if c.name not in ("created_at", "updated_at")
        ]

        goods_join = docs_sales_goods.join(
            nomenclature,
            docs_sales_goods.c.nomenclature == nomenclature.c.id,
            isouter=True,
        ).join(units, docs_sales_goods.c.unit == units.c.id, isouter=True)

        query = (
            select(
                *list_col,
                cast(
                    func.extract("epoch", docs_sales_goods.c.created_at), Integer
                ).label("created_at"),
                cast(
                    func.extract("epoch", docs_sales_goods.c.updated_at), Integer
                ).label("updated_at"),
                nomenclature.c.name.label("nomenclature_name"),
                units.c.convent_national_view.label("unit_name"),
            )
            .select_from(goods_join)
            .where(docs_sales_goods.c.docs_sales_id.in_(doc_ids))
        )

        goods_data = await database.fetch_all(query)
        goods_map: dict[int, list[dict]] = defaultdict(list)
        for good in goods_data:
            goods_map[good["docs_sales_id"]].append(dict(good))

        return goods_map

    async def fetch_goods_by_doc_id(self, doc_id: int) -> list[dict]:
        goods_map = await self.fetch_goods_by_docs_ids([doc_id])
        return goods_map.get(doc_id, [])
