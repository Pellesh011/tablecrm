from api.prices.infrastructure.readers.core.IPricesReader import IPricesReader
from database.db import database, price_types, prices
from sqlalchemy import select


class PricesReader(IPricesReader):

    async def get_by_nomenclature_id(self, id: int):
        price_list = await database.fetch_all(
            select(prices.c.price, price_types.c.name.label("price_type"))
            .where(prices.c.nomenclature == id)
            .select_from(prices)
            .join(price_types, price_types.c.id == prices.c.price_type)
        )
        return price_list
