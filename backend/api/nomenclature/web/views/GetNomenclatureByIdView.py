import hashlib
from datetime import datetime

from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from api.prices.infrastructure.readers.core.IPricesReader import IPricesReader
from database.db import database, nomenclature_hash
from functions.helpers import (
    datetime_to_timestamp,
    get_user_by_token,
)
from sqlalchemy import insert, select


class GetNomenclatureByIdView:

    def __init__(
        self, nomenclature_reader: INomenclatureReader, prices_reader: IPricesReader
    ):
        self.__nomenclature_reader = nomenclature_reader
        self.__prices_reader = prices_reader

    async def __call__(self, token: str, idx: int, with_prices: bool = False):
        user = await get_user_by_token(token)

        nomenclature_db = await self.__nomenclature_reader.get_by_id_with_prices(
            id=idx, cashbox_id=user.cashbox_id
        )
        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        if with_prices:
            prices = await self.__prices_reader.get_by_nomenclature_id(
                id=idx,
            )
            nomenclature_db["prices"] = prices

        # Добавляем qr_hash в ответ - ВСЕГДА возвращаем актуальный хеш
        hash_query = select(nomenclature_hash.c.hash).where(
            nomenclature_hash.c.nomenclature_id == idx
        )
        hash_record = await database.fetch_one(hash_query)
        if hash_record:
            # Хеш найден в БД - возвращаем его
            nomenclature_db["qr_hash"] = f"NOM:{idx}:{hash_record['hash']}"
        else:
            # Генерируем хеш, если его нет
            hash_base = f"{idx}:{nomenclature_db.get('name', '')}:{nomenclature_db.get('article', '')}"
            hash_string = "nm_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
            await database.execute(
                insert(nomenclature_hash).values(
                    nomenclature_id=idx, hash=hash_string, created_at=datetime.now()
                )
            )
            nomenclature_db["qr_hash"] = f"NOM:{idx}:{hash_string}"

        return nomenclature_db
