from database.db import database
from sqlalchemy.dialects.postgresql import insert


async def insert_in_batches(table, data, batch_size=10000):
    """
    Вставка данных пачками, чтобы избежать ограничения по параметрам.
    """
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        query = insert(table).values(batch)
        await database.execute(query)
