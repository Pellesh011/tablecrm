import dataclasses
from abc import ABC, abstractmethod
from enum import Enum
from typing import AsyncGenerator, Callable

from database.db import database, docs_sales, nomenclature
from sqlalchemy import select, table


class JoinTypes(Enum):
    outerjoin = "outerjoin"
    join = "join"


@dataclasses.dataclass
class SegmentHandlerJoinConfig:
    join_type: JoinTypes
    condition: Callable
    table: table


class SegmentCriteriaHandler(ABC):
    """Отвечает за обработку параметров сегмента"""

    def __init__(
        self,
        cashbox_id: int,
        data: dict,
        filter: Callable[[select, dict, select], select],
        join_config: SegmentHandlerJoinConfig = None,
    ):
        self.cashbox_id = cashbox_id
        self.data = data
        self.filter = filter
        self.join_config = join_config

    def _add_table_join(self, query):
        if not self.join_config:
            return query

        condition = self.join_config.condition(query)
        if self.join_config.join_type == JoinTypes.outerjoin:
            return query.select_from(query).outerjoin(self.join_config.table, condition)
        elif self.join_config.join_type == JoinTypes.join:
            return query.select_from(query).join(self.join_config.table, condition)
        else:
            return query

    def _get_query(self, sub_query):
        return self._add_table_join(sub_query)

    @abstractmethod
    async def handle(self) -> AsyncGenerator[list[int], None]:
        pass


class DocsSalesCriteriaHandler(SegmentCriteriaHandler):

    def _add_table_join(self, query):
        query = select(query.c.id, query.c.contragent)
        return super()._add_table_join(query)

    async def handle(self):
        offset = 0
        batch_size = 30000

        while True:
            docs_sales_rows_chunk = await database.fetch_all(
                select(docs_sales.c.id)
                .where(
                    docs_sales.c.cashbox == self.cashbox_id,
                    docs_sales.c.is_deleted == False,
                )
                .limit(batch_size)
                .offset(offset)
            )
            offset += batch_size

            docs_sales_ids = [row.id for row in docs_sales_rows_chunk]

            if not docs_sales_ids:
                break

            subq = (
                select(docs_sales)
                .where(docs_sales.c.id.in_(docs_sales_ids))
                .subquery("sub")
            )
            query = self._get_query(subq)
            query = self.filter(query, self.data, subq)

            yield query


class NomenclatureCriteriaHandler(SegmentCriteriaHandler):

    async def handle(self):
        offset = 0
        batch_size = 30000
        while True:
            nomenclature_chunk = (
                select(nomenclature)
                .where(
                    nomenclature.c.cashbox == self.cashbox_id,
                    nomenclature.c.is_deleted is not True,
                )
                .limit(batch_size)
                .offset(offset)
            )

            offset += batch_size

            if not await database.fetch_all(nomenclature_chunk):
                break

            query = self._get_query(nomenclature_chunk)
            query = self.filter(query, self.data, None)

            yield query
