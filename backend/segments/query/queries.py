from collections import defaultdict

from database.db import (
    SegmentObjectType,
    contragents,
    contragents_tags,
    database,
    docs_sales,
    docs_sales_delivery_info,
    docs_sales_tags,
    nomenclature,
    segments,
    users_cboxes_relation,
)
from segments.logger import logger
from segments.query import filters as filter_query
from segments.query.handlers import (
    DocsSalesCriteriaHandler,
    JoinTypes,
    NomenclatureCriteriaHandler,
    SegmentHandlerJoinConfig,
)
from sqlalchemy import select

FILTER_PRIORYTY_TAGS = {
    "self": 1,
    "purchase": 2,
    "delivery_info": 3,
    "docs_sales_tags": 4,
    "contragents_tags": 5,
    "loyality": 6,
}


def chunk_list(lst, chunk_size=30000):
    """Разбивает список на части заданного размера"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


class SegmentCriteriaQuery:

    def __init__(self, cashbox_id, criteria_data: dict):
        self.criteria_data = criteria_data
        self.cashbox_id = cashbox_id
        self.filters = filter_query
        self.criteria_config = {
            "picker": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("picker", {}),
                    self.filters.add_picker_filters,
                ),
                "filter_tag": "self",
            },
            "courier": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("courier", {}),
                    self.filters.add_courier_filters,
                ),
                "filter_tag": "self",
            },
            "delivery_required": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("delivery_required", {}),
                    self.filters.add_delivery_required_filters,
                    SegmentHandlerJoinConfig(
                        JoinTypes.outerjoin,
                        lambda base: docs_sales_delivery_info.c.docs_sales_id
                        == base.c.id,
                        docs_sales_delivery_info,
                    ),
                ),
                "filter_tag": "delivery_info",
            },
            "purchases": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("purchases", {}),
                    self.filters.add_purchase_filters,
                ),
                "filter_tag": "purchase",
            },
            "loyality": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("loyality", {}),
                    self.filters.add_loyality_filters,
                ),
                "filter_tag": "loyality",
            },
            "created_at": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("created_at", {}),
                    self.filters.created_at_filters,
                ),
                "filter_tag": "self",
            },
            "tags": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("tags", {}),
                    self.filters.tags_filters,
                    SegmentHandlerJoinConfig(
                        JoinTypes.join,
                        lambda base: contragents_tags.c.contragent_id
                        == base.c.contragent,
                        contragents_tags,
                    ),
                ),
                "filter_tag": "contragents_tags",
            },
            "docs_sales_tags": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("docs_sales_tags", {}),
                    self.filters.docs_sales_tags_filters,
                    SegmentHandlerJoinConfig(
                        JoinTypes.join,
                        lambda base: docs_sales_tags.c.docs_sales_id == base.c.id,
                        docs_sales_tags,
                    ),
                ),
                "filter_tag": "docs_sales_tags",
            },
            "delivery_info": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("delivery_info", {}),
                    self.filters.delivery_info_filters,
                    SegmentHandlerJoinConfig(
                        JoinTypes.outerjoin,
                        lambda base: docs_sales_delivery_info.c.docs_sales_id
                        == base.c.id,
                        docs_sales_delivery_info,
                    ),
                ),
                "filter_tag": "delivery_info",
            },
            "orders": {
                "handler": DocsSalesCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("orders", {}),
                    self.filters.orders_filters,
                ),
                "filter_tag": "self",
            },
            "nomenclatures": {
                "handler": NomenclatureCriteriaHandler(
                    self.cashbox_id,
                    self.criteria_data.get("nomenclatures", {}),
                    self.filters.add_nomenclatures_filters,
                ),
                "filter_tag": "self",
            },
        }

    def group_criteria_by_priority(self):
        """
        Группирует критерии по приоритету filter_tag.
        Возвращает список сетов, где каждый сет — это группа критериев с одинаковым приоритетом.
        """
        grouped = defaultdict(set)

        for key, value in self.criteria_data.items():
            if value in [{}, []]:
                continue
            cfg = self.criteria_config.get(key)
            if not cfg:
                continue
            tag = cfg["filter_tag"]
            priority = FILTER_PRIORYTY_TAGS.get(tag, 999)  # дефолт — низший приоритет
            grouped[priority].add(key)

        # сортируем по приоритету и собираем как list[set]
        return [grouped[p] for p in sorted(grouped.keys())]

    async def calculate(self):
        all_filtered_data = {}
        groups = self.group_criteria_by_priority()

        for group in groups:
            for criteria in group:
                handler = self.criteria_config.get(criteria, {}).get("handler")

                if not handler:
                    continue

                async for chunk_query in handler.handle():
                    # Выполняем запрос для части
                    try:
                        chunk_rows = await database.fetch_all(chunk_query)
                        chunk_ids = [row.id for row in chunk_rows]
                        all_filtered_data.setdefault(criteria, []).extend(chunk_ids)
                    except Exception as e:
                        logger.error("Error processing segment query")
                        raise

        # Возвращаем отфильтрованные данные по критериям
        return all_filtered_data

    async def collect_ids(self):
        collected_data = await self.calculate()

        data = {
            SegmentObjectType.docs_sales.value: set(),
            SegmentObjectType.contragents.value: set(),
            SegmentObjectType.nomenclatures.value: set(),
        }

        for key, value in collected_data.items():
            # Обрабатываем ID частями
            for chunk_num, chunk_ids in enumerate(chunk_list(value, 30000)):
                # Создаем запрос для текущей части
                if key != SegmentObjectType.nomenclatures.value:
                    query = select(docs_sales.c.id, docs_sales.c.contragent).where(
                        docs_sales.c.id.in_(chunk_ids)
                    )
                else:
                    query = select(nomenclature.c.id).where(
                        nomenclature.c.id.in_(chunk_ids)
                    )

                try:
                    chunk_rows = await database.fetch_all(query)

                    # Обрабатываем результаты части
                    for row in chunk_rows:
                        if key != SegmentObjectType.nomenclatures.value:
                            data[SegmentObjectType.docs_sales.value].add(row.id)
                            if row.contragent:
                                data[SegmentObjectType.contragents.value].add(
                                    row.contragent
                                )
                        else:
                            data[SegmentObjectType.nomenclatures.value].add(row.id)

                except Exception as e:
                    logger.error(f"Error processing chunk {chunk_num + 1}: {e}")
                    raise
        return data


async def get_token_by_segment_id(segment_id: int) -> str:
    """Получение токена по ID сегмента"""
    query = (
        select(users_cboxes_relation.c.token)
        .join(segments, users_cboxes_relation.c.cashbox_id == segments.c.cashbox_id)
        .where(segments.c.id == segment_id)
    )
    row = await database.fetch_one(query)
    return row.token if row else None


async def fetch_contragent_by_id(cid):
    row = await database.fetch_one(
        select([contragents.c.name, contragents.c.phone]).where(contragents.c.id == cid)
    )
    return row
