import asyncio
from typing import Any, Dict, List, Optional, Tuple

from api.contragents.infrastructure.repositories.ContragentRepository import (
    ContragentRepository,
)
from api.docs_sales import schemas
from api.docs_sales.delivery.infrastructure.repositories.DeliveryRepository import (
    DeliveryRepository,
)
from api.docs_sales.goods.infrastructure.repositories.GoodsRepository import (
    GoodsRepository,
)
from api.docs_sales.sales.infrastructure.helpers.FilterService import FilterService
from api.docs_sales.sales.infrastructure.helpers.id_extractors import (
    extract_contragent_id,
    extract_doc_id,
    extract_settings_id,
)
from api.docs_sales.sales.infrastructure.repositories.DocsSalesListRepository import (
    DocsSalesListRepository,
)
from api.loyality_transactions.infrastructure.repositories.LoyalityRepository import (
    LoyalityRepository,
)
from api.payments.infrastructure.repositories.PaymentsRepository import (
    PaymentsRepository,
)
from api.settings.infrastructure.repositories.SettingsRepository import (
    SettingsRepository,
)
from database.db import database
from functions.helpers import datetime_to_timestamp
from sqlalchemy import and_


class GetDocsSalesListQuery:
    async def execute(
        self,
        cashbox_id: int,
        limit: int = 100,
        offset: int = 0,
        show_goods: bool = False,
        filters: Optional[schemas.FilterSchema] = None,
        kanban: bool = False,
    ) -> dict[str, Any]:
        items_db, count = await self._get_items_and_count_in_db(
            cashbox_id, limit, offset, filters
        )
        results_map = await self._get_all_maps(items_db)
        items_db = self._enrich_docs_sales_items(items_db, results_map)
        return {"result": items_db, "count": count}

    @staticmethod
    async def _get_items_and_count_in_db(
        cashbox_id, limit, offset, filters
    ) -> Tuple[List[Dict[str, Any]], int]:
        query = DocsSalesListRepository.base_list_query(cashbox_id)
        count_query = DocsSalesListRepository.base_count_list_query(cashbox_id)

        filter_list, query_with_joins = FilterService().apply_filters(filters, query)

        if filter_list:
            query = query_with_joins.where(and_(*filter_list))
            count_query = count_query.where(and_(*filter_list))
        else:
            query = query_with_joins

        query = query.limit(limit).offset(offset)

        items_task = database.fetch_all(query)
        count_task = database.fetch_val(count_query)

        items_db_raw, count = await asyncio.gather(items_task, count_task)
        return items_db_raw, count

    @staticmethod
    def _aggregate_ids(items_db) -> Dict[str, Any]:
        doc_ids: list[int] = []
        contragent_ids: set[int] = set()
        settings_ids: list[int] = []

        for idx, item in enumerate(items_db):
            normalized = datetime_to_timestamp(dict(item))
            items_db[idx] = normalized

            extract_doc_id(normalized, doc_ids)
            extract_contragent_id(normalized, contragent_ids)
            extract_settings_id(normalized, settings_ids)

        return {
            "doc_ids": doc_ids,
            "contragents_ids": list(contragent_ids),
            "settings_ids": settings_ids,
        }

    async def _get_all_maps(self, items_db):
        aggregate_ids = self._aggregate_ids(items_db)

        doc_ids = aggregate_ids.get("doc_ids")
        contragent_ids = aggregate_ids.get("contragents_ids")
        settings_ids = aggregate_ids.get("settings_ids")

        tasks = {
            "delivery_map": DeliveryRepository().fetch_delivery_info_by_ids(doc_ids),
            "contagents_map": ContragentRepository().fetch_segments_by_contragent_ids(
                contragent_ids
            ),
            "goods_map": GoodsRepository().fetch_goods_by_docs_ids(doc_ids),
            "settings_map": SettingsRepository().fetch_settings_by_ids(settings_ids),
            "payment_map": PaymentsRepository().fetch_payments_map_by_ids(doc_ids),
            "loyality_map": LoyalityRepository().fetch_loyality_map_by_ids(doc_ids),
        }

        results = await asyncio.gather(*tasks.values())
        results_map = dict(zip(tasks.keys(), results))
        return results_map

    @staticmethod
    def _enrich_docs_sales_items(items_db, results_map: Dict[str, Any]):
        for item in items_db:
            doc_id = item.get("id")

            goods = results_map["goods_map"].get(doc_id, [])
            item["goods"] = goods
            item["nomenclature_count"] = len(goods)
            item["doc_discount"] = round(
                sum((g.get("sum_discounted") or 0) for g in goods), 2
            )

            contragent_id = item.get("contragent")
            item["has_contragent"] = bool(contragent_id)
            item["contragent_segments"] = results_map["contagents_map"].get(
                contragent_id, []
            )

            delivery_info = results_map["delivery_map"].get(doc_id, {})
            item.update(delivery_info)

            key_transaction = (item.get("id"), item.get("cashbox"))
            loyality_value = results_map["loyality_map"].get(key_transaction, {})

            item["paid_loyality"] = round(loyality_value.get("total_amount", 0), 2)
            item["has_loyality_card"] = bool(loyality_value.get("has_link"))

            if item["has_loyality_card"]:
                item["color_status"] = "green"
            elif item.get("has_contragent"):
                item["color_status"] = "blue"
            else:
                item["color_status"] = "default"

            settings_id = item.get("settings")
            item["settings"] = results_map["settings_map"].get(settings_id)

            payment_value = results_map["payment_map"].get(key_transaction) or {}
            item["paid_rubles"] = round(payment_value.get("total_amount", 0), 2)

            paid_loyality = item.get("paid_loyality", 0)
            paid_rubles = item.get("paid_rubles", 0)
            item["paid_doc"] = round(paid_loyality + paid_rubles, 2)

        return items_db
