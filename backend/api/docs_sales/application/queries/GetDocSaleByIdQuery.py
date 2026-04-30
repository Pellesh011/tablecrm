import asyncio
from typing import Any, Dict

from api.contragents.infrastructure.repositories.ContragentRepository import (
    ContragentRepository,
)
from api.docs_sales.delivery.infrastructure.repositories.DeliveryRepository import (
    DeliveryRepository,
)
from api.docs_sales.goods.infrastructure.repositories.GoodsRepository import (
    GoodsRepository,
)
from api.docs_sales.sales.infrastructure.helpers.id_extractors import extract_user_ids
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
from api.users.infrastructure.repositories.UsersRepository import UsersRepository
from database.db import database
from fastapi import HTTPException
from functions.helpers import datetime_to_timestamp


class GetDocSaleByIdQuery:
    async def execute(self, idx: int, user_cashbox_id: int) -> dict[str, Any]:
        item_db = await self._get_item_in_db(idx, user_cashbox_id)
        results_map = await self._get_all_maps(item_db)
        item_db = self._enrich_doc_sale(item_db, results_map)
        return item_db

    @staticmethod
    async def _get_item_in_db(doc_id: int, user_cashbox_id: int) -> Dict[str, Any]:
        query = DocsSalesListRepository().get_doc_sale_by_id(doc_id, user_cashbox_id)
        item_db_raw = await database.fetch_one(query)

        if not item_db_raw:
            raise HTTPException(status_code=404, detail="Не найдено.")

        item_db = datetime_to_timestamp(item_db_raw)
        return item_db

    @staticmethod
    async def _get_all_maps(item):
        doc_id = item.get("id")
        contragent_id = item.get("contragent")
        setting_id = item.get("settings")
        users_ids: set[int] = set()

        extract_user_ids(item, users_ids)

        tasks = {
            "delivery_map": DeliveryRepository().fetch_delivery_info_by_id(doc_id),
            "goods_map": GoodsRepository().fetch_goods_by_doc_id(doc_id),
            "payment_map": PaymentsRepository().fetch_payments_map_by_id(doc_id),
            "loyality_map": LoyalityRepository().fetch_loyality_map_by_id(doc_id),
        }
        if contragent_id is not None:
            tasks["contagents_map"] = (
                ContragentRepository().fetch_segments_by_contragent_id(contragent_id)
            )

        if setting_id is not None:
            tasks["settings_map"] = SettingsRepository().fetch_settings_by_id(
                setting_id
            )

        if users_ids:
            tasks["users_map"] = UsersRepository().fetch_users_by_ids(list(users_ids))

        results = await asyncio.gather(*tasks.values())
        results_map = dict(zip(tasks.keys(), results))
        return results_map

    @staticmethod
    def _enrich_doc_sale(item, results_map: Dict[str, Any]):
        delivery_info = results_map["delivery_map"]
        item["delivery_info"] = delivery_info

        goods = results_map["goods_map"]
        item["goods"] = goods
        item["nomenclature_count"] = len(goods)
        item["doc_discount"] = round(
            sum((g.get("sum_discounted") or 0) for g in goods), 2
        )

        key_transaction = (item.get("id"), item.get("cashbox"))
        payment_value = results_map["payment_map"].get(key_transaction, {})
        item["paid_rubles"] = round(payment_value.get("total_amount", 0), 2)

        loyality_value = results_map["loyality_map"].get(key_transaction, {})
        item["paid_loyality"] = round(loyality_value.get("total_amount", 0), 2)
        item["has_loyality_card"] = bool(loyality_value.get("has_link"))

        # ВАЖНО: has_contragent должен быть вычислен ДО выбора color_status
        contragent_id = item.get("contragent")
        item["has_contragent"] = bool(contragent_id)
        item["contragent_segments"] = results_map.get("contagents_map") or []

        if item["has_loyality_card"]:
            item["color_status"] = "green"
        elif item["has_contragent"]:
            item["color_status"] = "blue"
        else:
            item["color_status"] = "default"

        paid_loyality = item.get("paid_loyality", 0)
        paid_rubles = item.get("paid_rubles", 0)
        item["paid_doc"] = round(paid_loyality + paid_rubles, 2)

        item["settings"] = results_map.get("settings_map") or {}

        for field_name in ("assigned_picker", "assigned_courier"):
            user_id = item.get(field_name)
            if isinstance(user_id, int):
                user = results_map.get("users_map").get(user_id)
                item[field_name] = {
                    "id": user["id"],
                    "first_name": user.get("first_name"),
                    "last_name": user.get("last_name"),
                }

        return item
