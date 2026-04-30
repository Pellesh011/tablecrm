import asyncio
import json
import logging
from datetime import datetime
from typing import List

from api.loyality_transactions.routers import raschet_bonuses
from api.pictures.routers import create_picture_from_bytes
from common.apple_wallet_service.impl.WalletNotificationService import (
    WalletNotificationService,
)
from common.apple_wallet_service.impl.WalletPassService import (
    WalletPassGeneratorService,
)
from database.db import (
    SegmentObjectType,
    contragents,
    contragents_tags,
    database,
    docs_sales,
    docs_sales_tags,
    employee_shifts,
    loyality_cards,
    loyality_transactions,
    nomenclature,
    price_types,
    prices,
    segments,
    tags,
    users,
    users_cboxes_relation,
)
from segments.actions.segment_image_research import (
    IImageResearcher,
    YandexImageResearcher,
)
from segments.actions.segment_llm_request import LLMRequestConfig, send_llm_request
from segments.actions.segment_tg_notification import send_segment_notification
from segments.constants import SegmentChangeType
from segments.helpers.collect_obj_ids import collect_objects
from segments.helpers.functions import create_replacements
from segments.helpers.http_client import HttpClient
from segments.masks import replace_masks
from sqlalchemy import and_, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)


class SegmentActions:
    def __init__(self, segment_obj):
        self.segment_obj = segment_obj
        self.ACTIONS = {
            "add_existed_tags": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.add_existed_tags,
            },
            "remove_tags": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.remove_tags,
            },
            "client_tags": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.client_tags,
            },
            "send_tg_notification": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.send_tg_notification,
            },
            "add_docs_sales_tags": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.add_docs_sales_tags,
            },
            "remove_docs_sales_tags": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.remove_docs_sales_tags,
            },
            "transform_loyality_card": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.transform_loyality_card,
            },
            "add_loyality_transaction": {
                "obj_type": SegmentObjectType.contragents.value,
                "method": self.add_loyality_transaction,
            },
            "send_wa_notification": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.send_whatsapp_notification,
            },
            "generate_nomenclature_fields": {
                "obj_type": SegmentObjectType.nomenclatures.value,
                "method": self.generate_nomenclature_fields,
            },
            "find_nomenclatures_images": {
                "obj_type": SegmentObjectType.nomenclatures.value,
                "method": self.find_nomenclatures_images,
            },
            "move_price_type": {
                "obj_type": SegmentObjectType.nomenclatures.value,
                "method": self.move_price_type,
            },
            "delete_prices": {
                "obj_type": SegmentObjectType.nomenclatures.value,
                "method": self.delete_prices,
            },
            "do_http_request": {
                "obj_type": SegmentObjectType.docs_sales.value,
                "method": self.do_http_request,
            },
            "add_nomenclature_tags": {
                "obj_type": SegmentObjectType.nomenclatures.value,
                "method": self.add_nomenclature_tags,
            },
        }

    async def refresh_segment_obj(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_obj.id)
        )

    async def run(self, action: str, ids: List[int], data: dict = None):
        """Метод для выполения action"""
        await self.ACTIONS[action]["method"](ids, data)

    async def start_actions(self):
        """Метод для запуска actions"""
        await self.refresh_segment_obj()
        if self.segment_obj.actions is None:
            return
        actions = json.loads(self.segment_obj.actions)
        if not actions or not isinstance(actions, dict):
            return
        for k, v in actions.items():
            if k not in self.ACTIONS:
                continue
            if v.get("trigger_on_new"):
                del v["trigger_on_new"]
                ids = await collect_objects(
                    self.segment_obj.id,
                    self.ACTIONS[k]["obj_type"],
                    SegmentChangeType.new.value,
                )
            elif v.get("trigger_on_removed"):
                ids = await collect_objects(
                    self.segment_obj.id,
                    self.ACTIONS[k]["obj_type"],
                    SegmentChangeType.removed.value,
                )
                del v["trigger_on_removed"]
            else:
                ids = await collect_objects(
                    self.segment_obj.id,
                    self.ACTIONS[k]["obj_type"],
                    SegmentChangeType.active.value,
                )
            if ids:
                logger.info(f"Start segment {self.segment_obj.id} task '{k}'")
                await self.run(k, ids, v)
        return

    async def add_existed_tags(self, contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(
            and_(
                tags.c.name.in_(tag_names),
                tags.c.cashbox_id == self.segment_obj.cashbox_id,
            )
        )
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        new_values = []
        for tag_id in tag_ids:
            for contragent_id in contragents_ids:
                new_values.append(
                    {
                        "contragent_id": contragent_id,
                        "tag_id": tag_id,
                        "cashbox_id": self.segment_obj.cashbox_id,
                    }
                )
        if not new_values:
            return
        query = insert(contragents_tags).values(new_values)
        query = query.on_conflict_do_nothing(
            index_elements=["tag_id", "contragent_id"]  # <- уникальная пара
        )
        await database.execute(query)

    async def remove_tags(self, contragents_ids: List[int], data: dict):
        tag_names = data.get("name", [])
        query = select(tags.c.id).where(
            tags.c.name.in_(tag_names), tags.c.cashbox_id == self.segment_obj.cashbox_id
        )
        rows = await database.fetch_all(query)
        tag_ids = [row.id for row in rows]
        query = contragents_tags.delete().where(
            contragents_tags.c.tag_id.in_(tag_ids),
            contragents_tags.c.contragent_id.in_(contragents_ids),
        )
        await database.execute(query)

    async def client_tags(self, contragents_ids: List[int], data: dict):
        names = []
        prepared_data = []
        tags_data = data.get("tags", [])
        for d in tags_data:
            names.append(d["name"])
            prepared_data.append(
                {
                    "name": d["name"],
                    "emoji": d.get("emoji", None),
                    "color": d.get("color", None),
                    "description": d.get("description", None),
                    "cashbox_id": self.segment_obj.cashbox_id,
                }
            )

        count_query = (
            select(func.count())
            .select_from(tags)
            .where(
                tags.c.name.in_(names), tags.c.cashbox_id == self.segment_obj.cashbox_id
            )
        )

        count_rows = await database.execute(count_query)

        if count_rows != len(set(names)):
            insert_query = (
                insert(tags)
                .values(prepared_data)
                .on_conflict_do_nothing(index_elements=["name", "cashbox_id"])
            )
            await database.execute(insert_query)

        await self.add_existed_tags(contragents_ids, {"name": names})

    def _check_recipient_conditions(self, data: dict):
        """
        Проверяет, соответствует ли текущее время всем заданным условиям.
        Возвращает True, если все условия выполнены или не заданы.
        """

        now = datetime.now()

        # Проверка временного диапазона
        if data.get("time_range"):
            time_range = data["time_range"]
            current_time = now.time()

            # Парсим время начала и конца
            from_time = datetime.strptime(time_range["from_"], "%H:%M").time()
            to_time = datetime.strptime(time_range["to_"], "%H:%M").time()

            # Проверяем, попадает ли текущее время в диапазон
            if from_time <= to_time:
                # Обычный случай: диапазон в пределах одних суток (например, 09:00-17:00)
                if not (from_time <= current_time <= to_time):
                    return False
            else:
                # Диапазон через полночь (например, 22:00-06:00)
                if not (current_time >= from_time or current_time <= to_time):
                    return False

        # Проверка дней недели (1=понедельник, 7=воскресенье)
        if data.get("weekdays"):
            current_weekday = now.isoweekday()  # 1=понедельник, 7=воскресенье
            if current_weekday not in data["weekdays"]:
                return False

        # Проверка дней месяца
        if data.get("month_days"):
            current_day = now.day
            if current_day not in data["month_days"]:
                return False

        # Проверка модуло дня месяца
        if data.get("month_day_modulo"):
            modulo = data["month_day_modulo"]
            current_day = now.day
            if current_day % modulo["divisor"] != modulo["remainder"]:
                return False

        return True

    async def send_tg_notification(self, order_ids: List[int], data: dict):
        chat_ids = set()
        message = data.get("message")
        send_to = data.get("send_to")
        user_tag = data.get("user_tag")
        recipients = data.get("recipients")
        shift_status = data.get("shift_status")
        if not message or (not send_to and not user_tag and not recipients):
            return
        if user_tag:
            chat_ids.update(await self.get_user_chat_ids_by_tag(user_tag, shift_status))

        if recipients:
            for recipient in recipients:
                if self._check_recipient_conditions(recipient.get("conditions", {})):
                    chat_ids.update(
                        await self.get_user_chat_ids_by_tag(
                            recipient.get("user_tag"), recipient.get("shift_status")
                        )
                    )

        for order_id in order_ids:
            message_text = f"Заказ # - {str(order_id)}\n\n" + message

            replacements = await create_replacements(order_id)

            message_text = replace_masks(message_text, replacements)
            if send_to == "picker":
                chat_ids.update(await self.get_picker_chat_id(order_id))
            elif send_to == "courier":
                chat_ids.update(await self.get_courier_chat_id(order_id))
            if not chat_ids:
                return False
            await send_segment_notification(
                recipient_ids=list(chat_ids),
                notification_text=message_text,
                segment_id=self.segment_obj.id,
            )

    async def get_user_chat_ids_by_tag(self, user_tag: str, shift_status: str = None):
        subquery = (
            select(users.c.chat_id, users_cboxes_relation.c.id.label("relation_id"))
            .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
            .where(
                and_(
                    users_cboxes_relation.c.cashbox_id == self.segment_obj.cashbox_id,
                    literal(user_tag) == func.any(users_cboxes_relation.c.tags),
                )
            )
        ).subquery("sub")
        query = select(subquery.c.chat_id)
        if shift_status:
            if shift_status == "off_shift":
                where_clause = or_(
                    employee_shifts.c.user_id.is_(None),
                    employee_shifts.c.status == shift_status,
                )
            else:
                where_clause = or_(employee_shifts.c.status == shift_status)
            query = query.outerjoin(
                employee_shifts, subquery.c.user_id == employee_shifts.c.user_id
            ).where(
                or_(
                    employee_shifts.c.user_id.is_(None),
                    employee_shifts.c.status == shift_status,
                )
            )
        rows = await database.fetch_all(query)
        return [row.chat_id for row in rows]

    async def get_picker_chat_id(self, order_id: int):
        query = (
            select(users.c.chat_id)
            .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
            .outerjoin(
                docs_sales, docs_sales.c.assigned_picker == users_cboxes_relation.c.id
            )
            .where(
                and_(
                    docs_sales.c.id == order_id,
                    docs_sales.c.cashbox == self.segment_obj.cashbox_id,
                )
            )
        )
        rows = await database.fetch_all(query)
        return [row.chat_id for row in rows]

    async def get_courier_chat_id(self, order_id: int):
        query = (
            select(users.c.chat_id)
            .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
            .outerjoin(
                docs_sales, docs_sales.c.assigned_courier == users_cboxes_relation.c.id
            )
            .where(
                and_(
                    docs_sales.c.id == order_id,
                    docs_sales.c.cashbox == self.segment_obj.cashbox_id,
                )
            )
        )
        rows = await database.fetch_all(query)
        return [row.chat_id for row in rows]

    async def add_docs_sales_tags(self, docs_ids: List[int], data: dict):

        tags = data.get("tags")
        prepared_data = []

        for doc_id in docs_ids:
            for tag in set(tags):
                prepared_data.append({"docs_sales_id": doc_id, "name": tag})

        if prepared_data:
            query = insert(docs_sales_tags).values(prepared_data)
            await database.execute(query)

    async def remove_docs_sales_tags(self, docs_ids: List[int], data: dict):
        tags = data.get("tags")
        query = docs_sales_tags.delete().where(
            and_(
                docs_sales_tags.c.docs_sales_id.in_(docs_ids),
                docs_sales_tags.c.name.in_(tags),
            )
        )

        await database.execute(query)

    async def send_whatsapp_notification(self, docs_ids: List[int], data: dict):
        message = data.get("message")
        wappi_token = data.get("wappi_token")
        wappi_profile_id = data.get("wappi_profile_id")
        if not docs_ids or not all([message, wappi_token, wappi_profile_id]):
            return False

        for idx in docs_ids:
            query = (
                select(contragents)
                .join(docs_sales, docs_sales.c.contragent == contragents.c.id)
                .where(docs_sales.c.id == idx, contragents.c.phone.isnot(None))
            )
            contragent = await database.fetch_one(query)
            if not contragent:
                continue
            replacements = await create_replacements(idx)
            message_text = replace_masks(message, replacements)

            url = (
                f"https://wappi.pro/api/sync/message/send?profile_id={wappi_profile_id}"
            )
            headers = {"Authorization": f"{wappi_token}"}
            d = {"body": message_text, "recipient": contragent.phone}

            async with HttpClient() as client:
                status, response = await client.post(url, headers=headers, data=d)
                print("Status:", status)
            await asyncio.sleep(data["sleep"])

    async def do_http_request(self, docs_ids: List[int], data: dict):
        for idx in docs_ids:
            replacements = await create_replacements(idx)
            data = replace_masks(data, replacements)
            url = data["url"]
            if data.get("params"):
                url = (
                    data["url"]
                    + "?"
                    + "&".join([f"{k}={v}" for k, v in data["params"].items()])
                )
            async with HttpClient() as client:
                try:

                    method = data["method"].lower()

                    if method == "get":
                        status, response = await client.get(
                            url=url,
                            headers=data.get("headers"),
                        )
                    else:
                        status, response = await getattr(client, method)(
                            url=url,
                            headers=data.get("headers"),
                            data=data.get("body"),
                        )

                    logger.info(f"Status for contragent {idx}: {status}")
                    if status not in [200, 201]:
                        logger.info(f"Response for contragent {idx}: {response}")
                except Exception as e:
                    logger.error(e)
            await asyncio.sleep(data["sleep"])

    async def transform_loyality_card(self, contragents_ids: List[int], data: dict):
        fields_for_update = {}
        if data.get("cashback_percent"):
            fields_for_update["cashback_percent"] = data.get("cashback_percent")
        if data.get("max_withdraw_percentage"):
            fields_for_update["max_withdraw_percentage"] = data.get(
                "max_withdraw_percentage"
            )
        if data.get("lifetime"):
            fields_for_update["lifetime"] = data.get("lifetime")
        if data.get("tag"):
            fields_for_update["tags"] = data.get("tag")
        if data.get("apple_wallet_advertisement"):
            fields_for_update["apple_wallet_advertisement"] = data.get(
                "apple_wallet_advertisement"
            )

        query = (
            update(loyality_cards)
            .where(
                loyality_cards.c.contragent_id.in_(contragents_ids),
                loyality_cards.c.cashbox_id == self.segment_obj.cashbox_id,
            )
            .values(**fields_for_update)
        )

        await database.execute(query)

        apple_notification_service = WalletNotificationService()
        apple_wallet_service = WalletPassGeneratorService()

        loyality_ids_query = select(loyality_cards.c.id).where(
            loyality_cards.c.contragent_id.in_(contragents_ids),
            loyality_cards.c.cashbox_id == self.segment_obj.cashbox_id,
        )
        loyality_ids = [i.id for i in (await database.fetch_all(loyality_ids_query))]
        for card_id in loyality_ids:
            await apple_wallet_service.update_pass(card_id)
            await apple_notification_service.ask_update_pass(card_id)

    async def add_loyality_transaction(self, contragents_ids: List[int], data: dict):
        insert_data = {}
        insert_data["amount"] = data.get("amount")
        insert_data["type"] = (
            "accrual" if data.get("direction") == "plus" else "withdraw"
        )
        if data.get("comment"):
            insert_data["name"] = data.get("comment")
        else:
            insert_data["name"] = "Обновление условий карты лояльности"
        insert_data["is_deleted"] = False
        insert_data["status"] = True
        insert_data["cashbox"] = self.segment_obj.cashbox_id

        query = select(loyality_cards).where(
            loyality_cards.c.contragent_id.in_(contragents_ids),
            loyality_cards.c.cashbox_id == self.segment_obj.cashbox_id,
        )
        cards = await database.fetch_all(query)
        for card in cards:
            insert_data["loyality_card_id"] = card.id
            insert_data["loyality_card_number"] = card.card_number
            await database.execute(loyality_transactions.insert().values(insert_data))
            await asyncio.gather(asyncio.create_task(raschet_bonuses(card.id)))

    async def _gather_in_batches(self, tasks, batch_size=10):
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            yield await asyncio.gather(*batch)

    async def generate_nomenclature_fields(
        self, nomenclatures_ids: List[int], data: dict
    ):
        api_key = data.get("openai_api_key")
        base_url = data.get("openai_url_base")
        model = data.get("llm_model")
        fields = data.get("fields")

        nomenclature_query = (
            select(nomenclature)
            .where(nomenclature.c.id.in_(nomenclatures_ids))
            .where(
                or_(
                    nomenclature.c.segment_tags.is_(None),
                    ~nomenclature.c.segment_tags.any(self.segment_obj.hash_tag),
                )
            )
        )

        nomenclatures = await database.fetch_all(nomenclature_query)
        tasks = []
        for nomenclature_obj in nomenclatures:
            fields_to_generate = []
            for field in fields:
                # Пропускаем те поля, которые уже заполнены
                attr = nomenclature_obj.get(field, None)
                if attr is None:
                    fields_to_generate.append(field)

            if not fields_to_generate:
                continue

            request_config = LLMRequestConfig(nomenclature_obj.name, fields_to_generate)
            tasks.append(
                send_llm_request(
                    model,
                    api_key,
                    base_url,
                    self.segment_obj.cashbox_id,
                    request_config,
                )
            )

        start = 0
        async for responses in self._gather_in_batches(tasks):
            for index, response in enumerate(responses):
                nomenclature_obj = nomenclatures[start + index]

                nomenclature_dict = dict(nomenclature_obj)
                response_json = response
                # Обновляем поля номенклатуры
                for key, value in response_json.items():
                    nomenclature_dict[key] = value

                # Отмечаем, что уже обошли эту номенклатуру этим сегментом
                if not nomenclature_dict.get("segment_tags"):
                    nomenclature_dict["segment_tags"] = []
                nomenclature_dict["segment_tags"].append(self.segment_obj.hash_tag)

                nomenclature_update_query = (
                    update(nomenclature)
                    .where(nomenclature.c.id == nomenclature_obj.id)
                    .values(**nomenclature_dict)
                )
                await database.execute(nomenclature_update_query)
            start += len(responses)

    async def move_price_type(self, nomenclatures_ids: List[int], data: dict):
        types = data.get("from_types")
        to_type = data.get("to_type")
        prices_query = (
            select(prices)
            .where(prices.c.nomenclature.in_(nomenclatures_ids))
            .join(price_types, price_types.c.id == prices.c.price_type)
            .where(price_types.c.name.in_(types))
        )
        to_type_query = select(price_types).where(price_types.c.name == to_type)

        to_type_obj = await database.fetch_one(to_type_query)
        price_objs = await database.fetch_all(prices_query)
        for price in price_objs:
            update_price_query = (
                update(prices)
                .where(prices.c.id == price.id)
                .values(price_type=to_type_obj.id)
            )
            await database.execute(update_price_query)

    async def delete_prices(self, nomenclatures_ids: List[int], data: dict):
        types = data.get("types")

        update_prices_query = (
            select(prices.c.id)
            .where(prices.c.nomenclature.in_(nomenclatures_ids))
            .join(price_types, price_types.c.id == prices.c.price_type)
            .where(price_types.c.name.in_(types))
        ).subquery()

        stmt = (
            update(prices)
            .where(prices.c.id.in_(update_prices_query))
            .values(is_deleted=True)
        )

        await database.execute(stmt)

    async def add_nomenclature_tags(self, nomenclatures_ids: List[int], data: dict):
        new_tags = set(data.get("tags"))

        if not new_tags:
            return

        nomenclature_query = nomenclature.select().where(
            nomenclature.c.id.in_(nomenclatures_ids)
        )
        nomenclatures = await database.fetch_all(nomenclature_query)

        for nomenclature_obj in nomenclatures:
            exists_tags = set()
            if nomenclature_obj.tags:
                exists_tags = set(nomenclature_obj.tags)

            nomenclature_new_tags = list(exists_tags.union(new_tags))
            await database.execute(
                nomenclature.update()
                .where(nomenclature.c.id == nomenclature_obj.id)
                .values(tags=nomenclature_new_tags)
            )

    async def find_nomenclatures_images(self, nomenclatures_ids: List[int], data: dict):
        image_searcher: IImageResearcher = YandexImageResearcher(
            data.get("yandex_api_token")
        )
        nomenclature_query = nomenclature.select().where(
            nomenclature.c.id.in_(nomenclatures_ids)
        )

        nomenclatures = await database.fetch_all(nomenclature_query)

        for nomenclature_obj in nomenclatures:
            image_bytes = await image_searcher.search(nomenclature_obj.name)
            if image_bytes:
                await create_picture_from_bytes(
                    image_bytes,
                    "nomenclature",
                    nomenclature_obj.id,
                    nomenclature_obj.owner,
                    nomenclature_obj.cashbox,
                )
