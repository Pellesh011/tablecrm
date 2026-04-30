import asyncio
import calendar
import datetime
import hashlib
import os
from collections import defaultdict
from typing import Any, Dict, Optional, Union

from api.docs_sales import schemas
from api.docs_sales.application.queries.GetDocSaleByIdQuery import GetDocSaleByIdQuery
from api.docs_sales.application.queries.GetDocsSalesListByCreatedDateQuery import (
    GetDocsSalesListByCreatedDateQuery,
)
from api.docs_sales.application.queries.GetDocsSalesListByDeliveryDateQuery import (
    GetDocsSalesListByDeliveryDateQuery,
)
from api.docs_sales.application.queries.GetDocsSalesListQuery import (
    GetDocsSalesListQuery,
)
from api.docs_sales.notify_service import (
    format_notification_text,
    send_order_notification,
)
from api.docs_warehouses.schemas import EditMass as WarehouseUpdate
from api.docs_warehouses.utils import create_warehouse_docs
from api.employee_shifts.service import (
    check_user_on_shift,
    get_available_couriers_on_shift,
    get_available_pickers_on_shift,
)
from api.loyality_transactions.routers import raschet_bonuses
from apps.yookassa.functions.impl.GetOauthCredentialFunction import (
    GetOauthCredentialFunction,
)
from apps.yookassa.models.PaymentModel import (
    AmountModel,
    ConfirmationRedirect,
    CustomerModel,
    ItemModel,
    PaymentCreateModel,
    ReceiptModel,
)
from apps.yookassa.repositories.impl.YookassaCrmPaymentsRepository import (
    YookassaCrmPaymentsRepository,
)
from apps.yookassa.repositories.impl.YookassaOauthRepository import (
    YookassaOauthRepository,
)
from apps.yookassa.repositories.impl.YookassaPaymentsRepository import (
    YookassaPaymentsRepository,
)
from apps.yookassa.repositories.impl.YookassaRequestRepository import (
    YookassaRequestRepository,
)
from apps.yookassa.repositories.impl.YookassaTableNomenclature import (
    YookassaTableNomenclature,
)
from apps.yookassa.repositories.impl.YookasssaAmoTableCrmRepository import (
    YookasssaAmoTableCrmRepository,
)
from apps.yookassa.services.impl.OauthService import OauthService
from apps.yookassa.services.impl.YookassaApiService import YookassaApiService
from apps.yookassa.utils.vat import vat_code_from_tax_percent
from database.db import (
    NomenclatureCashbackType,
    OrderStatus,
    articles,
    contracts,
    contragents,
    database,
    docs_sales,
    docs_sales_delivery_info,
    docs_sales_goods,
    docs_sales_links,
    docs_sales_settings,
    docs_sales_tags,
    docs_warehouse,
    entity_to_entity,
    fifo_settings,
    loyality_cards,
    loyality_transactions,
    nomenclature,
    organizations,
    payments,
    pboxes,
    price_types,
    units,
    users,
    users_cboxes_relation,
    warehouse_balances,
    warehouses,
)
from fastapi import APIRouter, Depends, HTTPException, Query
from functions.helpers import (
    add_delivery_info_to_doc,
    add_docs_sales_settings,
    add_nomenclature_name_to_goods,
    check_contragent_exists,
    check_entity_exists,
    check_period_blocked,
    check_unit_exists,
    datetime_to_timestamp,
    get_user_by_token,
    raschet_oplat,
)
from functions.users import _raschet_debounced
from producer import queue_notification
from sqlalchemy import and_, desc, func, or_, select
from ws_manager import manager

router = APIRouter(tags=["docs_sales"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()

# Секретный ключ для генерации MD5-хешей (в реальном приложении лучше хранить в переменных окружения)
SECRET_KEY = os.environ.get(
    "MD5_SECRET_KEY", "default_secret_key_for_notification_hashes"
)


def generate_notification_hash(order_id: int, role: str) -> str:
    """Генерация MD5-хеша для уведомлений на основе ID заказа и роли"""
    data = f"{order_id}:{role}:{SECRET_KEY}"
    return hashlib.md5(data.encode()).hexdigest()


async def generate_and_save_order_links(order_id: int) -> dict:
    """
    Генерирует и сохраняет ссылки для заказа для разных ролей

    Args:
        order_id: ID заказа

    Returns:
        dict: Словарь с сгенерированными ссылками
    """
    # Проверка существования заказа
    query = docs_sales.select().where(docs_sales.c.id == order_id)
    order = await database.fetch_one(query)

    if not order:
        return None

    # Получаем базовый URL
    base_url = os.environ.get("APP_URL")
    if not base_url:
        raise ValueError("APP_URL не задан в переменных окружения")

    # Генерация хешей и URL для каждой роли
    roles = ["general", "picker", "courier"]
    links = {}

    for role in roles:
        # Проверяем, существует ли уже ссылка для этой роли и заказа
        query = docs_sales_links.select().where(
            docs_sales_links.c.docs_sales_id == order_id,
            docs_sales_links.c.role == role,
        )
        existing_link = await database.fetch_one(query)

        if existing_link:
            # Если ссылка уже существует, используем её
            link_dict = dict(existing_link)
            # Преобразуем role из enum в строку, получая только значение
            link_dict["role"] = (
                link_dict["role"].value
                if hasattr(link_dict["role"], "value")
                else link_dict["role"].name
            )
            links[f"{role}_link"] = link_dict
        else:
            # Генерация нового хеша
            hash_value = generate_notification_hash(order_id, role)

            # Формирование URL
            if role == "general":
                url = f"{base_url}/orders/{order_id}?hash={hash_value}"
            else:
                url = f"{base_url}/orders/{order_id}/{role}?hash={hash_value}"

            # Сохраняем в базу данных
            query = docs_sales_links.insert().values(
                docs_sales_id=order_id, role=role, hash=hash_value, url=url
            )
            link_id = await database.execute(query)

            # Получаем созданную запись
            query = docs_sales_links.select().where(docs_sales_links.c.id == link_id)
            created_link = await database.fetch_one(query)

            link_dict = dict(created_link)
            # Преобразуем role из enum в строку, получая только значение
            link_dict["role"] = (
                link_dict["role"].value
                if hasattr(link_dict["role"], "value")
                else link_dict["role"].name
            )
            links[f"{role}_link"] = link_dict

    return links


async def exists_settings_docs_sales(docs_sales_id: int) -> bool:
    query = docs_sales.select().where(
        docs_sales.c.id == docs_sales_id, docs_sales.c.settings.is_not(None)
    )
    exists = await database.fetch_one(query)
    return bool(exists)


async def add_settings_docs_sales(settings: Optional[dict]) -> Optional[int]:
    if settings:
        query = docs_sales_settings.insert().values(settings)
        docs_sales_settings_id = await database.execute(query)
        return docs_sales_settings_id


async def update_settings_docs_sales(
    docs_sales_id: int, settings: Optional[dict]
) -> None:
    if settings:
        docs_sales_ids = (
            select(docs_sales.c.settings)
            .where(docs_sales.c.id == docs_sales_id)
            .subquery("docs_sales_ids")
        )
        query = (
            docs_sales_settings.update()
            .where(docs_sales_settings.c.id.in_(docs_sales_ids))
            .values(settings)
        )
        await database.execute(query)


@router.get("/docs_sales/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    user = await get_user_by_token(token)
    query = GetDocSaleByIdQuery()
    return await query.execute(idx=idx, user_cashbox_id=user.cashbox_id)


@router.get("/docs_sales/", response_model=schemas.CountRes)
async def get_list(
    token: str,
    limit: int = 100,
    offset: int = 0,
    show_goods: bool = True,
    filters: schemas.FilterSchema = Depends(),
    kanban: bool = False,
    sort: Optional[str] = "created_at:desc",
):
    user = await get_user_by_token(token)
    query = GetDocsSalesListQuery()
    return await query.execute(
        cashbox_id=user.cashbox_id, limit=limit, offset=offset, filters=filters
    )


@router.get("/docs_sales/created/{date}", response_model=schemas.CountRes)
async def get_list_by_created_date(
    token: str,
    date: str,
    show_goods: bool = False,
    filters: schemas.FilterSchema = Depends(),
    kanban: bool = False,
):
    """Получение списка документов"""
    user = await get_user_by_token(token)
    query = GetDocsSalesListByCreatedDateQuery()
    return await query.execute(cashbox_id=user.cashbox_id, date=date, filters=filters)


@router.get("/docs_sales/delivery/{date}", response_model=schemas.CountRes)
async def get_list_by_delivery_date(
    token: str,
    date: str,
    show_goods: bool = False,
    filters: schemas.FilterSchema = Depends(),
    kanban: bool = False,
):
    """Получение списка документов"""
    user = await get_user_by_token(token)
    query = GetDocsSalesListByDeliveryDateQuery()
    return await query.execute(cashbox_id=user.cashbox_id, date=date, filters=filters)


async def check_foreign_keys(instance_values, user, exceptions) -> bool:
    if instance_values.get("client") is not None:
        if instance_values["client"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["client"], user.cashbox_id
                )
                contragents_cache.add(instance_values["client"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contragent") is not None:
        if instance_values["contragent"] not in contragents_cache:
            try:
                await check_contragent_exists(
                    instance_values["contragent"], user.cashbox_id
                )
                contragents_cache.add(instance_values["contragent"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("contract") is not None:
        if instance_values["contract"] not in contracts_cache:
            try:
                await check_entity_exists(
                    contracts, instance_values["contract"], user.id
                )
                contracts_cache.add(instance_values["contract"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("organization") is not None:
        if instance_values["organization"] not in organizations_cache:
            try:
                await check_entity_exists(
                    organizations, instance_values["organization"], user.id
                )
                organizations_cache.add(instance_values["organization"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("warehouse") is not None:
        if instance_values["warehouse"] not in warehouses_cache:
            try:
                await check_entity_exists(
                    warehouses, instance_values["warehouse"], user.id
                )
                warehouses_cache.add(instance_values["warehouse"])
            except HTTPException as e:
                exceptions.append(str(instance_values) + " " + e.detail)
                return False

    if instance_values.get("sales_manager") is not None:
        if instance_values["sales_manager"] not in users_cache:
            query = users_cboxes_relation.select().where(
                users_cboxes_relation.c.id == instance_values["sales_manager"]
            )
            if not await database.fetch_one(query):
                exceptions.append(str(instance_values) + " Пользователь не существует!")
                return False
            users_cache.add(instance_values["sales_manager"])
    return True


async def create(
    token: str, docs_sales_data: schemas.CreateMass, generate_out: bool = True
):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []

    count_query = select(func.count(docs_sales.c.id)).where(
        docs_sales.c.cashbox == user.cashbox_id, docs_sales.c.is_deleted.is_(False)
    )
    count_docs_sales = await database.fetch_val(count_query, column=0)

    paybox_q = pboxes.select().where(
        pboxes.c.cashbox == user.cashbox_id,
        pboxes.c.deleted_at.is_(None),
    )
    paybox = await database.fetch_one(paybox_q)
    paybox_id = None if not paybox else paybox.id

    article_q = articles.select().where(
        articles.c.cashbox == user.cashbox_id, articles.c.name == "Продажи"
    )
    article_db = await database.fetch_one(article_q)

    for index, instance_values in enumerate(docs_sales_data.dict()["__root__"]):
        instance_values["created_by"] = user.id
        instance_values["sales_manager"] = user.id
        instance_values["is_deleted"] = False
        instance_values["cashbox"] = user.cashbox_id
        instance_values["settings"] = await add_settings_docs_sales(
            instance_values.pop("settings", None)
        )
        priority = instance_values.get("priority")
        if priority is not None and (priority < 0 or priority > 10):
            raise HTTPException(400, "Приоритет должен быть от 0 до 10")

        goods: Union[list, None] = instance_values.pop("goods", None)

        goods_tmp = goods

        paid_rubles = instance_values.pop("paid_rubles", 0)
        paid_rubles = 0 if not paid_rubles else paid_rubles

        paid_lt = instance_values.pop("paid_lt", 0)
        paid_lt = 0 if not paid_lt else paid_lt

        lt = instance_values.pop("loyality_card_id")

        if not await check_period_blocked(
            instance_values["organization"], instance_values.get("dated"), exceptions
        ):
            continue

        if not await check_foreign_keys(
            instance_values,
            user,
            exceptions,
        ):
            continue

        del instance_values["client"]

        if not instance_values.get("number"):
            query = (
                select(docs_sales.c.number)
                .where(
                    docs_sales.c.is_deleted == False,
                    docs_sales.c.organization == instance_values["organization"],
                )
                .order_by(desc(docs_sales.c.created_at))
            )
            prev_number_docs_sales = await database.fetch_one(query)
            if prev_number_docs_sales:
                if prev_number_docs_sales.number:
                    try:
                        number_int = int(prev_number_docs_sales.number)
                    except:
                        number_int = 0
                    instance_values["number"] = str(number_int + 1)
                else:
                    instance_values["number"] = "1"
            else:
                instance_values["number"] = "1"

        paybox = instance_values.pop("paybox", None)
        if paybox is None:
            if paybox_id is not None:
                paybox = paybox_id

        query = docs_sales.insert().values(instance_values)
        instance_id = await database.execute(query)

        # Генерация ссылок для заказа
        try:
            await generate_and_save_order_links(instance_id)
        except Exception as e:
            print(f"Ошибка при генерации ссылок для заказа {instance_id}: {e}")

        # Процесс разделения тегов(в другую таблицу)
        tags = instance_values.pop("tags", "")
        if tags:
            tags_insert_list = []
            tags_split = tags.split(",")
            for tag_name in tags_split:
                tags_insert_list.append(
                    {
                        "docs_sales_id": instance_id,
                        "name": tag_name,
                    }
                )
            if tags_insert_list:
                await database.execute(docs_sales_tags.insert(tags_insert_list))

        inserted_ids.add(instance_id)
        items_sum = 0

        cashback_sum = 0

        lcard = None
        if lt:
            lcard_q = loyality_cards.select().where(loyality_cards.c.id == lt)
            lcard = await database.fetch_one(lcard_q)

        for item in goods:
            item["docs_sales_id"] = instance_id
            del item["nomenclature_name"]
            del item["unit_name"]

            if item.get("price_type") is not None:
                if item["price_type"] not in price_types_cache:
                    try:
                        await check_entity_exists(
                            price_types, item["price_type"], user.id
                        )
                        price_types_cache.add(item["price_type"])
                    except HTTPException as e:
                        exceptions.append(str(item) + " " + e.detail)
                        continue
            if item.get("unit") is not None:
                if item["unit"] not in units_cache:
                    try:
                        await check_unit_exists(item["unit"])
                        units_cache.add(item["unit"])
                    except HTTPException as e:
                        exceptions.append(str(item) + " " + e.detail)
                        continue
            item["nomenclature"] = int(item["nomenclature"])
            query = docs_sales_goods.insert().values(item)
            await database.execute(query)

            items_sum += item["price"] * item["quantity"]

            if lcard:
                nomenclature_db = await database.fetch_one(
                    nomenclature.select().where(
                        nomenclature.c.id == item["nomenclature"]
                    )
                )
                calculated_share = paid_rubles / (paid_rubles + paid_lt)
                if nomenclature_db:
                    if (
                        nomenclature_db.cashback_type
                        == NomenclatureCashbackType.no_cashback
                    ):
                        pass
                    elif (
                        nomenclature_db.cashback_type
                        == NomenclatureCashbackType.percent
                    ):
                        current_percent = (
                            item["price"]
                            * item["quantity"]
                            * (nomenclature_db.cashback_value / 100)
                        )
                        cashback_sum += calculated_share * current_percent
                    elif (
                        nomenclature_db.cashback_type == NomenclatureCashbackType.const
                    ):
                        cashback_sum += (
                            item["quantity"] * nomenclature_db.cashback_value
                        )
                    elif (
                        nomenclature_db.cashback_type
                        == NomenclatureCashbackType.lcard_cashback
                    ):
                        current_percent = (
                            item["price"]
                            * item["quantity"]
                            * (lcard.cashback_percent / 100)
                        )
                        print(current_percent)
                        print(lcard.cashback_percent)
                        print(calculated_share)
                        print(calculated_share * current_percent)
                        cashback_sum += calculated_share * current_percent
                    else:
                        current_percent = (
                            item["price"]
                            * item["quantity"]
                            * (lcard.cashback_percent / 100)
                        )
                        cashback_sum += calculated_share * current_percent
                else:
                    current_percent = (
                        item["price"]
                        * item["quantity"]
                        * (lcard.cashback_percent / 100)
                    )
                    cashback_sum += calculated_share * current_percent

            if instance_values.get("warehouse") is not None:
                query = (
                    warehouse_balances.select()
                    .where(
                        warehouse_balances.c.warehouse_id
                        == instance_values["warehouse"],
                        warehouse_balances.c.nomenclature_id == item["nomenclature"],
                    )
                    .order_by(desc(warehouse_balances.c.created_at))
                )
                last_warehouse_balance = await database.fetch_one(query)
                warehouse_amount = (
                    last_warehouse_balance.current_amount
                    if last_warehouse_balance
                    else 0
                )

                query = warehouse_balances.insert().values(
                    {
                        "organization_id": instance_values["organization"],
                        "warehouse_id": instance_values["warehouse"],
                        "nomenclature_id": item["nomenclature"],
                        "document_sale_id": instance_id,
                        "outgoing_amount": item["quantity"],
                        "current_amount": warehouse_amount - item["quantity"],
                        "cashbox_id": user.cashbox_id,
                    }
                )
                await database.execute(query)

        if paid_rubles > 0:
            if article_db:
                article_id = article_db.id
            else:
                tstamp = int(datetime.datetime.now().timestamp())
                created_article_q = articles.insert().values(
                    {
                        "name": "Продажи",
                        "emoji": "🛍️",
                        "cashbox": user.cashbox_id,
                        "created_at": tstamp,
                        "updated_at": tstamp,
                    }
                )
                article_id = await database.execute(created_article_q)

            payment_id = await database.execute(
                payments.insert().values(
                    {
                        "contragent": instance_values["contragent"],
                        "type": "incoming",
                        "name": f"Оплата по документу {instance_values['number']}",
                        "amount_without_tax": round(paid_rubles, 2),
                        "tags": tags,
                        "amount": round(paid_rubles, 2),
                        "tax": 0,
                        "tax_type": "internal",
                        "article_id": article_id,
                        "article": "Продажи",
                        "paybox": paybox,
                        "date": int(datetime.datetime.now().timestamp()),
                        "account": user.user,
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": int(datetime.datetime.now().timestamp()),
                        "updated_at": int(datetime.datetime.now().timestamp()),
                        "status": instance_values["status"],
                        "stopped": True,
                        "docs_sales_id": instance_id,
                    }
                )
            )
            await database.execute(
                pboxes.update()
                .where(pboxes.c.id == paybox)
                .values({"balance": pboxes.c.balance - paid_rubles})
            )

            # Юкасса

            yookassa_oauth_service = OauthService(
                oauth_repository=YookassaOauthRepository(),
                request_repository=YookassaRequestRepository(),
                get_oauth_credential_function=GetOauthCredentialFunction(),
            )

            yookassa_api_service = YookassaApiService(
                request_repository=YookassaRequestRepository(),
                oauth_repository=YookassaOauthRepository(),
                payments_repository=YookassaPaymentsRepository(),
                crm_payments_repository=YookassaCrmPaymentsRepository(),
                table_nomenclature_repository=YookassaTableNomenclature(),
                amo_table_crm_repository=YookasssaAmoTableCrmRepository(),
            )

            if await yookassa_oauth_service.validation_oauth(
                user.cashbox_id, instance_values["warehouse"]
            ):
                org_row = await database.fetch_one(
                    select(organizations.c.tax_percent).where(
                        organizations.c.id == instance_values["organization"],
                        organizations.c.cashbox == user.cashbox_id,
                        organizations.c.is_deleted.is_(False),
                    )
                )
                vat_code = vat_code_from_tax_percent(
                    org_row.tax_percent if org_row else None
                )
                await yookassa_api_service.api_create_payment(
                    user.cashbox_id,
                    instance_values["warehouse"],
                    instance_id,
                    payment_id,
                    PaymentCreateModel(
                        amount=AmountModel(
                            value=str(round(paid_rubles, 2)), currency="RUB"
                        ),
                        description=f"Оплата по документу {instance_values['number']}",
                        capture=True,
                        receipt=ReceiptModel(
                            type="payment",
                            tax_system="usn_income",
                            customer=CustomerModel(),
                            items=[
                                ItemModel(
                                    description=good.get("nomenclature_name") or "",
                                    amount=AmountModel(
                                        value=good.get("price"), currency="RUB"
                                    ),
                                    quantity=good.get("quantity"),
                                    vat_code=vat_code,
                                )
                                for good in goods_tmp
                            ],
                        ),
                        confirmation=ConfirmationRedirect(
                            type="redirect",
                            return_url=f"https://${os.getenv('APP_URL')}/?token=${token}",
                        ),
                    ),
                )

            # юкасса

            await database.execute(
                entity_to_entity.insert().values(
                    {
                        "from_entity": 7,
                        "to_entity": 5,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_sales_payments",
                        "from_id": instance_id,
                        "to_id": payment_id,
                        "status": True,
                        "delinked": False,
                    }
                )
            )
            if lcard:
                if cashback_sum > 0:
                    calculated_cashback_sum = round((cashback_sum), 2)
                    if calculated_cashback_sum > 0:
                        rubles_body = {
                            "loyality_card_id": lt,
                            "loyality_card_number": lcard.card_number,
                            "type": "accrual",
                            "name": f"Кешбек по документу {instance_values['number']}",
                            "amount": calculated_cashback_sum,
                            "created_by_id": user.id,
                            "tags": tags,
                            "card_balance": lcard.balance,
                            "dated": datetime.datetime.now(),
                            "cashbox": user.cashbox_id,
                            "is_deleted": False,
                            "created_at": datetime.datetime.now(),
                            "updated_at": datetime.datetime.now(),
                            "status": True,
                        }

                        lt_id = await database.execute(
                            loyality_transactions.insert().values(rubles_body)
                        )

                        await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

            await asyncio.gather(asyncio.create_task(_raschet_debounced(user, token)))
        if lt:
            if paid_lt > 0:
                paybox_q = loyality_cards.select().where(loyality_cards.c.id == lt)
                payboxes = await database.fetch_one(paybox_q)
                print("loyality_transactions insert")
                rubles_body = {
                    "loyality_card_id": lt,
                    "loyality_card_number": payboxes.card_number,
                    "type": "withdraw",
                    "name": f"Оплата по документу {instance_values['number']}",
                    "amount": paid_lt,
                    "created_by_id": user.id,
                    "card_balance": lcard.balance,
                    "tags": tags,
                    "dated": datetime.datetime.now(),
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now(),
                    "status": True,
                }
                print("loyality_transactions insert")
                lt_id = await database.execute(
                    loyality_transactions.insert().values(rubles_body)
                )
                print("loyality_transactions insert")
                await database.execute(
                    loyality_cards.update()
                    .where(loyality_cards.c.card_number == payboxes.card_number)
                    .values({"balance": loyality_cards.c.balance - paid_lt})
                )
                print("loyality_transactions update")
                await database.execute(
                    entity_to_entity.insert().values(
                        {
                            "from_entity": 7,
                            "to_entity": 6,
                            "cashbox_id": user.cashbox_id,
                            "type": "docs_sales_loyality_transactions",
                            "from_id": instance_id,
                            "to_id": lt_id,
                            "status": True,
                            "delinked": False,
                        }
                    )
                )

                await asyncio.gather(asyncio.create_task(raschet_bonuses(lt)))

        query = (
            docs_sales.update()
            .where(docs_sales.c.id == instance_id)
            .values({"sum": round(items_sum, 2)})
        )
        await database.execute(query)

        if generate_out:
            goods_res = []
            for good in goods:
                nomenclature_id = int(good["nomenclature"])
                nomenclature_db = await database.fetch_one(
                    nomenclature.select().where(nomenclature.c.id == nomenclature_id)
                )
                if nomenclature_db.type == "product":
                    goods_res.append(
                        {
                            "price_type": 1,
                            "price": 0,
                            "quantity": good["quantity"],
                            "unit": good["unit"],
                            "nomenclature": nomenclature_id,
                        }
                    )

            body = {
                "number": None,
                "dated": instance_values["dated"],
                "docs_purchases": None,
                "to_warehouse": None,
                "status": True,
                "contragent": instance_values["contragent"],
                "organization": instance_values["organization"],
                "operation": "outgoing",
                "comment": instance_values["comment"],
                "warehouse": instance_values["warehouse"],
                "docs_sales_id": instance_id,
                "goods": goods_res,
            }
            body["docs_purchases"] = None
            body["number"] = None
            body["to_warehouse"] = None
            await create_warehouse_docs(token, body, user.cashbox_id)

    query = docs_sales.select().where(docs_sales.c.id.in_(inserted_ids))
    docs_sales_db = await database.fetch_all(query)
    docs_sales_db = [*map(datetime_to_timestamp, docs_sales_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_sales",
            "result": docs_sales_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_sales_db


@router.patch("/docs_sales/{idx}/", response_model=schemas.ListView)
async def update(token: str, docs_sales_data: schemas.EditMass):
    """Редактирование документов"""
    user = await get_user_by_token(token)

    updated_ids = set()
    exceptions = []

    clients_ids = {i.client for i in docs_sales_data.__root__ if i.client is not None}
    contragent_ids = {
        i.contragent for i in docs_sales_data.__root__ if i.contragent is not None
    }
    contract_ids = {
        i.contract for i in docs_sales_data.__root__ if i.contract is not None
    }
    organization_ids = {
        i.organization for i in docs_sales_data.__root__ if i.organization is not None
    }
    warehouse_ids = {
        i.warehouse for i in docs_sales_data.__root__ if i.warehouse is not None
    }
    sales_manager_ids = {
        i.sales_manager for i in docs_sales_data.__root__ if i.sales_manager is not None
    }
    loyalty_card_ids = {
        i.loyality_card_id
        for i in docs_sales_data.__root__
        if i.loyality_card_id is not None
    }
    goods_nomenclature_ids = {
        int(g.nomenclature)
        for i in docs_sales_data.__root__
        if i.goods
        for g in i.goods
        if g.nomenclature is not None
    }
    goods_price_type_ids = {
        int(g.price_type)
        for i in docs_sales_data.__root__
        if i.goods
        for g in i.goods
        if g.price_type is not None
    }
    goods_unit_ids = {
        int(g.unit)
        for i in docs_sales_data.__root__
        if i.goods
        for g in i.goods
        if g.unit is not None
    }
    goods_docs_ids = {
        i.id for i in docs_sales_data.__root__ if i.goods and i.id is not None
    }

    contragent_ids_total = clients_ids | contragent_ids

    found_contragents = set()
    if contragent_ids_total:
        q_contragents = (
            select(contragents.c.id)
            .where(
                contragents.c.id.in_(contragent_ids_total),
                contragents.c.cashbox == user.cashbox_id,
            )
            .with_only_columns([contragents.c.id])
        )
        found_contragents = {row.id for row in await database.fetch_all(q_contragents)}
        contragents_cache.update(found_contragents)

    found_contracts = set()
    if contract_ids:
        q_contracts = select(contracts.c.id).where(
            contracts.c.id.in_(contract_ids),
            contracts.c.cashbox == user.cashbox_id,
            contracts.c.is_deleted.is_not(True),
        )
        found_contracts = {row.id for row in await database.fetch_all(q_contracts)}
        contracts_cache.update(found_contracts)

    found_organizations = set()
    if organization_ids:
        q_orgs = select(organizations.c.id).where(
            organizations.c.id.in_(organization_ids),
            organizations.c.cashbox == user.cashbox_id,
            organizations.c.is_deleted.is_not(True),
        )
        found_organizations = {row.id for row in await database.fetch_all(q_orgs)}
        organizations_cache.update(found_organizations)

    found_warehouses = set()
    if warehouse_ids:
        q_warehouses = select(warehouses.c.id).where(
            warehouses.c.id.in_(warehouse_ids),
            warehouses.c.cashbox == user.cashbox_id,
            warehouses.c.is_deleted.is_not(True),
        )
        found_warehouses = {row.id for row in await database.fetch_all(q_warehouses)}
        warehouses_cache.update(found_warehouses)

    found_users = set()
    if sales_manager_ids:
        q_users = (
            select(users_cboxes_relation.c.id)
            .where(
                users_cboxes_relation.c.id.in_(sales_manager_ids),
                users_cboxes_relation.c.cashbox_id == user.cashbox_id,
            )
            .with_only_columns([users_cboxes_relation.c.id])
        )
        found_users = {row.id for row in await database.fetch_all(q_users)}
        users_cache.update(found_users)

    found_price_types = set()
    if goods_price_type_ids:
        q_price_types = select(price_types.c.id).where(
            price_types.c.id.in_(goods_price_type_ids)
        )
        found_price_types = {row.id for row in await database.fetch_all(q_price_types)}
        price_types_cache.update(found_price_types)

    found_units = set()
    if goods_unit_ids:
        q_units = select(units.c.id).where(units.c.id.in_(goods_unit_ids))
        found_units = {row.id for row in await database.fetch_all(q_units)}
        units_cache.update(found_units)

    loyalty_cards_map: Dict[int, Any] = {}
    found_loyalty_cards = set()
    if loyalty_card_ids:
        q_loyalty_cards = select(
            loyality_cards.c.id,
            loyality_cards.c.card_number,
            loyality_cards.c.balance,
            loyality_cards.c.cashback_percent,
        ).where(
            loyality_cards.c.id.in_(loyalty_card_ids),
            loyality_cards.c.cashbox_id == user.cashbox_id,
            loyality_cards.c.is_deleted.is_not(True),
        )
        loyalty_cards_rows = await database.fetch_all(q_loyalty_cards)
        found_loyalty_cards = {row.id for row in loyalty_cards_rows}
        loyalty_cards_map = {row.id: row for row in loyalty_cards_rows}

    nomenclature_map: Dict[int, Any] = {}
    if goods_nomenclature_ids:
        nom_rows = await database.fetch_all(
            select(
                nomenclature.c.id,
                nomenclature.c.cashback_type,
                nomenclature.c.cashback_value,
                nomenclature.c.type,
            ).where(nomenclature.c.id.in_(goods_nomenclature_ids))
        )
        nomenclature_map = {row.id: row for row in nom_rows}

    missing_contragents = contragent_ids_total - found_contragents
    missing_contracts = contract_ids - found_contracts
    missing_organizations = organization_ids - found_organizations
    missing_warehouses = warehouse_ids - found_warehouses
    missing_users = sales_manager_ids - found_users
    missing_price_types = goods_price_type_ids - found_price_types
    missing_units = goods_unit_ids - found_units
    missing_loyalty_cards = loyalty_card_ids - found_loyalty_cards

    if (
        missing_contragents
        or missing_contracts
        or missing_organizations
        or missing_warehouses
        or missing_users
        or missing_price_types
        or missing_units
        or missing_loyalty_cards
    ):
        errors = []
        if missing_contragents:
            errors.append(f"contragents: {sorted(missing_contragents)}")
        if missing_contracts:
            errors.append(f"contracts: {sorted(missing_contracts)}")
        if missing_organizations:
            errors.append(f"organizations: {sorted(missing_organizations)}")
        if missing_warehouses:
            errors.append(f"warehouses: {sorted(missing_warehouses)}")
        if missing_users:
            errors.append(f"users: {sorted(missing_users)}")
        if missing_price_types:
            errors.append(f"price_types: {sorted(missing_price_types)}")
        if missing_units:
            errors.append(f"units: {sorted(missing_units)}")
        if missing_loyalty_cards:
            errors.append(f"loyality_cards: {sorted(missing_loyalty_cards)}")
        raise HTTPException(
            400,
            "Не найдены связанные сущности перед обновлением docs_sales: "
            + "; ".join(errors),
        )

    payload_ids = [inst.id for inst in docs_sales_data.__root__ if inst.id is not None]

    existing_docs_map: Dict[int, Any] = {}
    if payload_ids:
        docs_q = select(
            docs_sales.c.id,
            docs_sales.c.organization,
            docs_sales.c.contragent,
            docs_sales.c.dated,
            docs_sales.c.comment,
            docs_sales.c.warehouse,
        ).where(docs_sales.c.id.in_(payload_ids))
        docs_rows = await database.fetch_all(docs_q)
        existing_docs_map = {row.id: row for row in docs_rows}

    conds = []
    for d in docs_sales_data.__root__:
        org_id = d.organization
        if org_id is None and d.id in existing_docs_map:
            org_id = existing_docs_map[d.id].organization

        if d.dated is not None and org_id is not None:
            conds.append(
                and_(
                    fifo_settings.c.organization_id == org_id,
                    fifo_settings.c.blocked_date >= d.dated,
                )
            )
    if conds:
        blocked = await database.fetch_all(
            select(fifo_settings.c.organization_id, fifo_settings.c.blocked_date).where(
                or_(*conds)
            )
        )
        if blocked:
            bad_orgs = {b.organization_id for b in blocked}
            raise HTTPException(
                400,
                f"Период закрыт для организаций: {', '.join(map(str, bad_orgs))}",
            )

    paybox_default = None
    paybox_row = await database.fetch_one(
        pboxes.select().where(pboxes.c.cashbox == user.cashbox_id)
    )
    if paybox_row:
        paybox_default = paybox_row.id
    ids_with_settings = set()
    settings_map: Dict[int, Optional[int]] = {}
    if payload_ids:
        settings_q = select(docs_sales.c.id, docs_sales.c.settings).where(
            docs_sales.c.id.in_(payload_ids), docs_sales.c.settings.is_not(None)
        )
        settings_rows = await database.fetch_all(settings_q)
        ids_with_settings = {row.id for row in settings_rows}
        settings_map = {row.id: row.settings for row in settings_rows}

    proxies_by_from_id: Dict[int, list] = defaultdict(list)
    if payload_ids:
        proxy_query = entity_to_entity.select().where(
            entity_to_entity.c.cashbox_id == user.cashbox_id,
            entity_to_entity.c.from_id.in_(payload_ids),
        )
        proxy_rows = await database.fetch_all(proxy_query)
        for proxy in proxy_rows:
            proxies_by_from_id[proxy.from_id].append(proxy)
    loyalty_proxy_ids = {
        proxy.to_id
        for proxies in proxies_by_from_id.values()
        for proxy in proxies
        if proxy.to_entity == 6
    }
    loyalty_transactions_map: Dict[int, Any] = {}
    if loyalty_proxy_ids:
        loyalty_tx_rows = await database.fetch_all(
            select(loyality_transactions.c.id, loyality_transactions.c.type).where(
                loyality_transactions.c.id.in_(loyalty_proxy_ids),
                loyality_transactions.c.cashbox == user.cashbox_id,
                loyality_transactions.c.status == True,
                loyality_transactions.c.is_deleted.is_not(True),
            )
        )
        loyalty_transactions_map = {row.id: row for row in loyalty_tx_rows}

    doc_warehouse_map: Dict[int, Any] = {}

    if goods_docs_ids:
        latest_doc_wh = (
            select(
                func.max(docs_warehouse.c.id).label("id"),
                docs_warehouse.c.docs_sales_id,
            )
            .where(docs_warehouse.c.docs_sales_id.in_(goods_docs_ids))
            .group_by(docs_warehouse.c.docs_sales_id)
        ).subquery()

        doc_warehouse_rows = await database.fetch_all(
            select(docs_warehouse.c.id, docs_warehouse.c.docs_sales_id).where(
                docs_warehouse.c.id.in_(select(latest_doc_wh.c.id))
            )
        )
        doc_warehouse_map = {row.docs_sales_id: row for row in doc_warehouse_rows}

    # Массовое добавление/обновление настроек
    settings_to_update: Dict[int, Dict[str, Any]] = {}
    settings_to_add: Dict[int, Dict[str, Any]] = {}
    # Массовое обновление/создание платежей
    payments_to_update: list[dict[str, Any]] = []
    payments_to_add: list[dict[str, Any]] = []
    # Массовое обновление транзакций лояльности
    loyalty_transactions_to_update: list[dict[str, Any]] = []
    loyalty_transactions_to_insert: list[dict[str, Any]] = []
    loyalty_transactions_doc_ids: list[int] = []
    cards_for_bonus: set[int] = set()
    docs_sales_updates: list[dict[str, Any]] = []
    docs_update_keys: set[str] = set()
    docs_goods_to_delete: set[int] = set()
    docs_goods_to_insert: list[dict[str, Any]] = []
    doc_sum_updates: list[dict[str, Any]] = []
    warehouse_balance_pending: list[dict[str, Any]] = []
    warehouse_balances_to_insert: list[dict[str, Any]] = []
    warehouse_updates_payload: list[dict[str, Any]] = []

    for index, instance in enumerate(docs_sales_data.__root__):
        instance_values = instance.dict(exclude_unset=True)

        goods: Union[list, None] = instance_values.pop("goods", None)

        paid_rubles = instance_values.pop("paid_rubles", 0)
        paid_lt = instance_values.pop("paid_lt", 0)
        lt = instance_values.pop("loyality_card_id", None)

        paybox = instance_values.pop("paybox", None)
        if paybox is None:
            paybox = paybox_default

        instance_id_db = instance_values["id"]

        settings: Optional[Dict[str, Any]] = instance_values.pop("settings", None)
        if settings:
            if instance_id_db in ids_with_settings:
                settings_to_update[instance_id_db] = settings
            else:
                settings_to_add[instance_id_db] = settings

        if paid_rubles or paid_lt or lt:
            proxyes = proxies_by_from_id.get(instance_values["id"], [])

            proxy_payment = False
            existing_withdraw_id = None
            existing_accrual_id = None

            for proxy in proxyes:
                if proxy.from_entity == 7:
                    # Платеж
                    if proxy.to_entity == 5:
                        payments_to_update.append(
                            {
                                "payment_id": proxy.to_id,
                                "amount": paid_rubles,
                                "amount_without_tax": paid_rubles,
                            }
                        )
                        proxy_payment = True
                    # Транзакция
                    if proxy.to_entity == 6:
                        txn = loyalty_transactions_map.get(proxy.to_id)
                        if txn:
                            if txn.type == "withdraw":
                                existing_withdraw_id = txn.id
                            else:
                                existing_accrual_id = txn.id

            full_payment = float(paid_rubles) + float(paid_lt)
            cashback_sum = 0.0
            if lt and goods:
                share_rubles = paid_rubles / full_payment if full_payment else 0
                lcard_row = loyalty_cards_map.get(lt)
                card_percent = (lcard_row.cashback_percent or 0) if lcard_row else 0
                for item in goods:
                    nom_id = int(item["nomenclature"])
                    nom = nomenclature_map.get(nom_id)
                    if not nom:
                        continue
                    if nom.cashback_type == NomenclatureCashbackType.no_cashback:
                        continue
                    elif nom.cashback_type == NomenclatureCashbackType.percent:
                        current_percent = (
                            item["price"]
                            * item["quantity"]
                            * (nom.cashback_value / 100)
                        )
                        cashback_sum += share_rubles * current_percent
                    elif nom.cashback_type == NomenclatureCashbackType.const:
                        cashback_sum += item["quantity"] * nom.cashback_value
                    elif nom.cashback_type == NomenclatureCashbackType.lcard_cashback:
                        current_percent = (
                            item["price"] * item["quantity"] * (card_percent / 100)
                        )
                        cashback_sum += share_rubles * current_percent
                    else:
                        current_percent = (
                            item["price"] * item["quantity"] * (card_percent / 100)
                        )
                        cashback_sum += share_rubles * current_percent

            if not proxy_payment:
                payments_to_add.append(
                    {
                        "contragent": instance_values["contragent"],
                        "type": "outgoing",
                        "name": f"Оплата по документу {instance_values['number']}",
                        "amount_without_tax": instance_values.get("paid_rubles"),
                        "amount": instance_values.get("paid_rubles"),
                        "paybox": paybox,
                        "tags": instance_values.get("tags", ""),
                        "date": int(datetime.datetime.now().timestamp()),
                        "account": user.user,
                        "cashbox": user.cashbox_id,
                        "is_deleted": False,
                        "created_at": int(datetime.datetime.now().timestamp()),
                        "updated_at": int(datetime.datetime.now().timestamp()),
                        "status": True,
                        "stopped": True,
                        "docs_sales_id": instance_id_db,
                    }
                )

            withdraw_amount = paid_lt if lt and paid_lt > 0 else 0
            if withdraw_amount:
                lcard = loyalty_cards_map.get(lt)
                if not lcard:
                    raise HTTPException(
                        400, f"Карта лояльности {lt} не найдена перед списанием"
                    )
                if existing_withdraw_id:
                    loyalty_transactions_to_update.append(
                        {
                            "transaction_id": existing_withdraw_id,
                            "amount": withdraw_amount,
                        }
                    )
                else:
                    loyalty_transactions_to_insert.append(
                        {
                            "loyality_card_id": lt,
                            "loyality_card_number": lcard.card_number,
                            "type": "withdraw",
                            "name": f"Оплата по документу {instance_values['number']}",
                            "amount": withdraw_amount,
                            "created_by_id": user.id,
                            "tags": instance_values.get("tags", ""),
                            "dated": datetime.datetime.utcnow(),
                            "card_balance": lcard.balance,
                            "cashbox": user.cashbox_id,
                            "is_deleted": False,
                            "created_at": datetime.datetime.utcnow(),
                            "updated_at": datetime.datetime.utcnow(),
                            "status": True,
                        }
                    )
                    loyalty_transactions_doc_ids.append(instance_id_db)
                cards_for_bonus.add(lt)

            if lt and cashback_sum > 0:
                lcard = loyalty_cards_map.get(lt)
                if not lcard:
                    raise HTTPException(
                        400, f"Карта лояльности {lt} не найдена перед начислением"
                    )
                if existing_accrual_id:
                    loyalty_transactions_to_update.append(
                        {
                            "transaction_id": existing_accrual_id,
                            "amount": round(cashback_sum, 2),
                        }
                    )
                else:
                    loyalty_transactions_to_insert.append(
                        {
                            "loyality_card_id": lt,
                            "loyality_card_number": lcard.card_number,
                            "type": "accrual",
                            "name": f"Кешбек по документу {instance_values['number']}",
                            "amount": round(cashback_sum, 2),
                            "created_by_id": user.id,
                            "card_balance": lcard.balance,
                            "tags": instance_values.get("tags", ""),
                            "dated": datetime.datetime.utcnow(),
                            "cashbox": user.cashbox_id,
                            "is_deleted": False,
                            "created_at": datetime.datetime.utcnow(),
                            "updated_at": datetime.datetime.utcnow(),
                            "status": True,
                        }
                    )
                    loyalty_transactions_doc_ids.append(instance_id_db)
                cards_for_bonus.add(lt)

        if instance_values.get("paid_rubles"):
            del instance_values["paid_rubles"]

        update_payload = dict(instance_values)
        docs_update_keys.update(set(update_payload.keys()) - {"id"})
        docs_sales_updates.append(update_payload)

        instance_id = instance_values["id"]
        updated_ids.add(instance_values["id"])

        existing_doc = existing_docs_map.get(instance_id)
        if not existing_doc:
            continue

        org_id = instance_values.get("organization") or existing_doc.organization
        contragent_id = instance_values.get("contragent") or existing_doc.contragent
        warehouse_id = instance_values.get("warehouse") or existing_doc.warehouse
        dated_val = instance_values.get("dated") or existing_doc.dated
        comment_val = instance_values.get("comment") or existing_doc.comment

        if goods:
            items_sum = 0
            for item in goods:
                item["docs_sales_id"] = instance_id
                item["nomenclature"] = int(item["nomenclature"])
                docs_goods_to_insert.append(item)

                items_sum += item["price"] * item["quantity"]
                if warehouse_id is not None:
                    warehouse_balance_pending.append(
                        {
                            "organization_id": org_id,
                            "warehouse_id": warehouse_id,
                            "nomenclature_id": item["nomenclature"],
                            "document_sale_id": instance_id,
                            "outgoing_amount": item["quantity"],
                        }
                    )

            docs_goods_to_delete.add(instance_id)
            doc_sum_updates.append({"id": instance_id, "sum": round(items_sum, 2)})

            doc_warehouse = doc_warehouse_map.get(instance_id)

            goods_res = []
            for good in goods:
                nomenclature_db = nomenclature_map.get(int(good["nomenclature"]))
                if nomenclature_db and nomenclature_db.type == "product":
                    goods_res.append(
                        {
                            "price_type": 1,
                            "price": 0,
                            "quantity": good["quantity"],
                            "unit": good["unit"],
                            "nomenclature": good["nomenclature"],
                        }
                    )

            if doc_warehouse:
                body = WarehouseUpdate(
                    __root__=[
                        {
                            "id": doc_warehouse.id,
                            "number": None,
                            "dated": dated_val,
                            "docs_purchases": None,
                            "to_warehouse": None,
                            "status": True,
                            "contragent": contragent_id,
                            "operation": "outgoing",
                            "comment": comment_val,
                            "warehouse": warehouse_id,
                            "docs_sales_id": instance_id,
                            "goods": goods_res,
                            "organization": org_id,
                        }
                    ]
                )

                warehouse_updates_payload.extend(body.__root__)

    if warehouse_balance_pending:
        balance_pairs = {
            (row["warehouse_id"], row["nomenclature_id"])
            for row in warehouse_balance_pending
        }
        balance_conditions = [
            and_(
                warehouse_balances.c.warehouse_id == wh,
                warehouse_balances.c.nomenclature_id == nom,
            )
            for wh, nom in balance_pairs
        ]
        latest_map: Dict[tuple[int, int], float] = {}
        if balance_conditions:
            subq = (
                select(
                    warehouse_balances.c.warehouse_id,
                    warehouse_balances.c.nomenclature_id,
                    warehouse_balances.c.current_amount,
                    func.row_number()
                    .over(
                        partition_by=(
                            warehouse_balances.c.warehouse_id,
                            warehouse_balances.c.nomenclature_id,
                        ),
                        order_by=warehouse_balances.c.created_at.desc(),
                    )
                    .label("rn"),
                ).where(
                    warehouse_balances.c.cashbox_id == user.cashbox_id,
                    or_(*balance_conditions),
                )
            ).subquery()

            latest_rows = await database.fetch_all(select(subq).where(subq.c.rn == 1))
            latest_map = {
                (row.warehouse_id, row.nomenclature_id): row.current_amount
                for row in latest_rows
            }

        running_balance = dict(latest_map)
        for row in warehouse_balance_pending:
            key = (row["warehouse_id"], row["nomenclature_id"])
            prev_amount = running_balance.get(key, 0)
            current_amount = prev_amount - row["outgoing_amount"]
            running_balance[key] = current_amount
            warehouse_balances_to_insert.append(
                {
                    "organization_id": row["organization_id"],
                    "warehouse_id": row["warehouse_id"],
                    "nomenclature_id": row["nomenclature_id"],
                    "document_sale_id": row["document_sale_id"],
                    "outgoing_amount": row["outgoing_amount"],
                    "current_amount": current_amount,
                    "cashbox_id": user.cashbox_id,
                }
            )

    new_payment_links: list[dict[str, Any]] = []
    if payments_to_add:
        payments_insert_query = (
            payments.insert()
            .values(payments_to_add)
            .returning(payments.c.id, payments.c.docs_sales_id)
        )
        inserted_payments = await database.fetch_all(payments_insert_query)
        for row in inserted_payments:
            new_payment_links.append(
                {
                    "from_entity": 7,
                    "to_entity": 5,
                    "cashbox_id": user.cashbox_id,
                    "type": "docs_sales_payments",
                    "from_id": row.docs_sales_id,
                    "to_id": row.id,
                    "status": True,
                    "delinked": False,
                }
            )

    if new_payment_links:
        await database.execute_many(entity_to_entity.insert(), new_payment_links)

    if docs_goods_to_delete:
        delete_goods_query = docs_sales_goods.delete().where(
            docs_sales_goods.c.docs_sales_id.in_(docs_goods_to_delete)
        )
        await database.execute(delete_goods_query)
    if docs_goods_to_insert:
        await database.execute_many(docs_sales_goods.insert(), docs_goods_to_insert)

    if warehouse_balances_to_insert:
        await database.execute_many(
            warehouse_balances.insert(), warehouse_balances_to_insert
        )

    # Временно отключено.
    # Складские документы ведутся через отдельную ручку
    # /api/v1/alt_docs_warehouse/, поэтому PATCH /docs_sales не должен
    # дополнительно запускать фоновый rewrite связанного docs_warehouse.
    # Разделяем ответственность: docs_sales отвечает за продажу,
    # alt_docs_warehouse — за склад и складские движения.
    #
    # if warehouse_updates_payload:
    #     asyncio.create_task(
    #         update_warehouse_doc(
    #             token, WarehouseUpdate(__root__=warehouse_updates_payload)
    #         )
    #     )

    if doc_sum_updates:
        sum_update_query = "UPDATE docs_sales SET sum = :sum WHERE id = :id"
        await database.execute_many(sum_update_query, doc_sum_updates)

    if docs_sales_updates and docs_update_keys:
        for row in docs_sales_updates:
            for key in docs_update_keys:
                row.setdefault(key, None)

        update_keys = sorted(docs_update_keys)
        set_clause = ", ".join(f"{key} = :{key}" for key in update_keys)
        docs_update_query = f"UPDATE docs_sales SET {set_clause} WHERE id = :id"

        await database.execute_many(docs_update_query, docs_sales_updates)

    new_loyalty_links: list[dict[str, Any]] = []
    if loyalty_transactions_to_insert:
        loyalty_insert_query = (
            loyality_transactions.insert()
            .values(loyalty_transactions_to_insert)
            .returning(loyality_transactions.c.id)
        )
        inserted_loyalty = await database.fetch_all(loyalty_insert_query)
        for doc_id, row in zip(loyalty_transactions_doc_ids, inserted_loyalty):
            new_loyalty_links.append(
                {
                    "from_entity": 7,
                    "to_entity": 6,
                    "cashbox_id": user.cashbox_id,
                    "type": "docs_sales_loyality_transactions",
                    "from_id": doc_id,
                    "to_id": row.id,
                    "status": True,
                    "delinked": False,
                }
            )

    if new_loyalty_links:
        await database.execute_many(entity_to_entity.insert(), new_loyalty_links)

    if payments_to_update:
        for payment_row in payments_to_update:
            payment_row["cashbox"] = user.cashbox_id
        payments_update_query = (
            "UPDATE payments "
            "SET amount = :amount, amount_without_tax = :amount_without_tax "
            "WHERE id = :payment_id AND cashbox = :cashbox AND status = TRUE AND is_deleted = FALSE"
        )
        await database.execute_many(payments_update_query, payments_to_update)

    asyncio.create_task(_raschet_debounced(user, token))

    if loyalty_transactions_to_update:
        for txn_row in loyalty_transactions_to_update:
            txn_row["cashbox"] = user.cashbox_id
        loyalty_update_query = (
            "UPDATE loyality_transactions "
            "SET amount = :amount "
            "WHERE id = :transaction_id AND cashbox = :cashbox AND status = TRUE AND is_deleted = FALSE"
        )
        await database.execute_many(
            loyalty_update_query, loyalty_transactions_to_update
        )

    if cards_for_bonus:
        for card_id in cards_for_bonus:
            asyncio.create_task(raschet_bonuses(card_id))

    settings_keys = [
        "repeatability_period",
        "repeatability_value",
        "date_next_created",
        "transfer_from_weekends",
        "skip_current_month",
        "repeatability_count",
        "default_payment_status",
        "repeatability_tags",
        "repeatability_status",
    ]
    if settings_to_add:
        doc_ids_add = []
        settings_values = []
        for doc_id, settings in settings_to_add.items():
            doc_ids_add.append(doc_id)
            settings_values.append({key: settings.get(key) for key in settings_keys})

        insert_settings_query = (
            docs_sales_settings.insert()
            .values(settings_values)
            .returning(docs_sales_settings.c.id)
        )
        inserted_rows = await database.fetch_all(insert_settings_query)

        rows_update = []
        for doc_id, row in zip(doc_ids_add, inserted_rows):
            if not row:
                continue
            rows_update.append({"doc_id": doc_id, "settings_id": row.id})

        if rows_update:
            update_new_settings = (
                "UPDATE docs_sales SET settings = :settings_id WHERE id = :doc_id"
            )
            await database.execute_many(update_new_settings, rows_update)

    if settings_to_update:
        values = []
        for doc_id, settings in settings_to_update.items():
            settings_id = settings_map.get(doc_id)
            if not settings_id:
                continue
            row = {"settings_id": settings_id}
            for key in settings_keys:
                row[key] = settings.get(key)
            values.append(row)
        if values:
            set_clause = ", ".join(f"{key} = :{key}" for key in settings_keys)
            update_query = (
                f"UPDATE docs_sales_settings SET {set_clause} WHERE id = :settings_id"
            )
            await database.execute_many(update_query, values)

    query = docs_sales.select().where(docs_sales.c.id.in_(updated_ids))
    docs_sales_db = await database.fetch_all(query)
    docs_sales_db = [*map(datetime_to_timestamp, docs_sales_db)]

    asyncio.create_task(
        manager.send_message(
            token,
            {
                "action": "edit",
                "target": "docs_sales",
                "result": docs_sales_db,
            },
        )
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return docs_sales_db


@router.delete("/docs_sales/", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Пакетное удаление документов"""
    await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id.in_(ids), docs_sales.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_sales.update()
            .where(docs_sales.c.id.in_(ids), docs_sales.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_sales",
                "result": items_db,
            },
        )

    return items_db


@router.delete("/docs_sales/{idx}/", response_model=schemas.ListView)
async def delete(token: str, idx: int):
    """Удаление документа"""
    await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_sales.update()
            .where(docs_sales.c.id == idx, docs_sales.c.is_deleted.is_not(True))
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_sales",
                "result": items_db,
            },
        )

    return items_db


@router.post(
    "/docs_sales/{idx}/delivery_info/",
    response_model=schemas.ResponseDeliveryInfoSchema,
)
async def delivery_info(token: str, idx: int, data: schemas.DeliveryInfoSchema):
    """Добавление информации о доставке в заказу"""
    user = await get_user_by_token(token)

    check_query = select(docs_sales.c.id).where(
        and_(
            docs_sales.c.id == idx,
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.is_deleted == False,
        )
    )

    item_db = await database.fetch_one(check_query)
    if not item_db:
        raise HTTPException(404, "Документ не найден!")

    data_dict = data.dict()
    data_dict["docs_sales_id"] = idx
    if data_dict.get("delivery_date") or data_dict.get("delivery_date") == 0:
        data_dict["delivery_date"] = datetime.datetime.fromtimestamp(
            data_dict["delivery_date"]
        )

    check_delivery_info_query = select(docs_sales_delivery_info.c.id).where(
        docs_sales_delivery_info.c.docs_sales_id == idx
    )
    delivery_info_db = await database.fetch_one(check_delivery_info_query)
    if delivery_info_db:
        query = (
            docs_sales_delivery_info.update()
            .values(data_dict)
            .where(docs_sales_delivery_info.c.docs_sales_id == idx)
            .returning(docs_sales_delivery_info.c.id)
        )
    else:
        query = docs_sales_delivery_info.insert().values(data_dict)

    entity_id = await database.execute(query)

    return schemas.ResponseDeliveryInfoSchema(
        id=entity_id, docs_sales_id=idx, **data.dict()
    )


@router.get("/docs_sales/{idx}/links", response_model=schemas.OrderLinksResponse)
async def get_order_links(token: str, idx: int):
    """Получение сгенерированных ссылок для заказа"""
    user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    links_data = await generate_and_save_order_links(idx)

    if not links_data:
        raise HTTPException(status_code=500, detail="Не удалось сгенерировать ссылки")

    return schemas.OrderLinksResponse(**links_data)


@router.post("/docs_sales/{idx}/notify", response_model=schemas.NotifyResponse)
async def notify_order(
    token: str, idx: int, notify_config: schemas.NotifyConfig = Depends()
):
    """Генерация и отправка уведомлений о заказе"""
    user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    order_data = dict(order)

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_data = [dict(good) for good in goods_db]

    contragent_data = {}
    if order.contragent:
        query = contragents.select().where(contragents.c.id == order.contragent)
        contragent = await database.fetch_one(query)
        if contragent:
            contragent_data = dict(contragent)

    delivery_info = None
    query = docs_sales_delivery_info.select().where(
        docs_sales_delivery_info.c.docs_sales_id == idx
    )
    delivery = await database.fetch_one(query)
    if delivery:
        delivery_info = dict(delivery)

    links_data = await generate_and_save_order_links(idx)

    if not links_data:
        raise HTTPException(status_code=500, detail="Не удалось сгенерировать ссылки")

    hashes = {
        "general": links_data["general_link"]["hash"],
        "picker": links_data["picker_link"]["hash"],
        "courier": links_data["courier_link"]["hash"],
    }

    links = {
        "general_url": links_data["general_link"]["url"],
        "picker_url": links_data["picker_link"]["url"],
        "courier_url": links_data["courier_link"]["url"],
    }

    notify_type_str = notify_config.type.value

    notification_text = format_notification_text(
        notification_type=notify_type_str,
        order_data=order_data,
        goods_data=goods_data,
        contragent_data=contragent_data,
        delivery_info=delivery_info,
        links=links,
        hashes=hashes,
    )

    recipients = []

    if notify_config.type == schemas.NotifyType.assembly:
        if order.assigned_picker:
            if await check_user_on_shift(
                order.assigned_picker, check_shift_settings=True
            ):
                picker_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id == order.assigned_picker)
                )
                picker = await database.fetch_one(picker_query)
                if picker and picker.chat_id:
                    recipients.append(picker.chat_id)

        if not recipients:
            available_pickers = await get_available_pickers_on_shift(order.cashbox)

            if available_pickers:
                pickers_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id.in_(available_pickers))
                )
                pickers = await database.fetch_all(pickers_query)
                for picker in pickers:
                    if picker.chat_id:
                        recipients.append(picker.chat_id)

    elif notify_config.type == schemas.NotifyType.delivery:
        if order.assigned_courier:
            if await check_user_on_shift(
                order.assigned_courier, check_shift_settings=True
            ):
                courier_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id == order.assigned_courier)
                )
                courier = await database.fetch_one(courier_query)
                if courier and courier.chat_id:
                    recipients.append(courier.chat_id)

        if not recipients:
            available_couriers = await get_available_couriers_on_shift(order.cashbox)

            if available_couriers:
                couriers_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id.in_(available_couriers))
                )
                couriers = await database.fetch_all(couriers_query)
                for courier in couriers:
                    if courier.chat_id:
                        recipients.append(courier.chat_id)

    elif notify_config.type == schemas.NotifyType.general:
        if order.assigned_picker and await check_user_on_shift(
            order.assigned_picker, check_shift_settings=True
        ):
            picker_query = (
                select([users.c.chat_id])
                .select_from(
                    users.join(
                        users_cboxes_relation,
                        users.c.id == users_cboxes_relation.c.user,
                    )
                )
                .where(users_cboxes_relation.c.id == order.assigned_picker)
            )
            picker = await database.fetch_one(picker_query)
            if picker and picker.chat_id:
                recipients.append(picker.chat_id)

        if order.assigned_courier and await check_user_on_shift(
            order.assigned_courier, check_shift_settings=True
        ):
            courier_query = (
                select([users.c.chat_id])
                .select_from(
                    users.join(
                        users_cboxes_relation,
                        users.c.id == users_cboxes_relation.c.user,
                    )
                )
                .where(users_cboxes_relation.c.id == order.assigned_courier)
            )
            courier = await database.fetch_one(courier_query)
            if courier and courier.chat_id:
                recipients.append(courier.chat_id)

        if not recipients:
            all_available = []
            available_pickers = await get_available_pickers_on_shift(
                order.cashbox
            )  # По умолчанию учитывает настройки
            available_couriers = await get_available_couriers_on_shift(
                order.cashbox
            )  # По умолчанию учитывает настройки
            all_available.extend(available_pickers)
            all_available.extend(available_couriers)
            all_available = list(set(all_available))

            if all_available:
                workers_query = (
                    select([users.c.chat_id])
                    .select_from(
                        users.join(
                            users_cboxes_relation,
                            users.c.id == users_cboxes_relation.c.user,
                        )
                    )
                    .where(users_cboxes_relation.c.id.in_(all_available))
                )
                workers = await database.fetch_all(workers_query)
                for worker in workers:
                    if worker.chat_id:
                        recipients.append(worker.chat_id)

    # Если никого не найдено среди работников со сменами - уведомляем админа
    if not recipients:
        owner_query = (
            select([users.c.chat_id])
            .select_from(
                users.join(
                    users_cboxes_relation, users.c.id == users_cboxes_relation.c.user
                )
            )
            .where(
                users_cboxes_relation.c.cashbox_id == order.cashbox,
                users_cboxes_relation.c.is_owner,
            )
        )
        owner = await database.fetch_one(owner_query)
        if owner and owner.chat_id:
            recipients.append(owner.chat_id)

    print(f"Determined recipients: {recipients}")

    if notify_config.type.value == "Общее":
        notify_type_str = "general"
    elif notify_config.type.value == "Сборка":
        notify_type_str = "assembly"
    elif notify_config.type.value == "Доставка":
        notify_type_str = "delivery"
    else:
        notify_type_str = notify_config.type.value

    await send_order_notification(
        notification_type=notify_type_str,
        order_id=idx,
        order_data=order_data,
        recipient_ids=recipients,
        notification_text=notification_text,
        links=links,
    )

    response = {
        "success": True,
        "message": f"Уведомление '{notify_config.type}' сформировано и отправлено",
    }

    if notify_config.type == schemas.NotifyType.general:
        response["general_url"] = links["general_url"]
    elif notify_config.type == schemas.NotifyType.assembly:
        response["picker_url"] = links["picker_url"]
    elif notify_config.type == schemas.NotifyType.delivery:
        response["courier_url"] = links["courier_url"]

    return response


@router.patch("/docs_sales/{idx}/status", response_model=schemas.View)
async def update_order_status(
    token: str, idx: int, status_update: schemas.OrderStatusUpdate
):
    """Обновление статуса заказа"""
    user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    current_status = order.order_status or OrderStatus.received
    target_status = status_update.status

    valid_transitions = {
        OrderStatus.received: [OrderStatus.processed, OrderStatus.closed],
        OrderStatus.processed: [OrderStatus.collecting, OrderStatus.closed],
        OrderStatus.collecting: [OrderStatus.collected, OrderStatus.closed],
        OrderStatus.collected: [OrderStatus.picked, OrderStatus.closed],
        OrderStatus.picked: [OrderStatus.delivered, OrderStatus.closed],
        OrderStatus.delivered: [OrderStatus.success, OrderStatus.closed],
    }

    if target_status not in valid_transitions.get(current_status, []):
        raise HTTPException(
            status_code=400,
            detail=f"Недопустимый переход статуса с '{current_status}' на '{target_status}'",
        )

    update_values = {"order_status": target_status}

    notification_recipients = []

    # Автоматическое назначение сборщика при переходе в статус "Сборка начата"
    if target_status == OrderStatus.collecting:
        update_values["picker_started_at"] = datetime.datetime.now()
        # Если сборщик еще не назначен, назначаем текущего пользователя
        if not order.assigned_picker:
            update_values["assigned_picker"] = user.id

        # Проверяем назначенного сборщика
        assigned_picker = order.assigned_picker or user.id
        if await check_user_on_shift(assigned_picker):
            notification_recipients.append(assigned_picker)
        else:
            # Ищем всех доступных сборщиков на смене
            available_pickers = await get_available_pickers_on_shift(order.cashbox)
            notification_recipients.extend(available_pickers)

    elif target_status == OrderStatus.picked:
        update_values["courier_picked_at"] = datetime.datetime.now()
        # Если курьер еще не назначен, назначаем текущего пользователя
        if not order.assigned_courier:
            update_values["assigned_courier"] = user.id

        assigned_courier = order.assigned_courier or user.id
        if await check_user_on_shift(assigned_courier):
            notification_recipients.append(assigned_courier)
        else:
            available_couriers = await get_available_couriers_on_shift(order.cashbox)
            notification_recipients.extend(available_couriers)

    elif target_status == OrderStatus.collected:
        update_values["picker_finished_at"] = datetime.datetime.now()
        if order.assigned_courier:
            if await check_user_on_shift(order.assigned_courier):
                notification_recipients.append(order.assigned_courier)
            else:
                available_couriers = await get_available_couriers_on_shift(
                    order.cashbox
                )
                notification_recipients.extend(available_couriers)

    elif target_status == OrderStatus.delivered:
        update_values["courier_delivered_at"] = datetime.datetime.now()

    notification_recipients = list(set(notification_recipients))

    if status_update.comment:
        update_values["comment"] = (
            f"{order.comment or ''}\n[{datetime.datetime.now()}] {status_update.comment}"
        )

    query = docs_sales.update().where(docs_sales.c.id == idx).values(update_values)
    await database.execute(query)

    query = docs_sales.select().where(docs_sales.c.id == idx)
    updated_order = await database.fetch_one(query)
    updated_order = datetime_to_timestamp(updated_order)
    updated_order = await raschet_oplat(updated_order)
    updated_order = await add_docs_sales_settings(updated_order)

    # Получаем данные о назначенных пользователях
    if updated_order.get("assigned_picker"):
        user_query = users.select().where(
            users.c.id == updated_order["assigned_picker"]
        )
        picker_user = await database.fetch_one(user_query)
        if picker_user:
            updated_order["assigned_picker"] = {
                "id": picker_user.id,
                "first_name": picker_user.first_name,
                "last_name": picker_user.last_name,
            }
            await manager.send_message(
                token,
                {
                    "action": "assign_user",
                    "target": "docs_sales",
                    "id": idx,
                    "role": "picker",
                    "user_id": picker_user.id,
                },
            )

    if updated_order.get("assigned_courier"):
        user_query = users.select().where(
            users.c.id == updated_order["assigned_courier"]
        )
        courier_user = await database.fetch_one(user_query)
        if courier_user:
            updated_order["assigned_courier"] = {
                "id": courier_user.id,
                "first_name": courier_user.first_name,
                "last_name": courier_user.last_name,
            }
            await manager.send_message(
                token,
                {
                    "action": "assign_user",
                    "target": "docs_sales",
                    "id": idx,
                    "role": "courier",
                    "user_id": courier_user.id,
                },
            )

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
    goods_db = [await instance for instance in goods_db]

    updated_order["goods"] = goods_db
    updated_order = await add_delivery_info_to_doc(updated_order)

    await manager.send_message(
        token,
        {
            "action": "update_status",
            "target": "docs_sales",
            "id": idx,
            "status": target_status,
        },
    )

    if notification_recipients:
        recipient_chat_ids = []
        for recipient_id in notification_recipients:
            recipient_query = (
                select([users.c.chat_id])
                .select_from(
                    users_cboxes_relation.join(
                        users, users_cboxes_relation.c.user == users.c.id
                    )
                )
                .where(users_cboxes_relation.c.id == recipient_id)
            )
            recipient = await database.fetch_one(recipient_query)
            if recipient and recipient.chat_id:
                recipient_chat_ids.append(recipient.chat_id)

        links_data = await generate_and_save_order_links(idx)

        if not links_data:
            links_data = await generate_and_save_order_links(idx)

        links = {
            "general_url": links_data["general_link"]["url"],
            "picker_url": links_data["picker_link"]["url"],
            "courier_url": links_data["courier_link"]["url"],
        }

        notification_data = {
            "type": "status_change",
            "order_id": idx,
            "previous_status": current_status,
            "status": target_status,
            "recipients": recipient_chat_ids,
            "links": links,
            "updated_by": user.id,
            "updated_at": datetime.datetime.now().timestamp(),
        }

        await queue_notification(notification_data)

    return updated_order


@router.patch("/docs_sales/{idx}/assign/{role}", response_model=schemas.View)
async def assign_user_to_order(token: str, idx: int, role: schemas.AssignUserRole):
    """Назначение сборщика или курьера для заказа"""
    current_user = await get_user_by_token(token)

    query = docs_sales.select().where(
        docs_sales.c.id == idx,
        docs_sales.c.is_deleted.is_not(True),
        docs_sales.c.cashbox == current_user.cashbox_id,
    )
    order = await database.fetch_one(query)

    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    if role == schemas.AssignUserRole.picker:
        update_field = "assigned_picker"
    else:  # courier
        update_field = "assigned_courier"

    query = (
        docs_sales.update()
        .where(docs_sales.c.id == idx)
        .values({update_field: current_user.id})
    )
    await database.execute(query)

    query = docs_sales.select().where(docs_sales.c.id == idx)
    updated_order = await database.fetch_one(query)
    updated_order = datetime_to_timestamp(updated_order)
    updated_order = await raschet_oplat(updated_order)
    updated_order = await add_docs_sales_settings(updated_order)

    query = docs_sales_goods.select().where(docs_sales_goods.c.docs_sales_id == idx)
    goods_db = await database.fetch_all(query)
    goods_db = [*map(datetime_to_timestamp, goods_db)]
    goods_db = [*map(add_nomenclature_name_to_goods, goods_db)]
    goods_db = [await instance for instance in goods_db]

    updated_order["goods"] = goods_db
    updated_order = await add_delivery_info_to_doc(updated_order)

    await manager.send_message(
        token,
        {
            "action": "assign_user",
            "target": "docs_sales",
            "id": idx,
            "role": role,
            "user_id": current_user.id,
        },
    )

    return updated_order


@router.get("/docs_sales/verify/{hash}")
async def verify_hash_and_get_order(hash: str, order_id: int, role: str):
    """Проверка валидности хеш-ссылки и получение информации о заказе"""
    expected_hash = generate_notification_hash(order_id, role)

    if hash != expected_hash:
        raise HTTPException(status_code=403, detail="Недействительная ссылка")

    if role == "general" or role == "courier":
        query = docs_sales.select().where(
            docs_sales.c.id == order_id, docs_sales.c.is_deleted.is_not(True)
        )
        order = await database.fetch_one(query)

        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")

        if role == "general":
            order_data = datetime_to_timestamp(order)
            return order_data

        elif role == "courier":
            courier_data = {
                "id": order.id,
                "number": order.number,
                "status": order.order_status,
                "assigned_courier": order.assigned_courier,
            }

            query = docs_sales_delivery_info.select().where(
                docs_sales_delivery_info.c.docs_sales_id == order_id
            )
            delivery = await database.fetch_one(query)

            if delivery:
                courier_data["delivery"] = {
                    "address": delivery.address,
                    "delivery_date": delivery.delivery_date,
                    "delivery_price": delivery.delivery_price,
                    "recipient": delivery.recipient,
                    "note": delivery.note,
                }

            return courier_data

    elif role == "picker":
        query = f"""
            SELECT
                sales.*,
                {", ".join(f"warehouse.{c.name} AS warehouse_{c.name}" for c in warehouses.c)},
                {", ".join(f"contragent.{c.name} AS contragent_{c.name}" for c in contragents.c)}
            FROM docs_sales sales
            LEFT JOIN warehouses warehouse ON warehouse.id = sales.warehouse
            LEFT JOIN contragents contragent ON contragent.id = sales.contragent
            WHERE sales.id = :order_id AND sales.is_deleted IS NOT TRUE
        """
        order = await database.fetch_one(query, {"order_id": order_id})
        order_dict = dict(order)

        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")

        order_dict["status"] = order_dict["order_status"]

        query = f"""
            select
                "goods".*,
                {", ".join(f"nomenclature.{c.name} AS nomenclature_{c.name}" for c in nomenclature.c)},
                "pictures"."id" AS "picture_id",
                "pictures"."url" AS "picture_url",
                "pictures"."is_main" AS "picture_is_main",
                "unit"."id" as "nomenclature_unit_id",
                "unit"."convent_national_view" as "nomenclature_unit_convent_national_view"
            from "docs_sales_goods" "goods"
            left join "nomenclature" "nomenclature"
            on "goods"."nomenclature" = "nomenclature"."id"
            left join "units" "unit"
            on "nomenclature"."unit" = "unit"."id"
            left join lateral (
                select "id", "url", "is_main"
                from "pictures"
                where
                    "entity" = 'nomenclature' AND
                    "entity_id" = "nomenclature"."id"
                order by
                    "is_main" desc,
                    "id" asc
                limit 1
            ) "pictures" on true
            where "goods"."docs_sales_id" = :order_id
        """
        goods = await database.fetch_all(query, {"order_id": order_id})

        if goods:
            order_dict["goods"] = goods

        # собираем инфу о доставке
        query = docs_sales_delivery_info.select().where(
            docs_sales_delivery_info.c.docs_sales_id == order_id
        )
        delivery = await database.fetch_one(query)

        if delivery:
            order_dict["delivery"] = {
                "address": delivery.address,
                "delivery_date": delivery.delivery_date,
                "delivery_price": delivery.delivery_price,
                "recipient": delivery.recipient,
                "note": delivery.note,
            }

        return order_dict
    else:
        raise HTTPException(status_code=400, detail="Неизвестная роль")


@router.get("/docs_sales/stats", response_model=schemas.CashierStats)
async def get_cashier_stats(
    token: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
):
    """Получение статистики кассира за период."""
    try:
        user = await get_user_by_token(token)

        if year is None or month is None:
            now = datetime.datetime.now()
            year = year or now.year
            month = month or now.month

        first_day_dt = datetime.datetime(year, month, 1)
        last_day_dt = datetime.datetime(
            year, month, calendar.monthrange(year, month)[1], 23, 59, 59
        )
        first_day = int(first_day_dt.timestamp())
        last_day = int(last_day_dt.timestamp())

        date_from = date_from or first_day
        date_to = date_to or last_day

        conditions = [
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.dated >= date_from,
            docs_sales.c.dated <= date_to,
            docs_sales.c.is_deleted.is_not(True),
        ]

        query = select(
            docs_sales.c.id,
            docs_sales.c.order_status,
            docs_sales.c.sum,
            docs_sales.c.status,
            docs_sales.c.picker_started_at,
            docs_sales.c.picker_finished_at,
        ).where(and_(*conditions))

        orders = await database.fetch_all(query)

        if not orders:
            return schemas.CashierStats(
                orders_completed=0,
                errors=0,
                rating=0.0,
                average_check=0.0,
                hours_processed=0.0,
                successful_orders_percent=0.0,
            )

        total_orders = len(orders)
        completed = sum(1 for o in orders if o["order_status"] == "success")
        errors = sum(1 for o in orders if o["order_status"] == "closed")
        total_sum = sum(o["sum"] or 0 for o in orders)

        average_check = total_sum / total_orders if total_orders > 0 else 0.0

        hours_processed = 0.0
        for order in orders:
            if order["picker_started_at"] and order["picker_finished_at"]:
                diff = order["picker_finished_at"] - order["picker_started_at"]
                hours_processed += diff.total_seconds() / 3600

        hours_processed = float(hours_processed)

        successful_percent = (
            (completed / total_orders * 100) if total_orders > 0 else 0.0
        )

        rating = 0.0

        return schemas.CashierStats(
            orders_completed=completed,
            errors=errors,
            rating=rating,
            average_check=average_check,
            hours_processed=hours_processed,
            successful_orders_percent=successful_percent,
        )
    except Exception:
        return schemas.CashierStats(
            orders_completed=0,
            errors=0,
            rating=0.0,
            average_check=0.0,
            hours_processed=0.0,
            successful_orders_percent=0.0,
        )


@router.get("/docs_sales/analytics", response_model=schemas.AnalyticsResponse)
async def get_docs_sales_analytics(
    token: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
    role: Optional[str] = Query(None, description="picker, courier, или manager"),
):
    """Получение детальной аналитики по заказам за период."""
    try:
        user = await get_user_by_token(token)

        if year is None or month is None:
            now = datetime.datetime.now()
            year = year or now.year
            month = month or now.month

        first_day_dt = datetime.datetime(year, month, 1)
        last_day_dt = datetime.datetime(
            year, month, calendar.monthrange(year, month)[1], 23, 59, 59
        )
        first_day = int(first_day_dt.timestamp())
        last_day = int(last_day_dt.timestamp())

        date_from = date_from or first_day
        date_to = date_to or last_day

        conditions = [
            docs_sales.c.cashbox == user.cashbox_id,
            docs_sales.c.dated >= date_from,
            docs_sales.c.dated <= date_to,
            docs_sales.c.is_deleted.is_not(True),
        ]

        if role == "picker":
            conditions.append(docs_sales.c.assigned_picker == user.user)
        elif role == "courier":
            conditions.append(docs_sales.c.assigned_courier == user.user)
        elif role == "manager":
            conditions.append(docs_sales.c.created_by == user.user)

        query = (
            select(
                docs_sales.c.id,
                docs_sales.c.dated,
                docs_sales.c.order_status,
                docs_sales.c.sum,
                docs_sales.c.status,
            )
            .where(and_(*conditions))
            .order_by(docs_sales.c.dated)
        )

        orders = await database.fetch_all(query)

        days_data = {}
        for order in orders:
            order_dt = datetime.datetime.fromtimestamp(order["dated"])
            day_dt = order_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            day_timestamp = int(day_dt.timestamp())
            day_number = order_dt.day

            if day_timestamp not in days_data:
                days_data[day_timestamp] = {
                    "date": day_timestamp,
                    "day_number": day_number,
                    "orders_created": 0,
                    "orders_paid": 0,
                    "revenue": 0.0,
                    "by_status": {
                        "received": 0,
                        "processed": 0,
                        "collecting": 0,
                        "collected": 0,
                        "picked": 0,
                        "delivered": 0,
                        "closed": 0,
                        "success": 0,
                    },
                }

            days_data[day_timestamp]["orders_created"] += 1
            days_data[day_timestamp]["revenue"] += float(order["sum"] or 0.0)

            if order["status"] is True:
                days_data[day_timestamp]["orders_paid"] += 1

            status = order["order_status"]
            if status in days_data[day_timestamp]["by_status"]:
                days_data[day_timestamp]["by_status"][status] += 1

        days_list = []
        total_orders = 0
        total_revenue = 0.0
        total_paid = 0
        peak_day_date = None
        peak_day_orders = 0
        orders_completed = 0
        orders_planned = 0
        orders_cancelled = 0

        today_dt = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        today_timestamp = int(today_dt.timestamp())

        for day_timestamp in sorted(days_data.keys()):
            day_data = days_data[day_timestamp]
            total_orders += day_data["orders_created"]
            total_revenue += day_data["revenue"]
            total_paid += day_data["orders_paid"]

            orders_completed += day_data["by_status"]["success"]
            orders_planned += (
                day_data["by_status"]["received"]
                + day_data["by_status"]["processed"]
                + day_data["by_status"]["collecting"]
                + day_data["by_status"]["collected"]
            )
            orders_cancelled += day_data["by_status"]["closed"]

            if day_data["orders_created"] > peak_day_orders:
                peak_day_orders = day_data["orders_created"]
                peak_day_date = day_timestamp

            days_list.append(
                schemas.DayAnalytics(
                    date=day_data["date"],
                    day_number=day_data["day_number"],
                    orders_created=day_data["orders_created"],
                    orders_paid=day_data["orders_paid"],
                    revenue=day_data["revenue"],
                    by_status=schemas.DayStatusBreakdown(**day_data["by_status"]),
                )
            )

        days_count = len(days_list)
        average_daily_load = total_orders / days_count if days_count > 0 else 0.0

        today_data = days_data.get(
            today_timestamp,
            {
                "orders_created": 0,
                "revenue": 0.0,
                "by_status": {
                    "success": 0,
                    "received": 0,
                    "processed": 0,
                    "collecting": 0,
                    "collected": 0,
                    "closed": 0,
                },
            },
        )

        today_completed = today_data["by_status"].get("success", 0)
        today_planned = (
            today_data["by_status"].get("received", 0)
            + today_data["by_status"].get("processed", 0)
            + today_data["by_status"].get("collecting", 0)
            + today_data["by_status"].get("collected", 0)
        )
        today_cancelled = today_data["by_status"].get("closed", 0)

        return schemas.AnalyticsResponse(
            period=schemas.AnalyticsPeriod(date_from=date_from, date_to=date_to),
            filter=schemas.AnalyticsFilter(role=role, user_id=user.user),
            summary=schemas.AnalyticsSummary(
                total_orders=total_orders,
                total_revenue=total_revenue,
                total_paid=total_paid,
                average_daily_load=average_daily_load,
                peak_day_date=peak_day_date or date_from,
                peak_day_orders=peak_day_orders,
                orders_completed=orders_completed,
                orders_planned=orders_planned,
                orders_cancelled=orders_cancelled,
                today_total_orders=today_data["orders_created"],
                today_revenue=today_data["revenue"],
                today_completed=today_completed,
                today_planned=today_planned,
                today_cancelled=today_cancelled,
            ),
            days=days_list,
        )
    except Exception:
        today_dt = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        today_timestamp = int(today_dt.timestamp())
        return schemas.AnalyticsResponse(
            period=schemas.AnalyticsPeriod(
                date_from=date_from or int(today_dt.timestamp()),
                date_to=date_to or int(today_dt.timestamp()),
            ),
            filter=schemas.AnalyticsFilter(role=role, user_id=0),
            summary=schemas.AnalyticsSummary(
                total_orders=0,
                total_revenue=0.0,
                total_paid=0,
                average_daily_load=0.0,
                peak_day_date=today_timestamp,
                peak_day_orders=0,
                orders_completed=0,
                orders_planned=0,
                orders_cancelled=0,
                today_total_orders=0,
                today_revenue=0.0,
                today_completed=0,
                today_planned=0,
                today_cancelled=0,
            ),
            days=[],
        )
