import datetime
import json
import traceback
from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from api.docs_sales.api.routers import delivery_info as create_delivery_info
from api.docs_sales.schemas import (
    Create as CreateDocsSales,
    CreateMass as CreateMassDocsSales,
    Item as DocsSalesItem,
)
from api.docs_sales.web.views.CreateDocsSalesView import CreateDocsSalesView
from api.docs_sales_utm_tags.schemas import CreateUTMTag
from api.docs_sales_utm_tags.service import get_docs_sales_utm_service
from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import (
    CreateMarketplaceOrderMessage,
)
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import (
    CreateOrderUtm,
    MarketplaceOrderGood,
)
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from database.db import (
    database,
    marketplace_cart_goods,
    marketplace_carts,
    marketplace_clients_list,
    marketplace_orders,
    organizations,
    prices,
    users_cboxes_relation,
)
from fastapi import HTTPException
from sqlalchemy import and_, delete, select, update


class CreateMarketplaceOrderHandler(IEventHandler[CreateMarketplaceOrderMessage]):
    def __init__(self, rabbit_factory: IRabbitFactory):
        self.__rabbit_factory = rabbit_factory

    @staticmethod
    async def __add_utm(token, entity_id: int, utm: CreateOrderUtm):
        # Сохраняем UTM в docs_sales_utm_tags (для документов продажи)
        service = await get_docs_sales_utm_service()
        try:
            await service.create_utm_tag(token, entity_id, CreateUTMTag(**utm.dict()))
        except HTTPException:
            # UTM не должен ломать создание заказа
            pass

        # Также сохраняем UTM в marketplace_utm_tags (для маркетплейса)
        try:
            await BaseMarketplaceService._add_utm(
                entity_id=entity_id,
                utm=utm,
            )
        except Exception:
            # UTM не должен ломать создание заказа
            pass

    @staticmethod
    async def __set_marketplace_order_status(
        marketplace_order_id: int,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        values = {"status": status}
        if error is not None:
            # Обрежем, чтобы не уронить UPDATE на слишком длинном тексте
            values["error"] = error[:8000]

        stmt = update(marketplace_orders).where(
            marketplace_orders.c.id == marketplace_order_id
        )

        # ВАЖНО: если уже стоит error — не затираем его на success/processing
        if status != "error":
            stmt = stmt.where(marketplace_orders.c.status != "error")

        await database.execute(stmt.values(**values))

    @staticmethod
    async def __clear_cart(phone: str) -> None:
        """Очистить корзину после успешного создания заказа"""
        try:
            print(f"[CART CLEAR] Attempting to clear cart for phone: {phone}")
            # Находим client_id по phone
            client_query = select(marketplace_clients_list.c.id).where(
                marketplace_clients_list.c.phone == phone
            )
            client = await database.fetch_one(client_query)
            if not client:
                print(f"[CART CLEAR] Client not found for phone: {phone}")
                return

            print(f"[CART CLEAR] Found client_id: {client.id}")
            # Находим корзину по client_id
            cart_query = select(marketplace_carts.c.id).where(
                marketplace_carts.c.client_id == client.id
            )
            cart = await database.fetch_one(cart_query)

            # Если не нашли по client_id, ищем по phone (как в get_cart)
            if not cart:
                print(
                    f"[CART CLEAR] Cart not found by client_id: {client.id}, trying by phone: {phone}"
                )
                cart_query = select(marketplace_carts.c.id).where(
                    marketplace_carts.c.phone == phone
                )
                cart = await database.fetch_one(cart_query)

            if not cart:
                print(
                    f"[CART CLEAR] Cart not found for client_id: {client.id} and phone: {phone}"
                )
                return

            # Проверяем количество товаров перед удалением
            count_query = select(marketplace_cart_goods.c.id).where(
                marketplace_cart_goods.c.cart_id == cart.id
            )
            items_before = await database.fetch_all(count_query)
            items_count = len(items_before)
            print(
                f"[CART CLEAR] Found cart_id: {cart.id}, clearing {items_count} goods..."
            )

            if items_count == 0:
                print("[CART CLEAR] Cart is already empty, nothing to delete")
                return

            # Удаляем все товары из корзины
            await database.execute(
                delete(marketplace_cart_goods).where(
                    marketplace_cart_goods.c.cart_id == cart.id
                )
            )

            # Проверяем, что товары действительно удалены
            items_after = await database.fetch_all(count_query)
            remaining_count = len(items_after)
            print(
                f"[CART CLEAR] Deleted {items_count} items from cart_id: {cart.id}, remaining: {remaining_count}"
            )
        except Exception as e:
            # Ошибка очистки корзины не должна ломать создание заказа
            print(f"[CART CLEAR] Error clearing cart: {type(e).__name__}: {e}")
            import traceback

            traceback.print_exc()

    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        data = CreateMarketplaceOrderMessage(**event)

        # Статус: начали обработку
        await self.__set_marketplace_order_status(
            data.marketplace_order_id, "processing"
        )

        try:
            token_query = select(users_cboxes_relation.c.token).where(
                users_cboxes_relation.c.cashbox_id == data.cashbox_id
            )
            token_row = await database.fetch_one(token_query)
            if not token_row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Не найден token для cashbox_id={data.cashbox_id}",
                )
            token = token_row.token

            comment = (
                json.dumps(data.additional_data, ensure_ascii=False)
                if data.additional_data
                else ""
            )

            # разделить по warehouses
            warehouses_dict: dict[tuple[int, int], list[MarketplaceOrderGood]] = {}

            for good in data.goods:
                key = (good.warehouse_id, good.organization_id)
                if warehouses_dict.get(key):
                    warehouses_dict[key].append(good)
                else:
                    warehouses_dict[key] = [good]

            docs_sales_ids: list[int] = []

            # Получаем первую организацию для cashbox (если понадобится)
            default_org_query = (
                select(organizations.c.id)
                .where(
                    and_(
                        organizations.c.cashbox == data.cashbox_id,
                        organizations.c.is_deleted.is_not(True),
                    )
                )
                .limit(1)
            )
            default_org_row = await database.fetch_one(default_org_query)
            default_organization_id = default_org_row.id if default_org_row else None

            for warehouse_and_organization, goods in warehouses_dict.items():
                organization_id = warehouse_and_organization[1]
                warehouse_id = warehouse_and_organization[0]

                # Если organization_id не задан или = -1, используем первую организацию для cashbox
                if organization_id is None or organization_id == -1:
                    if default_organization_id is None:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Не найдена организация для cashbox_id={data.cashbox_id}",
                        )
                    organization_id = default_organization_id

                create_docs_sales_view = CreateDocsSalesView(
                    rabbitmq_messaging_factory=self.__rabbit_factory
                )

                create_result = await create_docs_sales_view.__call__(
                    token=token,
                    docs_sales_data=CreateMassDocsSales(
                        __root__=[
                            CreateDocsSales(
                                contragent=data.contragent_id,
                                organization=organization_id,
                                warehouse=warehouse_id,
                                goods=[
                                    DocsSalesItem(
                                        price=(
                                            await database.fetch_one(
                                                select(prices.c.price).where(
                                                    prices.c.nomenclature
                                                    == good.nomenclature_id
                                                )
                                            )
                                        ).price,
                                        quantity=good.quantity,
                                        nomenclature=good.nomenclature_id,
                                    )
                                    for good in goods
                                ],
                                dated=datetime.datetime.now().timestamp(),
                                is_marketplace_order=True,
                                comment=comment,
                            )
                        ]
                    ),
                )

                docs_sales_id = create_result[0]["id"]
                docs_sales_ids.append(docs_sales_id)

                # Сохраняем связку marketplace_order -> docs_sales.
                # Если заказ разбился на несколько docs_sales, храним первый созданный.
                if len(docs_sales_ids) == 1:
                    await database.execute(
                        update(marketplace_orders)
                        .where(marketplace_orders.c.id == data.marketplace_order_id)
                        .values(docs_sales_id=docs_sales_id)
                    )

                # выставляем delivery_info
                await create_delivery_info(
                    token=token, idx=docs_sales_id, data=data.delivery_info
                )

                # добавляем utm
                if data.utm:
                    await self.__add_utm(token, docs_sales_id, data.utm)

            # Если всё ок — success (success не затирает error, если он уже стоит)
            await self.__set_marketplace_order_status(
                data.marketplace_order_id, "success"
            )

        except Exception as e:
            # Обновляем marketplace_orders.error и пробрасываем дальше,
            # чтобы не менять семантику ack/nack сообщений.
            err_text = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            await self.__set_marketplace_order_status(
                data.marketplace_order_id, "error", err_text
            )
            raise

        # Очищаем корзину после успешного создания заказа (вне try-except, чтобы ошибка не ломала заказ)
        await self.__clear_cart(data.phone)
