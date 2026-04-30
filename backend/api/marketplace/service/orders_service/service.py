import json
import uuid
from typing import Dict, List, Literal, Optional

from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import (
    CreateMarketplaceOrderMessage,
    OrderGoodMessage,
)
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import (
    ORDER_STATUS_MAP,
    CreateOrderUtm,
    LastDeliveryAddressResponse,
    MarketplaceOrderRequest,
    MarketplaceOrderResponse,
    OrderGoodItem,
    OrderItemResponse,
    OrderListResponse,
    OrderSortBy,
    OrderStatusLabel,
)
from api.marketplace.service.products_list_service.schemas import AvailableWarehouse
from database.db import (
    database,
    docs_sales,
    docs_sales_goods,
    marketplace_clients_list,
    marketplace_orders,
    nomenclature,
    organizations,
    units,
    warehouse_balances_latest,
)
from fastapi import HTTPException
from sqlalchemy import (
    Integer as SAInteger,
    and_,
    asc,
    cast,
    desc,
    func,
    select,
    update,
)


class MarketplaceOrdersService(BaseMarketplaceService):
    """Сервис оформления заказа"""

    @staticmethod
    async def __set_marketplace_order_status(
        marketplace_order_id: int, status: str, error: Optional[str] = None
    ) -> None:
        values = {"status": status}
        if error is not None:
            values["error"] = error[:8000]

        stmt = update(marketplace_orders).where(
            marketplace_orders.c.id == marketplace_order_id
        )

        # Не затираем error статусом success/queued
        if status != "error":
            stmt = stmt.where(marketplace_orders.c.status != "error")

        await database.execute(stmt.values(**values))

    @staticmethod
    async def __transform_good(good: OrderGoodMessage) -> OrderGoodMessage:
        # Если organization_id не задан и склад указан - находим organization_id по балансу
        # Если склад не указан (warehouse_id = None) - organization_id остаётся -1
        if good.organization_id == -1 and good.warehouse_id is not None:
            good.organization_id = (
                await BaseMarketplaceService._get_latest_organization_id_for_balance(
                    warehouse_id=good.warehouse_id,
                    nomenclature_id=good.nomenclature_id,
                )
            )
        return good

    @staticmethod
    def _map_order_status(
        docs_sales_order_status: Optional[str], marketplace_status: Optional[str]
    ) -> OrderStatusLabel:
        if docs_sales_order_status:
            status_key = (
                docs_sales_order_status.value
                if hasattr(docs_sales_order_status, "value")
                else str(docs_sales_order_status)
            )
            return ORDER_STATUS_MAP.get(status_key, OrderStatusLabel.processing)
        if marketplace_status == "error":
            return OrderStatusLabel.error
        return OrderStatusLabel.processing

    @staticmethod
    def _extract_delivery_info_fields(delivery_info: Optional[dict]) -> dict:
        info = delivery_info or {}
        if isinstance(info, str):
            try:
                info = json.loads(info)
            except Exception:
                info = {}
        if not isinstance(info, dict):
            info = {}
        recipient = info.get("recipient") or {}
        if isinstance(recipient, str):
            try:
                recipient = json.loads(recipient)
            except Exception:
                recipient = {}
        if not isinstance(recipient, dict):
            recipient = {}

        def _to_float(v) -> Optional[float]:
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        def _to_int(v) -> Optional[int]:
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        return {
            "address": info.get("address"),
            "delivery_date": _to_int(info.get("delivery_date")),
            "delivery_price": _to_float(info.get("delivery_price")),
            "recipient_name": recipient.get("name"),
            "recipient_surname": recipient.get("surname"),
            "recipient_phone": recipient.get("phone"),
            "note": info.get("note"),
            "payment_method": info.get("payment_method"),
        }

    async def get_last_delivery_address(
        self, contragent_phone: str
    ) -> LastDeliveryAddressResponse:
        phone = BaseMarketplaceService._validate_phone(contragent_phone)

        client = await database.fetch_one(
            select(
                marketplace_clients_list.c.id,
                marketplace_clients_list.c.first_name,
                marketplace_clients_list.c.last_name,
            ).where(marketplace_clients_list.c.phone == phone)
        )
        if not client:
            return LastDeliveryAddressResponse(found=False)

        order = await database.fetch_one(
            select(marketplace_orders.c.delivery_info)
            .where(marketplace_orders.c.client_id == client.id)
            .order_by(desc(marketplace_orders.c.created_at))
            .limit(1)
        )
        if not order:
            return LastDeliveryAddressResponse(found=False)

        fields = self._extract_delivery_info_fields(order.delivery_info)
        return LastDeliveryAddressResponse(
            found=True,
            contragent_first_name=client.first_name,
            contragent_last_name=client.last_name,
            **fields,
        )

    async def _fetch_goods_by_docs_sales_ids(
        self, docs_sales_ids: List[int]
    ) -> Dict[int, List[OrderGoodItem]]:
        if not docs_sales_ids:
            return {}

        rows = await database.fetch_all(
            select(
                docs_sales_goods.c.docs_sales_id,
                docs_sales_goods.c.quantity,
                docs_sales_goods.c.price,
                nomenclature.c.id.label("nomenclature_id"),
                nomenclature.c.name,
                units.c.name.label("unit"),
            )
            .select_from(docs_sales_goods)
            .join(nomenclature, nomenclature.c.id == docs_sales_goods.c.nomenclature)
            .outerjoin(units, units.c.id == docs_sales_goods.c.unit)
            .where(docs_sales_goods.c.docs_sales_id.in_(docs_sales_ids))
        )

        result: Dict[int, List[OrderGoodItem]] = {}
        for row in rows:
            ds_id = row.docs_sales_id
            if ds_id not in result:
                result[ds_id] = []
            result[ds_id].append(
                OrderGoodItem(
                    nomenclature_id=row.nomenclature_id,
                    name=row.name,
                    quantity=row.quantity,
                    price=row.price,
                    photo=None,
                    unit=row.unit,
                )
            )
        return result

    async def get_order_by_id(
        self, order_id: int, contragent_phone: str
    ) -> OrderItemResponse:
        phone = BaseMarketplaceService._validate_phone(contragent_phone)

        row = await database.fetch_one(
            select(
                marketplace_orders.c.id,
                marketplace_orders.c.delivery_info,
                marketplace_orders.c.status.label("mp_status"),
                marketplace_orders.c.created_at,
                marketplace_orders.c.docs_sales_id,
                docs_sales.c.number,
                docs_sales.c.order_status,
                docs_sales.c.track_number,
                docs_sales.c.delivery_company,
                docs_sales.c.sum,
            )
            .select_from(marketplace_orders)
            .join(
                marketplace_clients_list,
                marketplace_clients_list.c.id == marketplace_orders.c.client_id,
            )
            .outerjoin(
                docs_sales, docs_sales.c.id == marketplace_orders.c.docs_sales_id
            )
            .where(
                and_(
                    marketplace_orders.c.id == order_id,
                    marketplace_clients_list.c.phone == phone,
                )
            )
        )
        if not row:
            raise HTTPException(status_code=404, detail="Заказ не найден")

        goods_map = await self._fetch_goods_by_docs_sales_ids(
            [row.docs_sales_id] if row.docs_sales_id else []
        )
        status = self._map_order_status(row.order_status, row.mp_status)
        info_fields = self._extract_delivery_info_fields(row.delivery_info)

        return OrderItemResponse(
            id=row.id,
            docs_sales_id=row.docs_sales_id,
            number=row.number,
            status=status,
            track_number=row.track_number,
            delivery_company=row.delivery_company,
            created_at=row.created_at,
            goods=goods_map.get(row.docs_sales_id, []),
            address=info_fields["address"],
            delivery_date=info_fields["delivery_date"],
            delivery_price=info_fields["delivery_price"],
            recipient_name=info_fields["recipient_name"],
            recipient_surname=info_fields["recipient_surname"],
            recipient_phone=info_fields["recipient_phone"],
            note=info_fields["note"],
            payment_method=info_fields["payment_method"],
            total_sum=row.sum,
        )

    async def get_orders_by_phone(
        self,
        contragent_phone: str,
        page: int = 1,
        size: int = 20,
        status_filter: Optional[OrderStatusLabel] = None,
        sort_by: OrderSortBy = OrderSortBy.created_at,
        sort_order: Literal["asc", "desc"] = "desc",
    ) -> OrderListResponse:
        phone = BaseMarketplaceService._validate_phone(contragent_phone)

        client = await database.fetch_one(
            select(marketplace_clients_list.c.id).where(
                marketplace_clients_list.c.phone == phone
            )
        )
        if not client:
            return OrderListResponse(result=[], count=0, page=page, size=size)

        base_query = (
            select(
                marketplace_orders.c.id,
                marketplace_orders.c.delivery_info,
                marketplace_orders.c.status.label("mp_status"),
                marketplace_orders.c.created_at,
                marketplace_orders.c.docs_sales_id,
                docs_sales.c.number,
                docs_sales.c.order_status,
                docs_sales.c.track_number,
                docs_sales.c.delivery_company,
                docs_sales.c.sum,
            )
            .select_from(marketplace_orders)
            .outerjoin(
                docs_sales, docs_sales.c.id == marketplace_orders.c.docs_sales_id
            )
            .where(marketplace_orders.c.client_id == client.id)
        )

        if status_filter:
            if status_filter == OrderStatusLabel.error:
                base_query = base_query.where(marketplace_orders.c.status == "error")
            elif status_filter == OrderStatusLabel.processing:
                base_query = base_query.where(
                    and_(
                        marketplace_orders.c.status != "error",
                        marketplace_orders.c.docs_sales_id.is_(None),
                    )
                )
            else:
                mapped_db_statuses = [
                    db_status
                    for db_status, ui_status in ORDER_STATUS_MAP.items()
                    if ui_status == status_filter
                ]
                if mapped_db_statuses:
                    base_query = base_query.where(
                        docs_sales.c.order_status.in_(mapped_db_statuses)
                    )
                else:
                    return OrderListResponse(result=[], count=0, page=page, size=size)

        total_count = await database.fetch_val(
            select(func.count()).select_from(base_query.subquery())
        )

        if sort_by == OrderSortBy.delivery_date:
            sort_col = cast(
                func.jsonb_extract_path_text(
                    marketplace_orders.c.delivery_info, "delivery_date"
                ),
                SAInteger,
            )
        elif sort_by == OrderSortBy.updated_at:
            sort_col = marketplace_orders.c.updated_at
        else:
            sort_col = marketplace_orders.c.created_at

        order_expr = asc(sort_col) if sort_order == "asc" else desc(sort_col)

        rows = await database.fetch_all(
            base_query.order_by(order_expr).offset((page - 1) * size).limit(size)
        )

        docs_sales_ids = [row.docs_sales_id for row in rows if row.docs_sales_id]
        goods_map = await self._fetch_goods_by_docs_sales_ids(docs_sales_ids)

        result = []
        for row in rows:
            status = self._map_order_status(row.order_status, row.mp_status)
            info_fields = self._extract_delivery_info_fields(row.delivery_info)
            result.append(
                {
                    "id": row.id,
                    "docs_sales_id": row.docs_sales_id,
                    "number": row.number,
                    "status": status,
                    "track_number": row.track_number,
                    "delivery_company": row.delivery_company,
                    "created_at": row.created_at,
                    "goods": goods_map.get(row.docs_sales_id, []),
                    "address": info_fields["address"],
                    "delivery_date": info_fields["delivery_date"],
                    "delivery_price": info_fields["delivery_price"],
                    "recipient_name": info_fields["recipient_name"],
                    "recipient_surname": info_fields["recipient_surname"],
                    "recipient_phone": info_fields["recipient_phone"],
                    "note": info_fields["note"],
                    "payment_method": info_fields["payment_method"],
                    "total_sum": row.sum,
                }
            )

        return OrderListResponse(result=result, count=total_count, page=page, size=size)

    async def create_order(
        self, order_request: MarketplaceOrderRequest, utm: CreateOrderUtm
    ) -> MarketplaceOrderResponse:
        if not self._rabbitmq:
            raise HTTPException(status_code=500, detail="RabbitMQ не инициализирован")

        # Имя заказчика: приоритет contragent_first/last_name, иначе из delivery.recipient
        first_name = order_request.contragent_first_name
        last_name = order_request.contragent_last_name
        if not first_name and not last_name and order_request.delivery.recipient:
            first_name = order_request.delivery.recipient.name
            last_name = order_request.delivery.recipient.surname

        # Формируем полное имя получателя для сохранения в заказе
        recipient_name = None
        name_parts = [n for n in (first_name, last_name) if n]
        if name_parts:
            recipient_name = " ".join(name_parts)

        # Извлекаем UTM метки из utm объекта и ref_user из запроса
        utm_source = utm.utm_source if utm else None
        utm_medium = utm.utm_medium if utm else None
        utm_campaign = utm.utm_campaign if utm else None
        utm_term = utm.utm_term if utm else None
        ref_user = order_request.ref_user

        # Создаем или обновляем клиента marketplace с именами и UTM метками
        # Валидация и нормализация телефона происходит внутри _ensure_marketplace_client
        client_id = await self._ensure_marketplace_client(
            phone=order_request.contragent_phone,
            first_name=first_name,
            last_name=last_name,
            utm_source=utm_source,
            utm_medium=utm_medium,
            utm_campaign=utm_campaign,
            utm_term=utm_term,
            ref_user=ref_user,
        )

        # Получаем нормализованный телефон из созданного/найденного клиента
        # Если _ensure_marketplace_client вернул client_id, клиент гарантированно существует
        client_phone_query = select(marketplace_clients_list.c.phone).where(
            marketplace_clients_list.c.id == client_id
        )
        client_phone_row = await database.fetch_one(client_phone_query)
        if not client_phone_row:
            raise HTTPException(
                status_code=500,
                detail="Клиент marketplace не найден после создания",
            )
        normalized_phone = client_phone_row.phone

        # Валидируем и нормализуем recipient.phone, если он указан
        # Если recipient.phone невалидный - создаем заказ с ошибкой в поле error
        delivery_dict = order_request.delivery.dict()
        order_error = None
        order_status = "created"

        if delivery_dict.get("recipient") and delivery_dict["recipient"].get("phone"):
            recipient_phone = delivery_dict["recipient"]["phone"]
            try:
                # Валидируем recipient.phone
                validated_recipient_phone = BaseMarketplaceService._validate_phone(
                    recipient_phone
                )
                # Используем нормализованный номер для recipient.phone
                delivery_dict["recipient"]["phone"] = validated_recipient_phone
            except HTTPException as e:
                # Если recipient.phone невалидный - сохраняем ошибку в БД, но заказ создаем
                order_error = f"INVALID_RECIPIENT_PHONE: {recipient_phone} - {e.detail}"
                order_status = "error"
                # Оставляем оригинальный невалидный номер в delivery_info для истории

        ins = (
            marketplace_orders.insert()
            .values(
                phone=normalized_phone,  # Используем нормализованный телефон
                client_id=client_id,
                recipient_name=recipient_name,  # Имя получателя из этого конкретного заказа
                delivery_info=delivery_dict,  # Может содержать невалидный recipient.phone, если была ошибка
                additional_data=order_request.additional_data or [],
                status=order_status,  # "created" или "error"
                error=order_error,  # Сохраняем тип ошибки, если recipient.phone невалидный
            )
            .returning(marketplace_orders.c.id)
        )
        row = await database.fetch_one(ins)
        if not row:
            raise HTTPException(
                status_code=500, detail="Не удалось создать marketplace_order"
            )
        marketplace_order_id: int = row.id

        # Если заказ создан с ошибкой (например, невалидный recipient.phone),
        # выбрасываем HTTPException 422 для фронтенда, но заказ уже сохранен в БД
        if order_status == "error" and order_error:
            raise HTTPException(
                status_code=422,
                detail=f"Некорректный номер телефона получателя: {order_error.split(' - ')[-1] if ' - ' in order_error else order_error}",
            )

        goods_dict: dict[int, list[OrderGoodMessage]] = {}

        for good_req in order_request.goods:
            cashbox_query = select(nomenclature.c.cashbox).where(
                and_(
                    nomenclature.c.id == good_req.nomenclature_id,
                    nomenclature.c.is_deleted.is_not(True),
                )
            )
            cashbox_row = await database.fetch_one(cashbox_query)
            if not cashbox_row:
                await self.__set_marketplace_order_status(
                    marketplace_order_id,
                    "error",
                    f"Товар nomenclature_id={good_req.nomenclature_id} не найден",
                )
                raise HTTPException(status_code=404, detail="Товар не найден")

            cashbox_id = cashbox_row.cashbox
            if cashbox_id is None:
                await self.__set_marketplace_order_status(
                    marketplace_order_id,
                    "error",
                    f"Товар nomenclature_id={good_req.nomenclature_id} не привязан к кассе (cashbox is NULL)",
                )
                raise HTTPException(
                    status_code=422,
                    detail=f"Товар nomenclature_id={good_req.nomenclature_id} не привязан к кассе",
                )

            good = OrderGoodMessage(
                organization_id=-1,
                **good_req.dict(),
            )

            # Если склад не задан — подбираем доступный
            # Но если складов нет, склад остаётся None (не обязателен)
            if good.warehouse_id is None:
                warehouses: List[AvailableWarehouse] = (
                    await self._fetch_available_warehouses(
                        nomenclature_id=good.nomenclature_id,
                        client_lat=order_request.client_lat,
                        client_lon=order_request.client_lon,
                    )
                )
                if warehouses:
                    # Если есть доступные склады, выбираем первый
                    selected = warehouses[0]
                    good.warehouse_id = selected.warehouse_id
                    good.organization_id = selected.organization_id
                else:
                    # Если складов нет, пытаемся найти organization_id из любого остатка
                    # для этой номенклатуры (нужно для создания документа продажи)
                    org_query = (
                        select(warehouse_balances_latest.c.organization_id)
                        .where(
                            warehouse_balances_latest.c.nomenclature_id
                            == good.nomenclature_id
                        )
                        .limit(1)
                    )
                    org_row = await database.fetch_one(org_query)
                    if org_row:
                        good.organization_id = org_row.organization_id
                    else:
                        # Если остатков нет, берём первую организацию для cashbox
                        # (нужно для создания документа продажи)
                        default_org_query = (
                            select(organizations.c.id)
                            .where(
                                and_(
                                    organizations.c.cashbox == cashbox_id,
                                    organizations.c.is_deleted.is_not(True),
                                )
                            )
                            .limit(1)
                        )
                        default_org = await database.fetch_one(default_org_query)
                        if default_org:
                            good.organization_id = default_org.id
                        # Если и организации нет, organization_id останется -1
                        # (но это крайний случай, обычно организация должна быть)
                    # warehouse_id остаётся None - это допустимо

            # Если склад задан, но organization_id не задан — вычисляем из последнего balance
            good = await self.__transform_good(good)

            if goods_dict.get(cashbox_id):
                goods_dict[cashbox_id].append(good)
            else:
                goods_dict[cashbox_id] = [good]

        try:
            for cashbox_id, goods in goods_dict.items():
                contragent_id = await self._get_or_create_contragent_id(
                    phone=normalized_phone,  # Используем нормализованный телефон
                    cashbox_id=cashbox_id,
                )

                await self._rabbitmq.publish(
                    CreateMarketplaceOrderMessage(
                        message_id=uuid.uuid4(),
                        marketplace_order_id=marketplace_order_id,
                        phone=normalized_phone,  # Используем нормализованный телефон
                        cashbox_id=cashbox_id,
                        contragent_id=contragent_id,
                        goods=goods,
                        delivery_info=order_request.delivery,
                        utm=utm,
                        additional_data=order_request.additional_data,
                    ),
                    routing_key="create_marketplace_order",
                )

            # Если publish прошёл — считаем, что заказ в очереди
            await self.__set_marketplace_order_status(marketplace_order_id, "queued")

        except Exception as e:
            await self.__set_marketplace_order_status(
                marketplace_order_id, "error", f"{type(e).__name__}: {e}"
            )
            raise

        return MarketplaceOrderResponse(message="Заказ создан и отправлен на обработку")
