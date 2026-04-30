import logging
from datetime import datetime
from typing import Optional

import asyncpg
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.orders_service.schemas import MarketplaceOrderGood
from api.marketplace.service.product_cart_service.schemas import (
    MarketplaceAddToCartRequest,
    MarketplaceCartGoodRepresentation,
    MarketplaceCartResponse,
    MarketplaceGetCartRequest,
    MarketplaceRemoveFromCartRequest,
)
from database.db import (
    database,
    marketplace_cart_goods,
    marketplace_carts,
    marketplace_clients_list,
    nomenclature,
)
from fastapi import HTTPException
from sqlalchemy import and_, delete, select, update

logger = logging.getLogger(__name__)


class MarketplaceCartService(BaseMarketplaceService):
    async def add_to_cart(
        self, request: MarketplaceAddToCartRequest
    ) -> MarketplaceCartResponse:
        """
        Добавить товар в корзину (корзина создаётся автоматически).

        Логика как в старом коде:
        1) получить/создать корзину
        2) проверить, есть ли уже такая позиция (nomenclature_id + warehouse_id)
        3) если есть — увеличить quantity, иначе вставить новую строку
        4) вернуть корзину
        """

        phone = BaseMarketplaceService._validate_phone(request.contragent_phone)
        print(
            f"[ADD TO CART] Request: phone={phone}, nomenclature_id={request.good.nomenclature_id}, warehouse_id={request.good.warehouse_id}, quantity={request.good.quantity}"
        )
        logger.info(
            f"[ADD TO CART] Request: phone={phone}, nomenclature_id={request.good.nomenclature_id}, warehouse_id={request.good.warehouse_id}, quantity={request.good.quantity}"
        )

        client_id = await self._ensure_marketplace_client(phone)
        print(f"[ADD TO CART] Client ID: {client_id}")
        logger.info(f"[ADD TO CART] Client ID: {client_id}")

        product_query = select(nomenclature.c.id).where(
            and_(
                nomenclature.c.id == request.good.nomenclature_id,
                nomenclature.c.is_deleted == False,
            )
        )
        product = await database.fetch_one(product_query)
        if not product:
            logger.warning(
                f"[ADD TO CART] Product not found: nomenclature_id={request.good.nomenclature_id}"
            )
            raise HTTPException(
                status_code=404, detail="Товар не найден или не доступен"
            )

        cart_id = await self._get_or_create_cart(client_id, phone)
        print(f"[ADD TO CART] Cart ID: {cart_id}")
        logger.info(f"[ADD TO CART] Cart ID: {cart_id}")

        existing_item = await self._get_cart_item(
            cart_id=cart_id,
            nomenclature_id=request.good.nomenclature_id,
            warehouse_id=request.good.warehouse_id,
        )
        print(f"[ADD TO CART] Existing item: {existing_item}")

        if existing_item:
            new_quantity = existing_item.quantity + request.good.quantity
            print(
                f"[ADD TO CART] Updating existing item: id={existing_item.id}, old_quantity={existing_item.quantity}, new_quantity={new_quantity}"
            )
            logger.info(
                f"[ADD TO CART] Updating existing item: id={existing_item.id}, old_quantity={existing_item.quantity}, new_quantity={new_quantity}"
            )
            upd = (
                update(marketplace_cart_goods)
                .where(marketplace_cart_goods.c.id == existing_item.id)
                .values(quantity=new_quantity, updated_at=datetime.utcnow())
            )
            await database.execute(upd)
            print("[ADD TO CART] Update executed")
        else:
            print(
                f"[ADD TO CART] Inserting new item: nomenclature_id={request.good.nomenclature_id}, warehouse_id={request.good.warehouse_id}, quantity={request.good.quantity}"
            )
            logger.info(
                f"[ADD TO CART] Inserting new item: nomenclature_id={request.good.nomenclature_id}, warehouse_id={request.good.warehouse_id}, quantity={request.good.quantity}"
            )
            ins = marketplace_cart_goods.insert().values(
                nomenclature_id=request.good.nomenclature_id,
                warehouse_id=request.good.warehouse_id,
                quantity=request.good.quantity,
                cart_id=cart_id,
            )
            await database.execute(ins)
            print("[ADD TO CART] Insert executed")

        await database.execute(
            update(marketplace_carts)
            .where(marketplace_carts.c.id == cart_id)
            .values(updated_at=datetime.utcnow())
        )

        result = await self.get_cart(MarketplaceGetCartRequest(contragent_phone=phone))
        print(
            f"[ADD TO CART] Result: total_count={result.total_count}, goods_count={len(result.goods)}"
        )
        logger.info(
            f"[ADD TO CART] Result: total_count={result.total_count}, goods_count={len(result.goods)}"
        )
        return result

    async def get_cart(
        self, request: MarketplaceGetCartRequest
    ) -> MarketplaceCartResponse:
        """Получить содержимое корзины по номеру телефона."""

        phone = BaseMarketplaceService._validate_phone(request.contragent_phone)
        print(f"[GET CART] Request: phone={phone}")

        # Получаем client_id по phone
        client_query = select(marketplace_clients_list.c.id).where(
            marketplace_clients_list.c.phone == phone
        )
        client = await database.fetch_one(client_query)
        if client:
            print(f"[GET CART] Client found: id={client.id}")
        else:
            print("[GET CART] Client not found")

        if not client:
            return MarketplaceCartResponse(
                contragent_phone=phone,
                goods=[],
                total_count=0,
            )

        # Сначала ищем по client_id
        cart_query = select(
            marketplace_carts.c.id,
            marketplace_carts.c.client_id,
            marketplace_carts.c.phone,
        ).where(marketplace_carts.c.client_id == client.id)
        cart = await database.fetch_one(cart_query)

        # Если не нашли по client_id, ищем по phone
        if not cart:
            print(
                f"[GET CART] Cart not found by client_id={client.id}, trying by phone={phone}"
            )
            cart_query = select(
                marketplace_carts.c.id,
                marketplace_carts.c.client_id,
                marketplace_carts.c.phone,
            ).where(marketplace_carts.c.phone == phone)
            cart = await database.fetch_one(cart_query)

        if cart:
            print(
                f"[GET CART] Cart found: id={cart.id}, client_id={cart.client_id}, phone={cart.phone}"
            )
        else:
            print(f"[GET CART] Cart not found for client_id={client.id}, phone={phone}")

        if not cart:
            print("[GET CART] Cart not found")
            return MarketplaceCartResponse(
                contragent_phone=phone,
                goods=[],
                total_count=0,
            )

        items_query = select(
            marketplace_cart_goods.c.id,
            marketplace_cart_goods.c.nomenclature_id,
            marketplace_cart_goods.c.warehouse_id,
            marketplace_cart_goods.c.quantity,
            marketplace_cart_goods.c.created_at,
            marketplace_cart_goods.c.updated_at,
        ).where(marketplace_cart_goods.c.cart_id == cart.id)

        items = await database.fetch_all(items_query)
        print(f"[GET CART] Items found: {len(items)}")

        goods = [
            MarketplaceOrderGood(
                nomenclature_id=item.nomenclature_id,
                warehouse_id=item.warehouse_id,
                quantity=item.quantity,
            )
            for item in items
        ]

        print(
            f"[GET CART] Returning: total_count={len(items)}, goods_count={len(goods)}"
        )
        return MarketplaceCartResponse(
            contragent_phone=phone,
            goods=goods,
            total_count=len(items),
        )

    async def remove_from_cart(
        self, request: MarketplaceRemoveFromCartRequest
    ) -> MarketplaceCartResponse:
        """
        Удалить товар из корзины.

        В отличие от старого кода, не полагаемся на "result == 0" от database.execute(delete),
        а сначала ищем строку — так надёжнее.
        """

        phone = BaseMarketplaceService._validate_phone(request.contragent_phone)

        # Получаем client_id по phone
        client_query = select(marketplace_clients_list.c.id).where(
            marketplace_clients_list.c.phone == phone
        )
        client = await database.fetch_one(client_query)
        if not client:
            raise HTTPException(
                status_code=404, detail="Client not found for this phone"
            )

        cart = await database.fetch_one(
            select(marketplace_carts.c.id).where(
                marketplace_carts.c.client_id == client.id
            )
        )
        if not cart:
            raise HTTPException(status_code=404, detail="Cart not found for this phone")

        conditions = [
            marketplace_cart_goods.c.cart_id == cart.id,
            marketplace_cart_goods.c.nomenclature_id == request.nomenclature_id,
        ]
        if request.warehouse_id is not None:
            conditions.append(
                marketplace_cart_goods.c.warehouse_id == request.warehouse_id
            )
        else:
            conditions.append(marketplace_cart_goods.c.warehouse_id.is_(None))

        item = await database.fetch_one(
            select(marketplace_cart_goods.c.id).where(and_(*conditions))
        )
        if not item:
            raise HTTPException(status_code=404, detail="Item not found in cart")

        await database.execute(
            delete(marketplace_cart_goods).where(marketplace_cart_goods.c.id == item.id)
        )

        await database.execute(
            update(marketplace_carts)
            .where(marketplace_carts.c.id == cart.id)
            .values(updated_at=datetime.utcnow())
        )

        return await self.get_cart(MarketplaceGetCartRequest(contragent_phone=phone))

    @staticmethod
    async def _get_or_create_cart(client_id: int, phone: str) -> int:
        """Получить существующую корзину по client_id или создать новую."""
        # Сначала проверяем по client_id (основной способ)
        cart = await database.fetch_one(
            select(marketplace_carts.c.id).where(
                marketplace_carts.c.client_id == client_id
            )
        )
        if cart:
            return cart.id

        phone = BaseMarketplaceService._validate_phone(phone)

        # Если корзины нет по client_id, проверяем по phone
        # (на случай если корзина была создана раньше с другим client_id)
        cart_by_phone = await database.fetch_one(
            select(marketplace_carts.c.id).where(marketplace_carts.c.phone == phone)
        )
        if cart_by_phone:
            # Если найдена корзина по phone, обновляем её client_id только если он отличается
            # Но сначала проверяем, нет ли уже другой корзины с таким client_id
            existing_cart_by_client = await database.fetch_one(
                select(marketplace_carts.c.id).where(
                    marketplace_carts.c.client_id == client_id
                )
            )
            if not existing_cart_by_client:
                # Обновляем client_id только если нет конфликта
                try:
                    await database.execute(
                        update(marketplace_carts)
                        .where(marketplace_carts.c.id == cart_by_phone.id)
                        .values(client_id=client_id)
                    )
                except asyncpg.exceptions.UniqueViolationError:
                    # Если возник конфликт (например, другой client_id уже использует этот cart), просто возвращаем найденную корзину
                    pass
            return cart_by_phone.id

        # Пытаемся создать новую корзину
        try:
            cart_id = await database.execute(
                marketplace_carts.insert().values(client_id=client_id, phone=phone)
            )
            return cart_id
        except asyncpg.exceptions.UniqueViolationError:
            # Если все еще возник конфликт (race condition), получаем существующую корзину
            cart_final = await database.fetch_one(
                select(marketplace_carts.c.id).where(marketplace_carts.c.phone == phone)
            )
            if cart_final:
                return cart_final.id
            # Если не найдена корзина по phone, пробуем по client_id
            cart_by_client_final = await database.fetch_one(
                select(marketplace_carts.c.id).where(
                    marketplace_carts.c.client_id == client_id
                )
            )
            if cart_by_client_final:
                return cart_by_client_final.id
            raise HTTPException(
                status_code=500, detail="Failed to create or retrieve cart"
            )

    @staticmethod
    async def _get_cart_item(
        cart_id: int,
        nomenclature_id: int,
        warehouse_id: Optional[int] = None,
    ) -> Optional[MarketplaceCartGoodRepresentation]:
        conditions = [
            marketplace_cart_goods.c.cart_id == cart_id,
            marketplace_cart_goods.c.nomenclature_id == nomenclature_id,
        ]

        if warehouse_id is not None:
            conditions.append(marketplace_cart_goods.c.warehouse_id == warehouse_id)
        else:
            conditions.append(marketplace_cart_goods.c.warehouse_id.is_(None))

        row = await database.fetch_one(
            select(marketplace_cart_goods).where(and_(*conditions))
        )
        if not row:
            return None

        return MarketplaceCartGoodRepresentation(**dict(row))
