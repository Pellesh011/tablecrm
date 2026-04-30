import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import selectinload

logger = logging.getLogger(__name__)


async def get_tech_card_for_nomenclature(
    nomenclature_id: int,
    cashbox_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Ищет активную тех карту через ORM (SQLAlchemy 2.0 style).
    Возвращает dict полностью совместимый со старым кодом.
    """
    from api.tech_cards.models import TechCardDB, TechCardItemDB
    from database.db import (
        async_session_maker,
        nomenclature as nomenclature_table,
    )

    async with async_session_maker() as session:
        # 1. Получаем основную техкарту + output_items
        card_stmt = (
            select(TechCardDB)
            .options(selectinload(TechCardDB.output_items))
            .where(
                TechCardDB.parent_nomenclature_id == nomenclature_id,
                TechCardDB.cashbox_id == cashbox_id,
                TechCardDB.status == "active",
                TechCardDB.card_mode.in_(["semi_auto", "auto"]),
            )
            .order_by(TechCardDB.created_at.desc(), TechCardDB.id.desc())
            .limit(1)
        )

        result = await session.execute(card_stmt)
        tech_card = result.scalar_one_or_none()

        if not tech_card:
            return None

        # 2. Получаем компоненты + название номенклатуры (join без text())
        items_stmt = (
            select(
                TechCardItemDB,
                nomenclature_table.c.name.label("nomenclature_name"),
            )
            .join(
                nomenclature_table,
                nomenclature_table.c.id == TechCardItemDB.nomenclature_id,
                isouter=True,
            )
            .where(TechCardItemDB.tech_card_id == tech_card.id)
            .order_by(TechCardItemDB.id.asc())
        )

        items_result = await session.execute(items_stmt)
        items_rows = items_result.all()

        components = [
            {
                "nomenclature_id": row.TechCardItemDB.nomenclature_id,
                "quantity": row.TechCardItemDB.quantity,
                "name": row.nomenclature_name,
            }
            for row in items_rows
        ]

        output_items = [
            {
                "nomenclature_id": out.nomenclature_id,
                "quantity": out.quantity,
            }
            for out in tech_card.output_items
        ]

        # 3. Собираем полный dict (все поля техкарты + components + output_items)
        card_dict = {
            column.name: getattr(tech_card, column.name)
            for column in TechCardDB.__table__.columns
        }
        card_dict["components"] = components
        card_dict["output_items"] = output_items

        return card_dict


async def publish_tech_card_operation(
    *,
    docs_sale_id: int,
    cashbox_id: int,
    organization_id: int,
    user_id: int,
    sold_items: List[Dict[str, Any]],  # [{nomenclature_id, quantity}]
) -> None:
    """
    Для каждого товара в продаже проверяет наличие тех карты
    и публикует сообщение в RabbitMQ если режим semi_auto или auto.
    """
    from api.docs_sales.messages.TechCardWarehouseOperationMessage import (
        TechCardComponentItem,
        TechCardOutputItem,
        TechCardWarehouseOperationMessage,
    )
    from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
    from common.utils.ioc.ioc import ioc

    for item in sold_items:
        nomenclature_id = item["nomenclature_id"]
        quantity = item["quantity"]

        card = await get_tech_card_for_nomenclature(nomenclature_id, cashbox_id)
        if not card:
            continue

        card_mode = card.get("card_mode", "reference")
        if card_mode not in ("semi_auto", "auto"):
            continue

        components_raw = card.get("components") or []
        output_items_raw = card.get("output_items") or []

        # Масштабируем количества компонентов пропорционально проданному
        scale = quantity  # если в тех карте 1 единица продукта = эти компоненты
        components = [
            TechCardComponentItem(
                message_id=uuid.uuid4(),
                nomenclature_id=c["nomenclature_id"],
                quantity=c["quantity"] * scale,
                name=c.get("name"),
            )
            for c in components_raw
            if isinstance(c, dict)
        ]
        output_items = [
            TechCardOutputItem(
                message_id=uuid.uuid4(),
                nomenclature_id=o["nomenclature_id"],
                quantity=o["quantity"] * scale,
            )
            for o in output_items_raw
            if isinstance(o, dict)
        ]

        msg = TechCardWarehouseOperationMessage(
            message_id=uuid.uuid4(),
            docs_sale_id=docs_sale_id,
            tech_card_id=str(card["id"]),
            cashbox_id=cashbox_id,
            organization_id=organization_id,
            user_id=user_id,
            card_mode=card_mode,
            warehouse_from_id=card["warehouse_from_id"],
            warehouse_to_id=card.get("warehouse_to_id"),
            components=components,
            output_items=output_items,
            sold_nomenclature_id=nomenclature_id,
            sold_quantity=quantity,
        )

        try:
            rabbit_factory: IRabbitFactory = ioc.get(IRabbitFactory)
            messaging = await rabbit_factory()
            await messaging.publish(
                message=msg,
                routing_key="teach_card_operation",
            )
            print("Опубликовано TechCardWarehouseOperationMessage")
        except Exception as exc:
            print(f"Ошибка {exc}")
