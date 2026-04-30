import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from database.db import (
    database,
    docs_warehouse,
    docs_warehouse_goods,
    nomenclature,
    warehouse_register_movement,
)
from functions.warehouse_events import publish_balance_recalc_batch
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

OPERATION_WRITE_OFF = "outgoing"
OPERATION_PRODUCTION = "incoming"


async def _insert_warehouse_movement(
    organization_id: int,
    warehouse_id: int,
    nomenclature_id: int,
    amount: float,
    type_amount: str,
    cashbox_id: int,
    document_sale_id: Optional[int] = None,
    document_warehouse_id: Optional[int] = None,
    session: Optional[AsyncSession] = None,
    recalc_events: Optional[List[Dict[str, int]]] = None,
):
    print(
        f"_insert_warehouse_movement: org={organization_id}, wh={warehouse_id}, nom={nomenclature_id}, amount={amount}, type={type_amount}"
    )
    stmt = warehouse_register_movement.insert().values(
        type_amount=type_amount,
        organization_id=organization_id,
        warehouse_id=warehouse_id,
        nomenclature_id=nomenclature_id,
        document_sale_id=document_sale_id,
        document_warehouse_id=document_warehouse_id,
        amount=amount,
        cashbox_id=cashbox_id,
    )
    if session:
        await session.execute(stmt)
    else:
        await database.execute(stmt)
    event_payload = {
        "organization_id": organization_id,
        "warehouse_id": warehouse_id,
        "nomenclature_id": nomenclature_id,
        "cashbox_id": cashbox_id,
    }
    if session is not None:
        if recalc_events is not None:
            recalc_events.append(event_payload)
    else:
        await publish_balance_recalc_batch([event_payload])


async def reverse_tech_operation(
    operation_id: str,
    cashbox_id: int,
    organization_id: int,
    user_id: int,
) -> Dict[str, Any]:
    """
    Распроводит тех операцию:
    1. Создаёт обратные складские движения для документов списания и производства.
    2. Помечает оба docs_warehouse как is_deleted=True.
    3. Меняет статус TechOperation на 'reversed'.
    """
    from api.tech_operations.models import TechOperationDB
    from database.db import async_session_maker
    from sqlalchemy.future import select as sa_select

    async with async_session_maker() as session:
        result = await session.execute(
            sa_select(TechOperationDB).where(
                TechOperationDB.id == uuid.UUID(operation_id)
            )
        )
        op = result.scalar_one_or_none()
        if not op:
            raise ValueError(f"Тех операция {operation_id} не найдена")
        if op.status == "reversed":
            raise ValueError("Тех операция уже распроведена")
        if op.cashbox_id != cashbox_id:
            raise PermissionError("Нет доступа к тех операции")

        consumption_doc_id = op.consumption_doc_id
        production_doc_id = op.production_doc_id
        sale_write_off_doc_id = op.sale_write_off_doc_id
    # Откатить складские документы
    if consumption_doc_id:
        await _reverse_warehouse_doc(
            doc_id=consumption_doc_id,
            cashbox_id=cashbox_id,
            organization_id=organization_id,
            original_op="+",  # списание было "-", откатываем "+
        )
    if production_doc_id:
        await _reverse_warehouse_doc(
            doc_id=production_doc_id,
            cashbox_id=cashbox_id,
            organization_id=organization_id,
            original_op="-",  # производство было "+", откатываем "-"
        )

    # Обновить статус
    async with async_session_maker() as session:
        result = await session.execute(
            sa_select(TechOperationDB).where(
                TechOperationDB.id == uuid.UUID(operation_id)
            )
        )
        op = result.scalar_one()
        op.status = "reversed"
        op.updated_at = datetime.utcnow()
        await session.commit()

    return {
        "success": True,
        "operation_id": operation_id,
        "reversed_consumption_doc_id": consumption_doc_id,
        "reversed_production_doc_id": production_doc_id,
        "message": "Тех операция распроведена, складские движения откатаны",
    }


async def _reverse_warehouse_doc(
    doc_id: int,
    cashbox_id: int,
    organization_id: int,
    original_op: str,  # "+" если изначально был приход (откатываем "-"), иначе наоборот
):
    """
    Создаёт обратные движения в warehouse_register_movement и
    помечает docs_warehouse как is_deleted=True.
    """

    # Получить товары документа
    goods = await database.fetch_all(
        docs_warehouse_goods.select().where(
            docs_warehouse_goods.c.docs_warehouse_id == doc_id
        )
    )

    # Получить warehouse_id из документа
    doc = await database.fetch_one(
        docs_warehouse.select().where(docs_warehouse.c.id == doc_id)
    )
    if not doc:
        return

    warehouse_id = doc["warehouse"]

    for good in goods:
        nomenclature_id = good["nomenclature"]
        quantity = good["quantity"]
        delta = +quantity if original_op == "+" else -quantity

        await _update_warehouse_balance(
            organization_id=organization_id,
            warehouse_id=warehouse_id,
            nomenclature_id=nomenclature_id,
            delta=delta,
            cashbox_id=cashbox_id,
            document_warehouse_id=doc_id,
        )

    # Пометить документ удалённым
    await database.execute(
        docs_warehouse.update()
        .where(docs_warehouse.c.id == doc_id)
        .values(is_deleted=True)
    )


async def _update_warehouse_balance(
    organization_id: int,
    warehouse_id: int,
    nomenclature_id: int,
    delta: float,
    cashbox_id: int,
    document_warehouse_id: Optional[int] = None,
    document_sale_id: Optional[int] = None,
    session: Optional[AsyncSession] = None,
    recalc_events: Optional[List[Dict[str, int]]] = None,
):
    print(
        f"_update_warehouse_balance: org={organization_id}, wh={warehouse_id}, nom={nomenclature_id}, delta={delta}"
    )
    type_amount = "+" if delta >= 0 else "-"
    abs_delta = abs(delta)

    await _insert_warehouse_movement(
        organization_id=organization_id,
        warehouse_id=warehouse_id,
        nomenclature_id=nomenclature_id,
        amount=abs_delta,
        type_amount=type_amount,
        cashbox_id=cashbox_id,
        document_sale_id=document_sale_id,
        document_warehouse_id=document_warehouse_id,
        session=session,
        recalc_events=recalc_events,
    )


async def create_write_off_doc(
    *,
    cashbox_id: int,
    organization_id: int,
    warehouse_id: int,
    created_by: int,
    components: List[Dict[str, Any]],
    docs_sales_id: Optional[int] = None,
    tech_operation_id: Optional[str] = None,
    comment: Optional[str] = None,
    session: Optional[AsyncSession] = None,
    recalc_events: Optional[List[Dict[str, int]]] = None,
) -> int:
    """
    Создаёт docs_warehouse с operation='outgoing' (списание) и
    пишет движения в warehouse_register_movement.
    Возвращает id созданного документа.
    """
    print(
        f"create_write_off_doc: cashbox={cashbox_id}, org={organization_id}, wh={warehouse_id}, components={len(components)}"
    )

    # Получаем единицы измерения для всех компонентов
    comp_nomenclature_ids = {comp["nomenclature_id"] for comp in components}
    units_map = {}
    if comp_nomenclature_ids:
        stmt = select(nomenclature.c.id, nomenclature.c.unit).where(
            nomenclature.c.id.in_(comp_nomenclature_ids)
        )
        if session:
            rows = await session.execute(stmt)
            units_map = {row.id: row.unit for row in rows}
        else:
            rows = await database.fetch_all(stmt)
            units_map = {row["id"]: row["unit"] for row in rows}

    # Генерируем номер документа
    cnt_q = select(func.count(docs_warehouse.c.id)).where(
        docs_warehouse.c.cashbox == cashbox_id
    )
    if session:
        cnt = (await session.execute(cnt_q)).scalar() or 0
    else:
        cnt = await database.fetch_val(cnt_q) or 0
    doc_number = str(cnt + 1)
    print(f"create_write_off_doc: doc_number={doc_number}")

    # Вставляем заголовок документа
    stmt = docs_warehouse.insert().values(
        number=doc_number,
        dated=int(datetime.utcnow().timestamp()),
        operation=OPERATION_WRITE_OFF,
        status=True,
        cashbox=cashbox_id,
        organization=organization_id,
        warehouse=warehouse_id,
        docs_sales_id=docs_sales_id,
        created_by=created_by,
        comment=comment
        or (
            f"Списание сырья по тех. операции {tech_operation_id}"
            if tech_operation_id
            else "Расход"
        ),
        is_deleted=False,
    )
    if session:
        result = await session.execute(stmt)
        doc_id = result.inserted_primary_key[0]
    else:
        doc_id = await database.execute(stmt)
    print(f"create_write_off_doc: doc_id={doc_id}")

    # Вставляем строки товаров и вычисляем общую сумму
    total_sum = 0.0
    for comp in components:
        nomenclature_id = comp["nomenclature_id"]
        quantity = comp["quantity"]
        unit = units_map.get(nomenclature_id)
        # Цена для компонентов в техкарте пока не передаётся, ставим 0
        price = comp.get("price", 0.0)
        line_sum = price * quantity
        total_sum += line_sum
        print(
            f"  component: nom={nomenclature_id}, qty={quantity}, price={price}, unit={unit}"
        )

        goods_stmt = docs_warehouse_goods.insert().values(
            docs_warehouse_id=doc_id,
            nomenclature=nomenclature_id,
            price=price,
            quantity=quantity,
            unit=unit,
        )
        if session:
            await session.execute(goods_stmt)
        else:
            await database.execute(goods_stmt)

        await _update_warehouse_balance(
            organization_id=organization_id,
            warehouse_id=warehouse_id,
            nomenclature_id=nomenclature_id,
            delta=-quantity,
            cashbox_id=cashbox_id,
            document_warehouse_id=doc_id,
            document_sale_id=docs_sales_id,
            session=session,
            recalc_events=recalc_events,
        )

    # Обновляем сумму документа
    update_stmt = (
        docs_warehouse.update()
        .where(docs_warehouse.c.id == doc_id)
        .values(sum=total_sum)
    )
    if session:
        await session.execute(update_stmt)
    else:
        await database.execute(update_stmt)

    logger.info(
        "Создан документ списания doc_id=%s, warehouse=%s, components=%s, sum=%.2f",
        doc_id,
        warehouse_id,
        len(components),
        total_sum,
    )
    return doc_id


async def create_tech_operation(
    *,
    tech_card_id: str,
    cashbox_id: int,
    organization_id: int,
    user_id: int,
    from_warehouse_id: int,
    to_warehouse_id: int,
    components: List[Dict[str, Any]],
    output_items: List[Dict[str, Any]],
    output_quantity: float,
    nomenclature_id: Optional[int] = None,
    docs_sales_id: Optional[int] = None,
) -> Dict[str, Any]:
    from api.tech_operations.models import TechOperationComponentDB, TechOperationDB
    from database.db import async_session_maker

    now = datetime.utcnow()
    op_uuid = uuid.uuid4()
    recalc_events: List[Dict[str, int]] = []
    print(
        f"create_tech_operation: op_uuid={op_uuid}, tech_card={tech_card_id}, cashbox={cashbox_id}"
    )

    # Используем одну ORM-сессию с транзакцией
    async with async_session_maker() as session:
        async with session.begin():
            print("create_tech_operation: начата транзакция")
            # Создаём документ списания (используем сессию)
            consumption_doc_id = await create_write_off_doc(
                cashbox_id=cashbox_id,
                organization_id=organization_id,
                warehouse_id=from_warehouse_id,
                created_by=user_id,
                components=components,
                docs_sales_id=docs_sales_id,
                tech_operation_id=str(op_uuid),
                session=session,  # передаём сессию
                recalc_events=recalc_events,
            )
            # Создаём документ производства
            production_doc_id = await create_production_doc(
                cashbox_id=cashbox_id,
                organization_id=organization_id,
                warehouse_id=to_warehouse_id,
                created_by=user_id,
                output_items=output_items,
                docs_sales_id=docs_sales_id,
                tech_operation_id=str(op_uuid),
                session=session,
                recalc_events=recalc_events,
            )
            sale_write_off_doc_id = await create_write_off_doc(
                cashbox_id=cashbox_id,
                organization_id=organization_id,
                warehouse_id=to_warehouse_id,
                created_by=user_id,
                components=output_items,  # те же выходные изделия
                docs_sales_id=docs_sales_id,
                tech_operation_id=str(op_uuid),
                comment=f"Списание готового товара по тех. операции {op_uuid}",
                session=session,
            )
            op = TechOperationDB(
                id=op_uuid,
                tech_card_id=uuid.UUID(tech_card_id),
                output_quantity=output_quantity,
                from_warehouse_id=from_warehouse_id,
                to_warehouse_id=to_warehouse_id,
                user_id=user_id,
                cashbox_id=cashbox_id,
                nomenclature_id=nomenclature_id,
                docs_sales_id=docs_sales_id,
                production_doc_id=production_doc_id,
                consumption_doc_id=consumption_doc_id,
                status="active",
                production_order_id=uuid.uuid4(),
                consumption_order_id=uuid.uuid4(),
                created_at=now,
                updated_at=now,
            )
            op.sale_write_off_doc_id = sale_write_off_doc_id
            session.add(op)
            print(f"create_tech_operation: TechOperationDB добавлен, id={op.id}")
            for comp in components:
                comp_name = comp.get("name") or ""
                print(
                    f"  компонент: nom={comp['nomenclature_id']}, qty={comp['quantity']}, name='{comp_name}'"
                )
                op.components.append(
                    TechOperationComponentDB(
                        id=uuid.uuid4(),
                        operation_id=op_uuid,
                        nomeclature_id=comp["nomenclature_id"],
                        name=comp_name,
                        quantity=comp["quantity"],
                    )
                )
            # коммит произойдёт автоматически при выходе из async with session.begin()

        print("create_tech_operation: транзакция зафиксирована")

    logger.info(
        "Тех операция %s создана: consumption_doc=%s, production_doc=%s",
        op_uuid,
        consumption_doc_id,
        production_doc_id,
    )
    await publish_balance_recalc_batch(recalc_events)
    return {
        "tech_operation_id": str(op_uuid),
        "consumption_doc_id": consumption_doc_id,
        "production_doc_id": production_doc_id,
        "sale_write_off_doc_id": sale_write_off_doc_id,
    }


async def create_production_doc(
    *,
    cashbox_id: int,
    organization_id: int,
    warehouse_id: int,
    created_by: int,
    output_items: List[Dict[str, Any]],
    docs_sales_id: Optional[int] = None,
    tech_operation_id: Optional[str] = None,
    comment: Optional[str] = None,
    session: Optional[AsyncSession] = None,
    recalc_events: Optional[List[Dict[str, int]]] = None,
) -> int:
    """
    Создаёт docs_warehouse с operation='incoming' (производство) и
    обновляет складские остатки.
    Возвращает id созданного документа.
    """
    print(
        f"create_production_doc: cashbox={cashbox_id}, org={organization_id}, wh={warehouse_id}, items={len(output_items)}"
    )

    # Получаем единицы измерения для всех выходных изделий
    out_nomenclature_ids = {item["nomenclature_id"] for item in output_items}
    units_map = {}
    if out_nomenclature_ids:
        stmt = select(nomenclature.c.id, nomenclature.c.unit).where(
            nomenclature.c.id.in_(out_nomenclature_ids)
        )
        if session:
            rows = await session.execute(stmt)
            units_map = {row.id: row.unit for row in rows}
        else:
            rows = await database.fetch_all(stmt)
            units_map = {row["id"]: row["unit"] for row in rows}

    # Генерируем номер документа
    cnt_q = select(func.count(docs_warehouse.c.id)).where(
        docs_warehouse.c.cashbox == cashbox_id
    )
    if session:
        cnt = (await session.execute(cnt_q)).scalar() or 0
    else:
        cnt = await database.fetch_val(cnt_q) or 0
    doc_number = str(cnt + 1)
    print(f"create_production_doc: doc_number={doc_number}")

    # Вставляем заголовок документа
    stmt = docs_warehouse.insert().values(
        number=doc_number,
        dated=int(datetime.utcnow().timestamp()),
        operation=OPERATION_PRODUCTION,
        status=True,
        cashbox=cashbox_id,
        organization=organization_id,
        warehouse=warehouse_id,
        docs_sales_id=docs_sales_id,
        created_by=created_by,
        comment=comment
        or (
            f"Производство по тех. операции {tech_operation_id}"
            if tech_operation_id
            else "Приход"
        ),
        is_deleted=False,
    )
    if session:
        result = await session.execute(stmt)
        doc_id = result.inserted_primary_key[0]
    else:
        doc_id = await database.execute(stmt)
    print(f"create_production_doc: doc_id={doc_id}")

    # Вставляем строки товаров и вычисляем общую сумму
    total_sum = 0.0
    for item in output_items:
        nomenclature_id = item["nomenclature_id"]
        quantity = item["quantity"]
        unit = units_map.get(nomenclature_id)
        # Цена для выходных изделий пока не передаётся, ставим 0
        price = item.get("price", 0.0)
        line_sum = price * quantity
        total_sum += line_sum
        print(
            f"  item: nom={nomenclature_id}, qty={quantity}, price={price}, unit={unit}"
        )

        goods_stmt = docs_warehouse_goods.insert().values(
            docs_warehouse_id=doc_id,
            nomenclature=nomenclature_id,
            price=price,
            quantity=quantity,
            unit=unit,
        )
        if session:
            await session.execute(goods_stmt)
        else:
            await database.execute(goods_stmt)

        await _update_warehouse_balance(
            organization_id=organization_id,
            warehouse_id=warehouse_id,
            nomenclature_id=nomenclature_id,
            delta=+quantity,
            cashbox_id=cashbox_id,
            document_warehouse_id=doc_id,
            document_sale_id=docs_sales_id,
            session=session,
            recalc_events=recalc_events,
        )

    # Обновляем сумму документа
    update_stmt = (
        docs_warehouse.update()
        .where(docs_warehouse.c.id == doc_id)
        .values(sum=total_sum)
    )
    if session:
        await session.execute(update_stmt)
    else:
        await database.execute(update_stmt)

    logger.info(
        "Создан документ производства doc_id=%s, warehouse=%s, items=%s, sum=%.2f",
        doc_id,
        warehouse_id,
        len(output_items),
        total_sum,
    )
    return doc_id
