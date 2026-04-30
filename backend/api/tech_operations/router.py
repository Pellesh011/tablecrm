import uuid
from datetime import datetime
from typing import List, Optional

from api.tech_cards.models import TechCardDB
from api.tech_cards.utils import _tech_cards_cashbox_scope_query
from api.tech_operations.models import (
    TechOperationDB,
)
from api.tech_operations.schemas import (
    TechOperation,
    TechOperationCreate,
    TechOperationReverseResponse,
)
from api.tech_operations.utils import (
    _serialize_tech_operation,
    _tech_operations_cashbox_scope_query,
)
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..deps import get_db, get_user_by_token

router = APIRouter(prefix="/tech_operations", tags=["tech_operations"])


@router.post(
    "/",
    response_model=TechOperation,
    status_code=status.HTTP_201_CREATED,
    summary="Создать тех операцию вручную (только для режима auto)",
)
async def create_tech_operation(
    token: str,
    tech_operation: TechOperationCreate,
    db: Session = Depends(get_db),
):
    """
    Ручное создание тех операции.
    Автоматически создаёт:
    - docs_warehouse **Списание** (сырьё с warehouse_from)
    - docs_warehouse **Производство** (продукт на warehouse_to)
    И создаёт движения по складу.
    """
    user = await get_user_by_token(token, db)

    # Проверка тех карты
    scoped_cards = await _tech_cards_cashbox_scope_query(db, user.cashbox_id)
    db_tech_card = scoped_cards.filter(
        TechCardDB.id == tech_operation.tech_card_id
    ).first()
    if not db_tech_card:
        raise HTTPException(status_code=404, detail="Тех карта не найдена")
    if db_tech_card.card_mode not in ("auto", "automatic"):
        raise HTTPException(
            status_code=400,
            detail=f"Ручное создание операций доступно только для auto тех карт, "
            f"текущий режим: {db_tech_card.card_mode}",
        )

    # Получить organization_id из первой организации кассы
    from database.db import database, organizations

    org = await database.fetch_one(
        organizations.select()
        .where(
            organizations.c.cashbox == user.cashbox_id,
            organizations.c.is_deleted.is_not(True),
        )
        .limit(1)
    )
    if not org:
        raise HTTPException(status_code=400, detail="Организация не найдена")

    from api.tech_operations.services import create_tech_operation as svc_create

    result = await svc_create(
        tech_card_id=str(tech_operation.tech_card_id),
        cashbox_id=user.cashbox_id,
        organization_id=org["id"],
        user_id=user.id,
        from_warehouse_id=tech_operation.from_warehouse_id,
        to_warehouse_id=tech_operation.to_warehouse_id,
        components=[
            {
                "nomenclature_id": c.nomeclature_id,
                "quantity": c.quantity,
                "name": c.name,
                "gross_weight": c.gross_weight,
                "net_weight": c.net_weight,
            }
            for c in tech_operation.component_quantities
        ],
        output_items=[
            {
                "nomenclature_id": tech_operation.nomenclature_id,
                "quantity": tech_operation.output_quantity,
            }
        ],
        output_quantity=tech_operation.output_quantity,
        nomenclature_id=tech_operation.nomenclature_id,
        docs_sales_id=tech_operation.docs_sales_id,
    )

    # Вернуть созданную операцию
    op = (
        db.query(TechOperationDB)
        .filter(TechOperationDB.id == uuid.UUID(result["tech_operation_id"]))
        .first()
    )
    if not op:
        raise HTTPException(status_code=500, detail="Ошибка создания тех операции")

    return _serialize_tech_operation(op)


@router.post(
    "/bulk",
    response_model=List[TechOperation],
    status_code=status.HTTP_201_CREATED,
    summary="Создать несколько тех операций",
)
async def bulk_create_tech_operations(
    token: str,
    tech_operations: List[TechOperationCreate],
    db: Session = Depends(get_db),
):
    results = []
    for op in tech_operations:
        results.append(await create_tech_operation(token, op, db))
    return results


@router.get(
    "/",
    response_model=List[TechOperation],
    summary="Список тех операций",
)
async def get_tech_operations(
    token: str,
    tech_card_id: Optional[uuid.UUID] = Query(None),
    status: Optional[str] = Query(None, description="active|reversed|canceled"),
    docs_sales_id: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    query = await _tech_operations_cashbox_scope_query(db, user.cashbox_id)

    if tech_card_id:
        query = query.filter(TechOperationDB.tech_card_id == tech_card_id)
    if status:
        query = query.filter(TechOperationDB.status == status)
    if docs_sales_id:
        query = query.filter(TechOperationDB.docs_sales_id == docs_sales_id)

    operations = query.offset(offset).limit(limit).all()
    return [_serialize_tech_operation(op) for op in operations]


@router.get(
    "/{idx}",
    response_model=TechOperation,
    summary="Получить тех операцию по ID",
)
async def get_tech_operation(
    token: str,
    idx: uuid.UUID,
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    scoped = await _tech_operations_cashbox_scope_query(db, user.cashbox_id)
    op = scoped.filter(TechOperationDB.id == idx).first()
    if not op:
        raise HTTPException(status_code=404, detail="Тех операция не найдена")
    return _serialize_tech_operation(op)


@router.post(
    "/{idx}/reverse",
    response_model=TechOperationReverseResponse,
    summary="Распровести тех операцию",
)
async def reverse_tech_operation(
    token: str,
    idx: uuid.UUID,
    db: Session = Depends(get_db),
):
    """
    Распроводит тех операцию:
    1. Создаёт обратные складские движения для документов Списания и Производства.
    2. Помечает оба docs_warehouse как удалённые.
    3. Меняет статус операции на **reversed**.

    Нельзя распровести уже распроведённую операцию.
    """
    user = await get_user_by_token(token, db)
    scoped = await _tech_operations_cashbox_scope_query(db, user.cashbox_id)
    op = scoped.filter(TechOperationDB.id == idx).first()
    if not op:
        raise HTTPException(status_code=404, detail="Тех операция не найдена")
    if op.status == "reversed":
        raise HTTPException(status_code=400, detail="Тех операция уже распроведена")

    from database.db import database, organizations

    org = await database.fetch_one(
        organizations.select()
        .where(
            organizations.c.cashbox == user.cashbox_id,
            organizations.c.is_deleted.is_not(True),
        )
        .limit(1)
    )
    if not org:
        raise HTTPException(status_code=400, detail="Организация не найдена")

    from api.tech_operations.services import reverse_tech_operation as svc_reverse

    try:
        result = await svc_reverse(
            operation_id=str(idx),
            cashbox_id=user.cashbox_id,
            organization_id=org["id"],
            user_id=user.id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    # Обновить ORM объект
    db.expire(op)

    return TechOperationReverseResponse(**result)


@router.post(
    "/{idx}/cancel",
    response_model=TechOperation,
    summary="Отменить тех операцию (без отката складских документов)",
)
async def cancel_tech_operation(
    token: str,
    idx: uuid.UUID,
    db: Session = Depends(get_db),
):

    user = await get_user_by_token(token, db)
    scoped = await _tech_operations_cashbox_scope_query(db, user.cashbox_id)
    op = scoped.filter(TechOperationDB.id == idx).first()
    if not op:
        raise HTTPException(status_code=404, detail="Тех операция не найдена")
    if op.status != "active":
        raise HTTPException(status_code=400, detail="Операция не активна")

    op.status = "canceled"
    op.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(op)
    return _serialize_tech_operation(op)


@router.delete(
    "/{idx}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить тех операцию",
)
async def delete_tech_operation(
    token: str,
    idx: uuid.UUID,
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    scoped = await _tech_operations_cashbox_scope_query(db, user.cashbox_id)
    op = scoped.filter(TechOperationDB.id == idx).first()
    if not op:
        raise HTTPException(status_code=404, detail="Тех операция не найдена")
    op.status = "deleted"
    op.updated_at = datetime.utcnow()
    db.commit()
