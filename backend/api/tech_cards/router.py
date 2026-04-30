import uuid
from typing import List, Optional

from api.tech_cards.models import TechCardDB, TechCardItemDB, TechCardOutputItemDB
from api.tech_cards.schemas import (
    TechCardCreate,
    TechCardMode,
    TechCardResponse,
    TechCardUpdate,
)
from api.tech_cards.utils import _tech_cards_cashbox_scope_query
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..deps import get_db, get_user_by_token

router = APIRouter(prefix="/tech_cards", tags=["tech_cards"])


@router.post(
    "/",
    response_model=TechCardResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Создать тех карту",
)
async def create_tech_card(
    token: str,
    tech_card: TechCardCreate,
    db: Session = Depends(get_db),
):
    """
    Создаёт тех карту одного из трёх режимов:
    - **reference** — справочная (только список компонентов)
    - **semi_auto** — полуавтомат (продажа + авто списание сырья)
    - **auto** — автомат (полная тех операция через RabbitMQ)
    """
    user = await get_user_by_token(token, db)

    card_data = tech_card.dict(exclude={"items", "output_items"})
    items_data = tech_card.items
    output_items_data = tech_card.output_items

    # Валидация: auto требует warehouse_to_id
    if tech_card.card_mode == TechCardMode.auto and not tech_card.warehouse_to_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="warehouse_to_id обязателен для режима 'auto'",
        )
    if tech_card.card_mode in (TechCardMode.semi_auto, TechCardMode.auto):
        if not tech_card.warehouse_from_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="warehouse_from_id обязателен для semi_auto / auto",
            )

    db_tech_card = TechCardDB(
        **card_data,
        id=uuid.uuid4(),
        user_id=user.id,
        cashbox_id=user.cashbox_id,
    )
    db.add(db_tech_card)

    for item in items_data:
        db.add(
            TechCardItemDB(
                id=uuid.uuid4(),
                tech_card_id=db_tech_card.id,
                **item.dict(),
            )
        )

    for out_item in output_items_data:
        db.add(
            TechCardOutputItemDB(
                id=uuid.uuid4(),
                tech_card_id=db_tech_card.id,
                **out_item.dict(),
            )
        )

    db.commit()
    db.refresh(db_tech_card)
    return db_tech_card


@router.get(
    "/",
    response_model=List[TechCardResponse],
    summary="Список тех карт",
)
async def get_tech_cards(
    token: str,
    card_mode: Optional[TechCardMode] = Query(None, description="Фильтр по режиму"),
    nomenclature_id: Optional[int] = Query(None, description="Фильтр по номенклатуре"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    query = await _tech_cards_cashbox_scope_query(db, user.cashbox_id)

    if card_mode:
        query = query.filter(TechCardDB.card_mode == card_mode)
    if nomenclature_id:
        query = query.filter(TechCardDB.parent_nomenclature_id == nomenclature_id)

    return query.offset(offset).limit(limit).all()


@router.get(
    "/{idx}",
    response_model=TechCardResponse,
    summary="Получить тех карту по ID",
)
async def get_tech_card(
    token: str,
    idx: uuid.UUID,
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    scoped = await _tech_cards_cashbox_scope_query(db, user.cashbox_id)
    card = scoped.filter(TechCardDB.id == idx).first()
    if not card:
        raise HTTPException(status_code=404, detail="Тех карта не найдена")
    return card


@router.put(
    "/{idx}",
    response_model=TechCardResponse,
    summary="Обновить тех карту",
)
async def update_tech_card(
    token: str,
    idx: uuid.UUID,
    card_data: TechCardUpdate,
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    scoped = await _tech_cards_cashbox_scope_query(db, user.cashbox_id)
    card = scoped.filter(TechCardDB.id == idx).first()
    if not card:
        raise HTTPException(status_code=404, detail="Тех карта не найдена")

    update_dict = card_data.dict(exclude_unset=True, exclude={"items", "output_items"})
    for field, value in update_dict.items():
        setattr(card, field, value)

    if card_data.items is not None:
        # Пересоздаём компоненты
        db.query(TechCardItemDB).filter(TechCardItemDB.tech_card_id == idx).delete()
        for item in card_data.items:
            db.add(TechCardItemDB(id=uuid.uuid4(), tech_card_id=idx, **item.dict()))

    if card_data.output_items is not None:
        db.query(TechCardOutputItemDB).filter(
            TechCardOutputItemDB.tech_card_id == idx
        ).delete()
        for out_item in card_data.output_items:
            db.add(
                TechCardOutputItemDB(
                    id=uuid.uuid4(), tech_card_id=idx, **out_item.dict()
                )
            )

    db.commit()
    db.refresh(card)
    return card


@router.delete(
    "/{idx}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Удалить тех карту (soft-delete)",
)
async def delete_tech_card(
    token: str,
    idx: uuid.UUID,
    db: Session = Depends(get_db),
):
    user = await get_user_by_token(token, db)
    scoped = await _tech_cards_cashbox_scope_query(db, user.cashbox_id)
    card = scoped.filter(TechCardDB.id == idx).first()
    if not card:
        raise HTTPException(status_code=404, detail="Тех карта не найдена")
    card.status = "deleted"
    db.commit()
