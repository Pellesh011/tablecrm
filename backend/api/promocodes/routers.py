from datetime import datetime, timezone
from typing import List, Optional

import api.promocodes.schemas as schemas
from api.deps import get_db, get_user_by_token
from api.promocodes.models import PromocodeDB, PromocodeType
from api.promocodes.services.promo_service import promocode_service
from database.db import (
    contragents,
    database,
    loyality_cards,
    loyality_transactions,
    organizations,
)
from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy import and_, select
from sqlalchemy.orm import Session
from ws_manager import manager

router = APIRouter(tags=["promocodes"])


@router.post("/promocodes/run/", response_model=schemas.PromoActivateResponse)
async def run_promocode(
    token: str,
    payload: schemas.PromoActivateRequest = Body(...),
    db: Session = Depends(get_db),
):
    """
    Активация промокода.
    """
    user = await get_user_by_token(token, db)
    now = datetime.now(tz=timezone.utc)

    code_str = payload.code.strip()

    # Поиск клиента в кассе кассира
    phone_search = payload.phone_number.strip()
    if not phone_search.startswith("+"):
        phone_search = f"%{phone_search}"

    contragent_query = select(contragents).where(
        contragents.c.phone.ilike(phone_search),
        contragents.c.cashbox == user.cashbox_id,
        contragents.c.is_deleted != True,
    )
    contragent = await database.fetch_one(contragent_query)

    if not contragent:
        raise HTTPException(
            status_code=404, detail="Клиент с таким номером не найден в этой кассе"
        )

    # Поиск карты лояльности
    card_query = select(loyality_cards).where(
        loyality_cards.c.contragent_id == contragent.id,
        loyality_cards.c.cashbox_id == user.cashbox_id,
        loyality_cards.c.is_deleted != True,
    )
    card = await database.fetch_one(card_query)

    if not card:
        raise HTTPException(
            status_code=404, detail="У клиента нет активной карты лояльности"
        )

    # Валидация промокода
    promo = db.query(PromocodeDB).filter(PromocodeDB.code == code_str).first()

    if not promo:
        raise HTTPException(status_code=404, detail="Промокод не найден")

    if promo.deleted_at is not None or not promo.is_active:
        raise HTTPException(status_code=400, detail="Промокод неактивен")

    # Проверка сроков действия
    if promo.valid_after and now < promo.valid_after:
        raise HTTPException(
            status_code=400, detail="Действие промокода еще не началось"
        )

    if promo.valid_until and promo.valid_until < now:
        raise HTTPException(status_code=400, detail="Срок действия промокода истек")

    # Проверка организации
    if card.organization_id and promo.organization_id != card.organization_id:
        raise HTTPException(
            status_code=400, detail="Промокод не подходит для организации карты"
        )

    # Проверка глобальных лимитов промокода
    if promo.type == PromocodeType.ONE_TIME and promo.current_usages > 0:
        raise HTTPException(status_code=400, detail="Промокод уже использован")

    if promo.max_usages and promo.current_usages >= promo.max_usages:
        raise HTTPException(status_code=400, detail="Лимит активаций исчерпан")

    async with database.transaction():
        # Проверка персонального лимита
        check_usage_query = select(loyality_transactions).where(
            and_(
                loyality_transactions.c.loyality_card_id == card.id,
                loyality_transactions.c.type == "promocode",
                loyality_transactions.c.external_id == str(promo.id),
            )
        )
        existing_usage = await database.fetch_one(check_usage_query)

        if existing_usage:
            raise HTTPException(
                status_code=400, detail="Этот клиент уже активировал данный промокод"
            )

        # Начисление
        transaction_id = await promocode_service.apply_promo_bonus(
            card_id=card.id,
            card_number=card.card_number,
            amount=promo.points_amount,
            promo_id=promo.id,
            cashbox_id=user.cashbox_id,
            user_id=user.id,
        )

        # Обновление счетчика использований
        promo.current_usages += 1
        db.flush()

    db.commit()
    db.refresh(promo)

    final_balance = (card.balance or 0) + promo.points_amount

    return {
        "success": True,
        "added_points": promo.points_amount,
        "new_balance": final_balance,
        "message": "Баллы успешно начислены",
        "transaction_id": transaction_id,
    }


@router.get("/promocodes/", response_model=List[schemas.GetPromoCodeNoRelation])
async def get_promocodes(
    token: str,
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None,
    is_active: Optional[bool] = None,
    sort: Optional[str] = "created_at:desc",
    db: Session = Depends(get_db),
):
    """Получение списка промокодов"""
    user = await get_user_by_token(token, db)

    # Проверяем доступ через organizations
    org_query = select(organizations.c.id).where(
        organizations.c.cashbox == user.cashbox_id
    )
    allowed_org_ids = [row.id for row in await database.fetch_all(org_query)]

    query = db.query(PromocodeDB).filter(
        PromocodeDB.organization_id.in_(allowed_org_ids),
        PromocodeDB.deleted_at.is_(None),
    )

    if search:
        query = query.filter(PromocodeDB.code.ilike(f"%{search}%"))

    if is_active is not None:
        query = query.filter(PromocodeDB.is_active == is_active)

    if sort:
        order_fields = {"created_at", "updated_at", "points_amount"}
        directions = {"asc", "desc"}

        if (
            len(sort.split(":")) != 2
            or sort.split(":")[1].lower() not in directions
            or sort.split(":")[0].lower() not in order_fields
        ):
            raise HTTPException(
                status_code=400, detail="Вы ввели некорректный параметр сортировки!"
            )
        order_by, direction = sort.split(":")
        column = getattr(PromocodeDB, order_by)
        if direction.lower() == "desc":
            column = column.desc()
        query = query.order_by(column)

    promocodes_list = query.offset(offset).limit(limit).all()

    result = []
    for promo in promocodes_list:
        promo_dict = schemas.GetPromoCodeNoRelation.from_orm(promo)
        result.append(promo_dict)

    return result


@router.get("/promocodes/{idx}/", response_model=schemas.GetPromoCodeNoRelation)
async def get_promocode_by_id(token: str, idx: int, db: Session = Depends(get_db)):
    """Получение одного промокода"""
    user = await get_user_by_token(token, db)

    # Проверяем доступ через organizations
    org_query = select(organizations.c.id).where(
        organizations.c.cashbox == user.cashbox_id,
        (organizations.c.is_deleted.is_(None)) | (organizations.c.is_deleted == False),
    )
    allowed_org_ids = [row.id for row in await database.fetch_all(org_query)]

    promo = (
        db.query(PromocodeDB)
        .filter(
            PromocodeDB.id == idx,
            PromocodeDB.organization_id.in_(allowed_org_ids),
            PromocodeDB.deleted_at.is_(None),
        )
        .first()
    )

    if not promo:
        raise HTTPException(
            status_code=404,
            detail="Промокод не найден или у вас нет прав на его просмотр",
        )

    return schemas.GetPromoCodeNoRelation.from_orm(promo)


@router.post("/promocodes/", response_model=schemas.GetPromoCodeNoRelation)
async def create_promocode(
    token: str,
    payload: schemas.PromoCodeCreate = Body(...),
    db: Session = Depends(get_db),
):
    """Создание промокода"""
    user = await get_user_by_token(token, db)

    # Проверка уникальности кода
    existing = (
        db.query(PromocodeDB)
        .filter(
            PromocodeDB.code == payload.code,
            PromocodeDB.deleted_at.is_(None),
        )
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Такой промокод уже существует")

    # Проверка организации
    org_query = select(organizations).where(
        organizations.c.id == payload.organization_id,
        organizations.c.cashbox == user.cashbox_id,
        (organizations.c.is_deleted.is_(None)) | (organizations.c.is_deleted == False),
    )
    if not await database.fetch_one(org_query):
        raise HTTPException(
            status_code=404, detail="Организация не найдена или недоступна"
        )

    # Проверка дистрибьютора
    if payload.distributor_id is not None:
        distr_query = select(contragents).where(
            contragents.c.id == payload.distributor_id,
            contragents.c.cashbox == user.cashbox_id,
            contragents.c.is_deleted != True,
        )
        if not await database.fetch_one(distr_query):
            raise HTTPException(status_code=404, detail="Дистрибьютор не найден")

    now = datetime.now(tz=timezone.utc)
    values = payload.dict()

    # Конвертация timestamp -> datetime для БД
    for field in ["valid_after", "valid_until"]:
        if values.get(field) and isinstance(values[field], int):
            values[field] = datetime.fromtimestamp(values[field], tz=timezone.utc)

    # Создание кода
    promo = PromocodeDB(
        **values,
        creator_id=user.id,
        current_usages=0,
        created_at=now,
        updated_at=now,
    )
    db.add(promo)
    db.commit()
    db.refresh(promo)

    promo_response = schemas.GetPromoCodeNoRelation.from_orm(promo)
    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "promocodes",
            "result": promo_response,
        },
    )

    return promo_response


@router.patch("/promocodes/{idx}/", response_model=schemas.GetPromoCodeNoRelation)
async def update_promocode(
    token: str,
    idx: int,
    payload: schemas.PromoCodeUpdate = Body(...),
    db: Session = Depends(get_db),
):
    """Обновление промокода"""
    user = await get_user_by_token(token, db)

    # Проверяем доступ
    org_query = select(organizations.c.id).where(
        organizations.c.cashbox == user.cashbox_id,
        (organizations.c.is_deleted.is_(None)) | (organizations.c.is_deleted == False),
    )
    allowed_org_ids = [row.id for row in await database.fetch_all(org_query)]

    promo = (
        db.query(PromocodeDB)
        .filter(
            PromocodeDB.id == idx,
            PromocodeDB.organization_id.in_(allowed_org_ids),
            PromocodeDB.deleted_at.is_(None),
        )
        .first()
    )

    if not promo:
        raise HTTPException(
            status_code=404,
            detail="Промокод не найден или у вас нет прав на его изменение",
        )

    values = payload.dict(exclude_unset=True)

    for field in ["valid_after", "valid_until"]:
        if values.get(field) and isinstance(values[field], int):
            values[field] = datetime.fromtimestamp(values[field], tz=timezone.utc)

    # Обновление атрибутов
    for field, value in values.items():
        setattr(promo, field, value)

    promo.updated_at = datetime.now(tz=timezone.utc)
    db.commit()
    db.refresh(promo)

    promo_response = schemas.GetPromoCodeNoRelation.from_orm(promo)
    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "promocodes",
            "result": promo_response,
        },
    )

    return promo_response


@router.delete("/promocodes/{idx}/", response_model=schemas.GetPromoCodeNoRelation)
async def delete_promocode(token: str, idx: int, db: Session = Depends(get_db)):
    """Удаление промокода (Soft Delete)"""
    user = await get_user_by_token(token, db)

    # Проверяем доступ
    org_query = select(organizations.c.id).where(
        organizations.c.cashbox == user.cashbox_id,
        (organizations.c.is_deleted.is_(None)) | (organizations.c.is_deleted == False),
    )
    allowed_org_ids = [row.id for row in await database.fetch_all(org_query)]

    promo = (
        db.query(PromocodeDB)
        .filter(
            PromocodeDB.id == idx,
            PromocodeDB.organization_id.in_(allowed_org_ids),
            PromocodeDB.deleted_at.is_(None),
        )
        .first()
    )

    if not promo:
        raise HTTPException(
            status_code=404,
            detail="Промокод не найден или у вас нет прав на его удаление",
        )

    now = datetime.now(tz=timezone.utc)
    promo.deleted_at = now
    promo.is_active = False
    promo.updated_at = now
    db.commit()
    db.refresh(promo)

    promo_response = schemas.GetPromoCodeNoRelation.from_orm(promo)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "promocodes",
            "result": promo_response,
        },
    )

    return promo_response
