import logging
from datetime import datetime

import api.pboxes.schemas as pboxes_schemas
import functions.filter_schemas as filter_schemas
from const import PaymentType
from database.db import (
    database,
    payments,
    pboxes,
    user_permissions,
    users_cboxes_relation,
)
from fastapi import APIRouter, Depends, HTTPException
from functions.helpers import (
    check_user_permission,
    get_filters_pboxes,
)
from sqlalchemy import and_, asc, desc, func, or_, select
from ws_manager import manager

logger = logging.getLogger(__name__)


router = APIRouter(tags=["pboxes"])


@router.get("/payboxes/", response_model=pboxes_schemas.GetPayments)
async def read_payboxes_meta(
    token: str,
    limit: int = 100,
    offset: int = 0,
    sort: str = "created_at:desc",
    filters: filter_schemas.PayboxesFiltersQuery = Depends(),
):
    """Получение счетов"""
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    has_permission = await check_user_permission(user.id, user.cashbox_id, "pboxes")
    if not has_permission:
        raise HTTPException(
            status_code=403, detail="Недостаточно прав для просмотра счетов"
        )

    filters = get_filters_pboxes(pboxes, filters)

    sort_list = sort.split(":")
    if sort_list[0] not in ["created_at", "updated_at"]:
        raise HTTPException(
            status_code=400, detail="Вы ввели некорректный параметр сортировки!"
        )

    base_query = (
        pboxes.select()
        .where(
            pboxes.c.cashbox == user.cashbox_id,
            pboxes.c.deleted_at.is_(None),
        )
        .filter(*filters)
    )

    if not user.is_owner:
        allowed_payboxes_query = user_permissions.select().where(
            and_(
                user_permissions.c.user_id == user.id,
                user_permissions.c.cashbox_id == user.cashbox_id,
                user_permissions.c.section == "pboxes",
                user_permissions.c.can_view.is_(True),
            )
        )
        allowed_payboxes = await database.fetch_all(allowed_payboxes_query)

        if allowed_payboxes:
            paybox_ids = []
            for perm in allowed_payboxes:
                if perm.paybox_id:
                    paybox_ids.append(perm.paybox_id)

            if paybox_ids:
                base_query = base_query.where(pboxes.c.id.in_(paybox_ids))

    if sort_list[1] == "desc":
        q = (
            base_query.order_by(desc(getattr(pboxes.c, sort_list[0])))
            .offset(offset)
            .limit(limit)
        )
    elif sort_list[1] == "asc":
        q = (
            base_query.order_by(asc(getattr(pboxes.c, sort_list[0])))
            .offset(offset)
            .limit(limit)
        )
    else:
        raise HTTPException(
            status_code=400, detail="Вы ввели некорректный параметр сортировки!"
        )

    pbox_records = await database.fetch_all(q)

    # Скрываем баланс для неадминов
    pbox_list = [dict(record) for record in pbox_records]
    # pbox_list = await hide_balance_for_non_admin(user, pbox_list)

    count_query = select(func.count()).select_from(base_query.subquery())
    count = await database.fetch_val(count_query)

    return {"result": pbox_list, "count": count}


@router.get("/payboxes/{id}/")
async def get_paybox_by_id(token: str, id: int):
    """Получение счета по ID"""
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    has_permission = await check_user_permission(user.id, user.cashbox_id, "pboxes", id)
    if not has_permission:
        raise HTTPException(
            status_code=403, detail="Недостаточно прав для просмотра этого счета"
        )

    query = pboxes.select().where(
        pboxes.c.id == id,
        pboxes.c.cashbox == user.cashbox_id,
        pboxes.c.deleted_at.is_(None),
    )
    pbox = await database.fetch_one(query)

    if not pbox:
        raise HTTPException(
            status_code=400,
            detail="Вы ввели несуществующий счет, либо он не принадлежит вам!",
        )

    # Скрываем баланс для неадминов
    pbox_dict = dict(pbox)
    # pbox_dict = await hide_balance_for_non_admin(user, pbox_dict)

    return pbox_dict


@router.post("/payboxes/", response_model=pboxes_schemas.Payboxes)
async def create_paybox(token: str, paybox_data: pboxes_schemas.PayboxesCreate):
    """Создание счета"""
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    # Проверяем права на создание счетов (только админ может создавать)
    # if not user.is_owner:
    #     raise HTTPException(status_code=403, detail="Только администратор может создавать счета")

    created_date = datetime.utcnow().date()
    created_date_ts = int(
        datetime.timestamp(datetime.combine(created_date, datetime.min.time()))
    )

    pbox_data_dict = paybox_data.dict()
    pbox_data_dict["balance"] = pbox_data_dict["start_balance"]
    pbox_data_dict["balance_date"] = created_date_ts
    pbox_data_dict["cashbox"] = user.cashbox_id
    pbox_data_dict["update_start_balance"] = int(datetime.utcnow().timestamp())
    pbox_data_dict["update_start_balance_date"] = int(datetime.utcnow().timestamp())
    pbox_data_dict["created_at"] = int(datetime.utcnow().timestamp())
    pbox_data_dict["updated_at"] = int(datetime.utcnow().timestamp())

    q = pboxes.insert().values(pbox_data_dict)
    new_pbox_id = await database.execute(q)

    q = pboxes.select().where(pboxes.c.id == new_pbox_id)
    pbox = await database.fetch_one(q)

    await manager.send_message(
        token, {"action": "create", "target": "payboxes", "result": dict(pbox)}
    )

    return pbox


@router.put("/payboxes/", response_model=pboxes_schemas.Payboxes)
async def update_paybox_data(token: str, pbox_data: pboxes_schemas.PayboxesEdit):
    """Обновление счета"""
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    has_permission = await check_user_permission(
        user.id, user.cashbox_id, "pboxes", pbox_data.id, True
    )
    if not has_permission:
        raise HTTPException(
            status_code=403, detail="Недостаточно прав для редактирования этого счета"
        )

    pbox_data_dict = pbox_data.dict()
    del pbox_data_dict["id"]

    new_pbox = {}

    q = pboxes.select().where(
        pboxes.c.id == pbox_data.id,
        pboxes.c.deleted_at.is_(None),
    )
    pbox = await database.fetch_one(q)

    if not pbox:
        raise HTTPException(status_code=400, detail="Счет не найден или был удален")

    query = payments.select().where(
        or_(payments.c.paybox == pbox_data.id, payments.c.paybox_to == pbox_data.id),
        payments.c.is_deleted == False,
        payments.c.cashbox == user.cashbox_id,
    )
    pbox_payments = await database.fetch_all(query)

    for i, j in pbox_data_dict.items():
        if j is not None:
            if i == "start_balance":
                all_payments_amount = 0
                for payment in pbox_payments:
                    today_ts = int(datetime.timestamp(datetime.today()))
                    date_ts = int(payment.date)
                    if pbox["balance_date"] <= date_ts <= today_ts:
                        if payment.status:
                            if payment.type == PaymentType.incoming:
                                all_payments_amount += float(payment.amount)
                            elif payment.type == PaymentType.transfer:
                                all_payments_amount += float(payment.amount)
                            else:
                                all_payments_amount -= float(payment.amount)

                new_pbox["balance"] = round(all_payments_amount + j, 2)
                new_pbox["update_start_balance"] = int(datetime.utcnow().timestamp())

            if i == "balance_date":
                all_payments_amount = 0
                for payment in pbox_payments:
                    today_ts = int(datetime.timestamp(datetime.today()))
                    date_ts = int(payment.date)
                    if j <= date_ts <= today_ts:
                        if payment.status:
                            if payment.type == PaymentType.incoming:
                                all_payments_amount += float(payment.amount)
                            elif payment.type == PaymentType.transfer:
                                all_payments_amount += float(payment.amount)
                            else:
                                all_payments_amount -= float(payment.amount)
                                if payment.paybox_to:
                                    pbox_to = await database.fetch_one(
                                        pboxes.select(
                                            pboxes.c.id == payment.paybox_to,
                                            pboxes.c.deleted_at.is_(None),
                                        )
                                    )
                                    if not pbox_to:
                                        logger.warning(
                                            f"Платеж {payment.id} ссылается на удаленный счет {payment.paybox_to}. "
                                            f"Баланс не обновлен."
                                        )
                                        continue
                                    await database.execute(
                                        pboxes.update(pboxes.c.id == pbox_to.id).values(
                                            {
                                                "balance": round(
                                                    float(pbox_to["balance"])
                                                    + payment.amount,
                                                    2,
                                                ),
                                                "update_start_balance_date": int(
                                                    datetime.utcnow().timestamp()
                                                ),
                                            }
                                        )
                                    )

                # print(f"j:{j} - date_ts:{date_ts} - today_ts:{today_ts}")
                # print(all_payments_amount)
                new_pbox["balance"] = round(
                    float(pbox["start_balance"]) + all_payments_amount, 2
                )
                new_pbox["update_start_balance_date"] = int(
                    datetime.utcnow().timestamp()
                )

            new_pbox[i] = j

    if new_pbox:
        new_pbox["updated_at"] = int(datetime.utcnow().timestamp())

        q = (
            pboxes.update()
            .where(
                pboxes.c.id == pbox_data.id,
                pboxes.c.cashbox == user.cashbox_id,
                pboxes.c.deleted_at.is_(None),
            )
            .values(new_pbox)
        )
        await database.execute(q)

        q = pboxes.select().where(
            pboxes.c.id == pbox_data.id,
            pboxes.c.cashbox == user.cashbox_id,
            pboxes.c.deleted_at.is_(None),
        )
        pbox = await database.fetch_one(q)

        await manager.send_message(
            token, {"action": "edit", "target": "payboxes", "result": dict(pbox)}
        )

        return pbox

    else:
        raise HTTPException(status_code=400, detail="Неверный запрос!")


@router.get("/payboxes_alt/", response_model=pboxes_schemas.GetPaymentsShort)
async def read_payboxes_short(
    token: str,
    limit: int = 100,
    offset: int = 0,
    sort: str = "created_at:desc",
    filters: filter_schemas.PayboxesFiltersQuery = Depends(),
):
    """Получение краткой информации по всем счетам"""
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    filters = get_filters_pboxes(pboxes, filters)

    sort_list = sort.split(":")
    if sort_list[0] not in ["created_at", "updated_at"]:
        raise HTTPException(
            status_code=400, detail="Вы ввели некорректный параметр сортировки!"
        )

    base_query = (
        select(
            pboxes.c.id,
            pboxes.c.external_id,
            pboxes.c.name,
            pboxes.c.created_at,
            pboxes.c.updated_at,
        )
        .where(
            pboxes.c.cashbox == user.cashbox_id,
            pboxes.c.deleted_at.is_(None),
        )
        .filter(*filters)
    )

    if sort_list[1] == "desc":
        q = (
            base_query.order_by(desc(getattr(pboxes.c, sort_list[0])))
            .offset(offset)
            .limit(limit)
        )
    elif sort_list[1] == "asc":
        q = (
            base_query.order_by(asc(getattr(pboxes.c, sort_list[0])))
            .offset(offset)
            .limit(limit)
        )
    else:
        raise HTTPException(
            status_code=400, detail="Вы ввели некорректный параметр сортировки!"
        )

    pbox_records = await database.fetch_all(q)

    pbox_list = [dict(record) for record in pbox_records]

    count_query = select(func.count()).select_from(base_query.subquery())
    count = await database.fetch_val(count_query)

    return {"result": pbox_list, "count": count}


@router.delete("/payboxes/{id}/")
async def delete_paybox(token: str, id: int):
    """Мягкое удаление счета"""
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    pbox = await database.fetch_one(
        pboxes.select().where(pboxes.c.id == id, pboxes.c.cashbox == user.cashbox_id)
    )

    if not pbox:
        raise HTTPException(status_code=404, detail="Счет не найден")

    if pbox.deleted_at:
        raise HTTPException(status_code=400, detail="Счет уже удален")

    await database.execute(
        pboxes.update().where(pboxes.c.id == id).values(deleted_at=datetime.now())
    )

    await manager.send_message(
        token, {"action": "delete", "target": "payboxes", "result": id}
    )

    return {"result": "success"}
