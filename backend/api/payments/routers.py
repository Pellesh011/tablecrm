import asyncio
from datetime import datetime
from typing import List

import api.payments.schemas as pay_schemas
import functions.filter_schemas as filter_schemas
import pytz
from api.articles.schemas import Article
from api.cheques.schemas import Cheque
from api.contragents.schemas import Contragent
from api.projects.schemas import Project
from const import PaymentType
from database.db import (
    articles,
    cheques,
    contragents,
    database,
    docs_purchases,
    docs_sales,
    entity_to_entity,
    payments,
    pboxes,
    projects,
    user_permissions,
    users,
    users_cboxes_relation,
)
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from functions.helpers import (
    build_sql_filters,
    check_user_permission,
    get_filters,
    hide_balance_for_non_admin,
)
from functions.users import _raschet_debounced
from producer import queue_notification
from sqlalchemy import and_, desc, distinct, func, select
from ws_manager import manager

router = APIRouter(tags=["payments"])


@router.get("/payments/", response_model=pay_schemas.GetPaymentsBasic)
async def read_payments_list(
    token: str,
    filters: filter_schemas.PaymentFiltersQuery = Depends(),
    cu_filters: filter_schemas.CUIntegerFilters = Depends(),
    offset: int = 0,
    limit: int = 100,
    sort: str = "created_at:desc",
    created_by: str = None,
    _with: str = None,
    detail_json: str = "",
):
    """Получение списка платежей (с фильтрами или без)"""
    token = token.strip()

    async def _get_pays_list(pays_db, cashbox_user):
        payments_list = []
        for pay in pays_db:
            pay_dict = dict(pay)

            # определяем может ли юзер изменять payment
            pay_dict["can_be_deleted_or_edited"] = await can_user_edit_payment(
                cashbox_user, pay_dict
            )
            pay_dict["repeat"] = {
                "repeat_parent_id": pay_dict["repeat_parent_id"],
                "repeat_period": pay_dict["repeat_period"],
                "repeat_weekday": pay_dict["repeat_weekday"],
                "repeat_day": pay_dict["repeat_day"],
                "repeat_month": pay_dict["repeat_month"],
                "repeat_first": pay_dict["repeat_first"],
                "repeat_last": pay_dict["repeat_last"],
                "repeat_seconds": pay_dict["repeat_seconds"],
                "repeat_number": pay_dict["repeat_number"],
            }

            # Добавляем информацию о счете откуда
            if pay_dict["paybox"]:
                paybox_query = pboxes.select().where(
                    pboxes.c.id == pay_dict["paybox"],
                    pboxes.c.deleted_at.is_(None),
                )
                paybox_info = await database.fetch_one(paybox_query)
                if paybox_info:
                    has_permission = await check_user_permission(
                        user.id, user.cashbox_id, "pboxes", paybox_info.id
                    )

                    if has_permission:
                        paybox_info_dict = dict(paybox_info)
                        paybox_info_dict = await hide_balance_for_non_admin(
                            user, paybox_info_dict
                        )

                        pay_dict["source_account_name"] = paybox_info_dict["name"]
                        pay_dict["source_account_id"] = paybox_info_dict["id"]

            if _with:
                user_created_by_query = users.select(users.c.id == pay.account)
                user_created_by = await database.fetch_one(user_created_by_query)

                created_by_user = {
                    "user_id": user_created_by.id,
                    "user_name": f"{user_created_by.first_name or ''} {user_created_by.last_name or ''}",
                }
                pay_dict.update({"created_by": created_by_user})

            for key in ["cashbox", "account", "is_deleted"]:
                try:
                    del pay_dict[key]
                except KeyError:
                    pass

            for detail_num in set(detail_json.split(",")):
                if not detail_num:
                    continue
                try:
                    int(detail_num)
                except ValueError:
                    raise HTTPException(
                        400,
                        "detail_json parameter should be integers delimited by comma",
                    )
                if detail_num == "1" and pay_dict["project_id"]:
                    query = projects.select().where(
                        projects.c.id == pay_dict["project_id"],
                        projects.c.cashbox == user.cashbox_id,
                    )
                    project_db = await database.fetch_one(query)
                    if project_db:
                        pay_dict["project_info"] = Project(**dict(project_db)).dict()
                elif detail_num == "2" and pay_dict["article_id"]:
                    query = articles.select().where(
                        articles.c.id == pay_dict["article_id"],
                        articles.c.cashbox == user.cashbox_id,
                    )
                    article_db = await database.fetch_one(query)
                    if article_db:
                        pay_dict["article_info"] = Article(**dict(article_db)).dict()
                elif detail_num == "3" and pay_dict["contragent"]:
                    query = contragents.select().where(
                        contragents.c.id == pay_dict["contragent"],
                        contragents.c.cashbox == user.cashbox_id,
                    )
                    contragent_db = await database.fetch_one(query)
                    if contragent_db:
                        pay_dict["contragent_info"] = Contragent(
                            **dict(contragent_db)
                        ).dict()
                elif detail_num == "4":
                    pay_dict["user_info"] = dict(user)
                elif detail_num == "5" and pay_dict["cheque"]:
                    query = cheques.select().where(
                        cheques.c.id == pay_dict["cheque"],
                        cheques.c.cashbox == user.cashbox_id,
                    )
                    cheque_db = await database.fetch_one(query)
                    if cheque_db:
                        pay_dict["cheque_info"] = Cheque(**dict(cheque_db)).dict()

            payments_list.append(pay_dict)
        return payments_list

    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    has_permission = await check_user_permission(user.id, user.cashbox_id, "payments")
    if not has_permission:
        raise HTTPException(
            status_code=403, detail="Недостаточно прав для просмотра платежей"
        )

    filters = get_filters(payments, filters, cashbox_id=user.cashbox_id)

    # Дополнительный фильтр для source_account
    if filters and hasattr(filters, "source_account") and filters.source_account:
        source_account_ids = filters.source_account.split(",")
        source_filter = (
            " AND (payments.paybox IN ("
            + ",".join(source_account_ids)
            + ") OR payments.paybox_to IN ("
            + ",".join(source_account_ids)
            + "))"
        )
        filters += source_filter

    if not user.is_owner:
        allowed_payboxes_query = user_permissions.select().where(
            and_(
                user_permissions.c.user_id == user.id,
                user_permissions.c.cashbox_id == user.cashbox_id,
                user_permissions.c.section == "payments",
                user_permissions.c.can_view.is_(True),
            )
        )
        allowed_payboxes = await database.fetch_all(allowed_payboxes_query)

        if allowed_payboxes:
            paybox_ids = []
            for perm in allowed_payboxes:
                if perm.paybox_id:
                    paybox_ids.append(str(perm.paybox_id))

            if paybox_ids:
                paybox_filter = (
                    " AND (payments.paybox IN ("
                    + ",".join(paybox_ids)
                    + ") OR payments.paybox_to IN ("
                    + ",".join(paybox_ids)
                    + "))"
                )
                filters += paybox_filter

    sort_list = sort.split(":")
    if not (
        sort_list[1] in ["desc", "asc"]
        and sort_list[0] in ["created_at", "updated_at", "date"]
    ):
        raise HTTPException(
            status_code=400, detail="Вы ввели некорректный параметр сортировки!"
        )
    order_by_type = sort_list[1].upper()

    cu_filters = build_sql_filters(cu_filters)

    if created_by:
        created_by_param = created_by.split(",")
        total_pay_list = []
        for cb_id in set(created_by_param):
            try:
                int(cb_id)
            except ValueError:
                error_msg = "created_by parameter should be integers delimited by comma"
                return {"result": None, "count": 0, "errors": {"message": error_msg}}
            created_by_user_query = users_cboxes_relation.select(
                users_cboxes_relation.c.user == int(cb_id)
            )
            created_by_user = await database.fetch_one(created_by_user_query)
            if not created_by_user:
                not_found = f"Пользователь с id={cb_id} не найден"
                return {"result": None, "count": 0, "errors": {"message": not_found}}
            query = f"""
                SELECT *,
                (CASE WHEN payments.contragent IS NULL THEN NULL ELSE (SELECT contragents.name FROM contragents
                WHERE contragents.id = payments.contragent) END) AS contragent_name
                FROM payments WHERE payments.cashbox = {created_by_user.cashbox_id} AND payments.is_deleted = false AND
                payments.parent_id IS NULL {filters} {cu_filters} ORDER BY payments.{sort_list[0]} {order_by_type} LIMIT {limit} OFFSET {offset};"""
            pays = await database.fetch_all(query)

            pays_list = await _get_pays_list(pays, user)

            total_pay_list.extend(pays_list)

        c = f"SELECT count(*) FROM payments WHERE payments.cashbox = {created_by_user.cashbox_id} AND payments.is_deleted = false AND payments.parent_id IS NULL {filters} {cu_filters}"
        count = await database.fetch_one(c)

        return {"result": total_pay_list, "count": dict(count)["count"]}  # LOCAL

        # return {"result": pays_list, "count": count.count} # PROD

    else:
        query = f"""
            SELECT *,
            (CASE WHEN payments.contragent IS NULL THEN NULL ELSE (SELECT contragents.name FROM contragents
            WHERE contragents.id = payments.contragent) END) AS contragent_name
            FROM payments
            WHERE
                payments.cashbox = {user.cashbox_id}
                AND payments.is_deleted = false
                AND payments.parent_id IS NULL
                {filters}
                {cu_filters}
            ORDER BY
                payments.{sort_list[0]} {order_by_type},
                payments.id { 'ASC' if order_by_type == 'ASC' else 'DESC' }
            LIMIT {limit} OFFSET {offset};
        """
        pays = await database.fetch_all(query)

        pays_list = await _get_pays_list(pays, user)

        c = f"SELECT count(*) FROM payments WHERE payments.cashbox = {user.cashbox_id} AND payments.is_deleted = false AND payments.parent_id IS NULL {filters} {cu_filters}"
        count = await database.fetch_one(c)

        return {"result": pays_list, "count": dict(count)["count"]}  # LOCAL

        # return {"result": pays_list, "count": count.count} # PROD


@router.post("/payments/")
async def create_payment(token: str, payment: pay_schemas.PaymentCreate):
    """Создание платежа"""
    token = token.strip()

    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    has_permission = await check_user_permission(
        user.id, user.cashbox_id, "payments", payment.paybox, True
    )
    if not has_permission:
        raise HTTPException(
            status_code=403, detail="Недостаточно прав для создания платежей"
        )

    payment_dict = payment.dict()

    if not payment_dict.get("date"):
        payment_dict["date"] = int(
            datetime.now()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
        )

    if payment_dict.get("preamount") and payment_dict.get("percentamount"):
        amount = payment.preamount * payment.percentamount
        amount_without_tax = amount

        payment_dict["amount"] = amount
        payment_dict["amount_without_tax"] = amount_without_tax

        del payment_dict["preamount"]
        del payment_dict["percentamount"]
    else:
        del payment_dict["preamount"]
        del payment_dict["percentamount"]

    if payment_dict.get("repeat"):
        payment_dict.update(payment_dict["repeat"])
    del payment_dict["repeat"]

    paybox_q = pboxes.select().where(
        pboxes.c.id == payment.paybox,
        pboxes.c.cashbox == user.cashbox_id,
        pboxes.c.deleted_at.is_(None),
    )
    paybox = await database.fetch_one(paybox_q)

    if not paybox:
        raise HTTPException(
            status_code=403,
            detail="Введенный счет не принадлежит вам или не существует!",
        )
    paybox_dict = dict(paybox)
    project_dict = None

    if payment.project_id:
        project_q = projects.select().where(
            projects.c.id == payment.project_id,
            projects.c.cashbox == user.cashbox_id,
        )
        project = await database.fetch_one(project_q)
        project_dict = dict(project)
        payment_dict["project_id"] = project.id
    else:
        payment_dict["project_id"] = None

    if payment.contragent == 0:
        payment_dict["contragent"] = None
    else:
        payment_dict["contragent"] = payment.contragent

    if payment.article_id:
        article_query = articles.select().where(
            articles.c.cashbox == user.cashbox_id,
            articles.c.id == payment.article_id,
        )
        article_name = await database.fetch_one(article_query)

        if not article_name:
            raise HTTPException(
                status_code=403,
                detail="Выбранная статья не принадлежит вам или не существует!",
            )

        payment_dict["article"] = article_name[1]

    if payment.cheque:
        cheque_query = cheques.select().where(
            cheques.c.cashbox == user.cashbox_id, cheques.c.id == payment.cheque
        )
        cheque = await database.fetch_one(cheque_query)

        if not cheque:
            raise HTTPException(
                status_code=403,
                detail="Выбранный чек не принадлежит вам или не существует!",
            )

    payment_dict["cashbox"] = user.cashbox_id
    payment_dict["account"] = user.user
    payment_dict["paybox"] = paybox.id
    payment_dict["is_deleted"] = False
    payment_dict["raspilen"] = False
    payment_dict["created_at"] = int(datetime.utcnow().timestamp())
    payment_dict["updated_at"] = int(datetime.utcnow().timestamp())

    paybox_to_id = None

    if payment.type == PaymentType.transfer:
        if payment.status:
            paybox_to_id = payment.paybox_to
            paybox_dict["balance"] = round(
                float(paybox_dict["balance"]) - float(payment_dict["amount"]), 2
            )
            query = pboxes.select().where(
                pboxes.c.id == paybox_to_id,
                pboxes.c.cashbox == user.cashbox_id,
                pboxes.c.deleted_at.is_(None),
            )
            pbox_to = await database.fetch_one(query)

            if not pbox_to:
                raise HTTPException(
                    status_code=403,
                    detail="Введенный счет не принадлежит вам или не существует!",
                )

            pbox_to_dict = dict(pbox_to)
            pbox_to_dict["balance"] = round(
                float(pbox_to_dict["balance"]) + float(payment_dict["amount"]), 2
            )
            paybox_q = (
                pboxes.update()
                .where(
                    pboxes.c.id == paybox_to_id,
                    pboxes.c.cashbox == user.cashbox_id,
                )
                .values(pbox_to_dict)
            )
            await database.execute(paybox_q)

    query = payments.insert(values=payment_dict)
    pay_id = await database.execute(query)

    if payment.project_id:
        if payment.type == PaymentType.incoming:
            if payment.status:
                project_dict["incoming"] += payment_dict["amount_without_tax"]
        if payment.type == PaymentType.outgoing:
            if payment.status:
                project_dict["outgoing"] += payment_dict["amount_without_tax"]
        project_q = (
            projects.update()
            .where(
                projects.c.id == payment.project_id,
                projects.c.cashbox == user.cashbox_id,
            )
            .values(project_dict)
        )
        await database.execute(project_q)

        query = projects.select().where(
            projects.c.id == payment.project_id,
            projects.c.cashbox == user.cashbox_id,
        )
        project = await database.fetch_one(query)

        if not project:
            raise HTTPException(
                status_code=403,
                detail="Введенный проект не принадлежит вам или не существует!",
            )

        in_sum = float(project.incoming)
        out_sum = float(project.outgoing)

        if out_sum == 0 and in_sum != 0:
            project_dict["profitability"] = 100
        elif out_sum == 0 and in_sum == 0:
            project_dict["profitability"] = 0
        else:
            project_dict["profitability"] = round(
                ((in_sum - out_sum) / out_sum) * 100, 2
            )

        project_dict["updated_at"] = int(datetime.utcnow().timestamp())

        query = (
            projects.update()
            .where(
                projects.c.id == payment.project_id,
                projects.c.cashbox == user.cashbox_id,
            )
            .values(project_dict)
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {"action": "edit", "target": "projects", "result": project_dict},
        )

    payment_dict["id"] = pay_id

    # date_ts = int(datetime.timestamp(datetime.combine(payment.date, datetime.min.time())))

    if paybox_dict["balance_date"] <= payment_dict["date"]:
        if payment.status:
            if payment.type == PaymentType.incoming:
                paybox_dict["balance"] = round(
                    float(paybox_dict["balance"]) + float(payment_dict["amount"]), 2
                )
            elif payment.type == PaymentType.outgoing:
                paybox_dict["balance"] = round(
                    float(paybox_dict["balance"]) - float(payment_dict["amount"]), 2
                )

    await manager.send_message(
        token, {"action": "edit", "target": "payboxes", "result": paybox_dict}
    )

    del paybox_dict["id"]

    paybox_q = (
        pboxes.update()
        .where(pboxes.c.id == payment.paybox, pboxes.c.cashbox == user.cashbox_id)
        .values(paybox_dict)
    )
    await database.execute(paybox_q)

    query = f"""
    SELECT payments.id, payments.type, payments.name, payments.external_id, payments.article,
    payments.article_id, payments.project_id, payments.tags, payments.amount,
    payments.amount_without_tax, payments.description, payments.date, payments.repeat_freq,
    payments.repeat_parent_id, payments.repeat_period, payments.repeat_first, payments.repeat_last,
    payments.repeat_number, payments.repeat_day, payments.repeat_month, payments.repeat_weekday,
    payments.repeat_seconds, payments.parent_id, payments.stopped, payments.status, payments.tax,
    payments.tax_type, payments.deb_cred, payments.raspilen, payments.contragent, payments.cashbox,
    payments.paybox, payments.paybox_to, payments.account, payments.is_deleted, payments.cheque,
    payments.docs_sales_id, payments.created_at, payments.updated_at,
    (CASE WHEN payments.contragent IS NULL THEN NULL ELSE (SELECT contragents.name FROM contragents
     WHERE contragents.id = payments.contragent) END) AS contragent_name
    FROM payments WHERE payments.cashbox = {user.cashbox_id} AND payments.id = {pay_id}
    """
    pay_dict = await database.fetch_one(query)

    await manager.send_message(
        token,
        {"action": "create", "target": "payments", "result": dict(pay_dict)},
    )

    return {**payment_dict, **{"data": {"status": "success"}}}


@router.get("/payment/{id}/", response_model=pay_schemas.PaymentInList)
async def read_payment(token: str, id: int):
    """Просмотр платежа по ID"""
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = (
                payments.select()
                .filter(payments.c.cashbox == user.cashbox_id, payments.c.id == id)
                .order_by(desc(payments.c.id))
            )
            pay = await database.fetch_one(query)

            if pay:
                pay_dict = dict(pay)
                pay_dict["repeat"] = {
                    "repeat_parent_id": pay_dict["repeat_parent_id"],
                    "repeat_period": pay_dict["repeat_period"],
                    "repeat_weekday": pay_dict["repeat_weekday"],
                    "repeat_day": pay_dict["repeat_day"],
                    "repeat_month": pay_dict["repeat_month"],
                    "repeat_first": pay_dict["repeat_first"],
                    "repeat_last": pay_dict["repeat_last"],
                    "repeat_seconds": pay_dict["repeat_seconds"],
                    "repeat_number": pay_dict["repeat_number"],
                }

                # creator of payment
                created_by_query = users.select(users.c.id == user.user)
                created_by_obj = await database.fetch_one(created_by_query)
                created_by_user = {
                    "user_id": created_by_obj.id,
                    "user_name": f"{created_by_obj.first_name or ''} {created_by_obj.last_name or ''}",
                }
                pay_dict.update({"created_by": created_by_user})
                return pay_dict
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Платеж не существует или не принадлежит вам!",
                )

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


async def can_user_edit_payment(cashbox_user, payment) -> bool:
    # return (cashbox_user.payment_past_edit_days is None) or (datetime.utcnow().timestamp() - payment.get("created_at") <= timedelta(days=cashbox_user.payment_past_edit_days).total_seconds())
    return (cashbox_user.payment_past_edit_days is None) or (
        datetime.now(pytz.timezone(cashbox_user.timezone)).date()
        - datetime.fromtimestamp(payment.get("created_at")).date()
    ).days < cashbox_user.payment_past_edit_days


@router.put("/payments/{id}/", response_model=pay_schemas.PaymentInListWithData)
async def update_payment(
    token: str, id: int, payment: pay_schemas.PaymentEdit, bg_tasks: BackgroundTasks
):
    """Обновление данных платежа по ID

    Repeats:

        repeat_first, repeat last: unix time in seconds
        repeat_period: yearly, monthly, weekly, daily, hourly, seconds

        Required fields:
            yearly: repeat_month, repeat_day
            monthly: repeat_day
            weekly: repeat_weekday (specify with comma numbers of weekdays, eg: monday, tuesday and sunday "0,1,6")
            seconds: repeat_seconds

        to stop repeats set repeat_period to null
    """
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if user:
        if user.status:
            payment_q = payments.select().where(
                payments.c.id == id, payments.c.cashbox == user.cashbox_id
            )
            payment_db = await database.fetch_one(payment_q)

            if not payment_db:
                raise HTTPException(
                    status_code=404, detail="Выбранный платеж не найден"
                )

            if not (await can_user_edit_payment(user, payment_db)):
                raise HTTPException(
                    status_code=403,
                    detail="Вы не можете изменять настолько старый платеж",
                )

            update_dict: dict = payment.dict(exclude_unset=True)
            if update_dict.get("repeat"):
                update_dict.update(update_dict["repeat"])
                del update_dict["repeat"]

            q = (
                payments.update()
                .where(payments.c.id == id, payments.c.cashbox == user.cashbox_id)
                .values({"updated_at": int(datetime.utcnow().timestamp())})
            )
            await database.execute(q)

            await manager.send_message(
                token,
                {"action": "edit", "target": "payments", "result": dict(payment_db)},
            )

            if update_dict:

                if "paybox" in update_dict and update_dict["paybox"]:
                    q = pboxes.select().where(
                        pboxes.c.cashbox == user.cashbox_id,
                        pboxes.c.id == update_dict["paybox"],
                        pboxes.c.deleted_at.is_(None),
                    )
                    res = await database.fetch_one(q)

                    if not res:
                        raise HTTPException(
                            status_code=403,
                            detail="Введенный счет не принадлежит вам или не существует!",
                        )

                if "paybox_to" in update_dict and update_dict["paybox_to"]:
                    q = pboxes.select().where(
                        pboxes.c.cashbox == user.cashbox_id,
                        pboxes.c.id == update_dict["paybox_to"],
                        pboxes.c.deleted_at.is_(None),
                    )
                    res = await database.fetch_one(q)

                    if not res:
                        raise HTTPException(
                            status_code=403,
                            detail="Введенный счет не принадлежит вам или не существует!",
                        )

                if "project_id" in update_dict and update_dict["project_id"]:
                    q = projects.select().where(
                        projects.c.cashbox == user.cashbox_id,
                        projects.c.id == update_dict["project_id"],
                    )
                    res = await database.fetch_one(q)

                    if not res:
                        raise HTTPException(
                            status_code=403,
                            detail="Введенный проект не принадлежит вам или не существует!",
                        )

                update_dict["updated_at"] = int(datetime.utcnow().timestamp())
                if "project_id" in update_dict and update_dict["project_id"] == 0:
                    update_dict["project_id"] = None

                if "contragent" in update_dict and update_dict["contragent"] == 0:
                    update_dict["contragent"] = None

                if payment.article_id:
                    article_query = articles.select().where(
                        articles.c.cashbox == user.cashbox_id,
                        articles.c.id == payment.article_id,
                    )
                    article_name = await database.fetch_one(article_query)

                    if not article_name:
                        raise HTTPException(
                            status_code=403,
                            detail="Выбранная статья не принадлежит вам или не существует!",
                        )

                    update_dict["article"] = article_name[1]

                if update_dict.get("cheque"):
                    cheque_query = cheques.select().where(
                        cheques.c.cashbox == user.cashbox_id,
                        cheques.c.id == payment.cheque,
                    )
                    cheque = await database.fetch_one(cheque_query)

                    if not cheque:
                        raise HTTPException(
                            status_code=403,
                            detail="Выбранный чек не принадлежит вам или не существует!",
                        )

                q = (
                    payments.update()
                    .where(payments.c.id == id, payments.c.cashbox == user.cashbox_id)
                    .values(update_dict)
                )
                await database.execute(q)

                payment_q = f"""SELECT payments.id, payments.type, payments.name, payments.external_id, payments.article, payments.article_id, payments.project_id, payments.tags, payments.amount, payments.amount_without_tax, payments.description, payments.date, payments.repeat_freq, payments.repeat_parent_id, payments.repeat_period, payments.repeat_first, payments.repeat_last, payments.repeat_number, payments.repeat_day, payments.repeat_month, payments.repeat_weekday, payments.repeat_seconds, payments.parent_id, payments.stopped, payments.status, payments.tax, payments.tax_type, payments.deb_cred, payments.raspilen, payments.contragent, payments.cashbox, payments.paybox, payments.paybox_to, payments.account, payments.is_deleted, payments.cheque, payments.docs_sales_id, payments.created_at, payments.updated_at,
                (CASE WHEN payments.contragent IS NULL THEN NULL ELSE (SELECT contragents.name FROM contragents WHERE contragents.id = payments.contragent) END) AS contragent_name
                FROM payments WHERE payments.cashbox = {user.cashbox_id} AND payments.id = {id}"""
                upd_payment_db = await database.fetch_one(payment_q)

                if "status" in update_dict and payment_db.raspilen:
                    q = (
                        payments.update()
                        .where(
                            payments.c.parent_id == id,
                            payments.c.cashbox == user.cashbox_id,
                        )
                        .values({"status": update_dict["status"]})
                    )
                    await database.execute(q)

                if "project_id" in update_dict and payment_db.raspilen:
                    q = (
                        payments.update()
                        .where(
                            payments.c.parent_id == id,
                            payments.c.cashbox == user.cashbox_id,
                        )
                        .values({"project_id": None})
                    )
                    await database.execute(q)

                if update_dict.keys() & {
                    "type",
                    "amount",
                    "amount_without_tax",
                    "project_id",
                    "paybox",
                    "paybox_to",
                    "status",
                    "is_deleted",
                    "date",
                }:
                    asyncio.create_task(_raschet_debounced(user, token))
                await manager.send_message(
                    token,
                    {
                        "action": "edit",
                        "target": "payments",
                        "result": dict(upd_payment_db),
                    },
                )
                return {**upd_payment_db, **{"data": {"status": "success"}}}

    else:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.delete("/payments/{payment_id}/")
async def delete_payment(token: str, payment_id: int):
    """Удаление платежа по ID"""
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            q = payments.select().where(payments.c.id == payment_id)
            payment = await database.fetch_one(q)

            if not payment:
                raise HTTPException(
                    status_code=404, detail="Выбранный платеж не найден"
                )

            if not (await can_user_edit_payment(user, payment)):
                raise HTTPException(
                    status_code=403,
                    detail="Вы не можете изменять настолько старый платеж",
                )

            if payment.cashbox == user.cashbox_id and payment.account == user.user:

                paybox_q = pboxes.select().where(
                    pboxes.c.id == payment.paybox,
                    pboxes.c.deleted_at.is_(None),
                )
                paybox = await database.fetch_one(paybox_q)

                paybox_dict = dict(paybox)

                if payment.status:
                    if payment.type == PaymentType.incoming:
                        paybox_dict["balance"] = float(paybox_dict["balance"]) - float(
                            payment.amount
                        )
                    elif payment.type == PaymentType.outgoing:
                        paybox_dict["balance"] = float(paybox_dict["balance"]) + float(
                            payment.amount
                        )
                    elif payment.type == PaymentType.transfer:
                        paybox_dict["balance"] = float(paybox_dict["balance"]) + float(
                            payment.amount
                        )

                        paybox_to_q = pboxes.select().where(
                            pboxes.c.id == payment.paybox_to,
                            pboxes.c.deleted_at.is_(None),
                        )
                        paybox_to_rec = await database.fetch_one(paybox_to_q)

                        paybox_to = dict(paybox_to_rec)

                        paybox_to["balance"] = float(paybox_to["balance"]) - float(
                            payment.amount
                        )

                        await manager.send_message(
                            token,
                            {
                                "action": "edit",
                                "target": "payboxes",
                                "result": paybox_to,
                            },
                        )

                        paybox_q = (
                            pboxes.update()
                            .where(pboxes.c.id == payment.paybox_to)
                            .values(paybox_to)
                        )
                        await database.execute(paybox_q)

                    paybox_q = (
                        pboxes.update()
                        .where(pboxes.c.id == payment.paybox)
                        .values(paybox_dict)
                    )
                    await database.execute(paybox_q)

                    await manager.send_message(
                        token,
                        {"action": "edit", "target": "payboxes", "result": paybox_dict},
                    )

                q = (
                    payments.update()
                    .where(payments.c.id == payment_id)
                    .values(
                        is_deleted=True, updated_at=int(datetime.utcnow().timestamp())
                    )
                )
                await database.execute(q)

                if payment.project_id:
                    if payment.status:
                        query = projects.select().where(
                            projects.c.id == payment.project_id
                        )
                        project = await database.fetch_one(query)
                        project_dict = dict(project)

                        if payment.type == PaymentType.incoming:
                            project_dict["incoming"] -= payment.amount_without_tax
                        elif payment.type == PaymentType.outgoing:
                            project_dict["outgoing"] -= payment.amount_without_tax

                        q = payments.select().where(
                            payments.c.project_id == payment.project_id,
                            payments.c.status == True,
                            payments.c.is_deleted == False,
                        )
                        payments_db = await database.fetch_all(q)

                        in_sum = 0
                        out_sum = 0

                        for payment in payments_db:
                            if payment.type == PaymentType.incoming:
                                in_sum += payment.amount_without_tax
                            else:
                                out_sum += payment.amount_without_tax

                        if out_sum == 0 and in_sum != 0:
                            project_dict["profitability"] = 100
                        elif out_sum == 0 and in_sum == 0:
                            project_dict["profitability"] = 0
                        else:
                            project_dict["profitability"] = round(
                                ((in_sum - out_sum) / out_sum) * 100, 2
                            )

                        project_dict["updated_at"] = int(datetime.utcnow().timestamp())

                        await manager.send_message(
                            token,
                            {
                                "action": "edit",
                                "target": "projects",
                                "result": project_dict,
                            },
                        )

                        query = (
                            projects.update()
                            .where(projects.c.id == payment.project_id)
                            .values(project_dict)
                        )
                        await database.execute(query)

                await manager.send_message(
                    token,
                    {"action": "delete", "target": "payments", "result": payment_id},
                )
                return {"result": "success"}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.post("/payments_split/{id}/")
async def raspil_platezha(
    token: str,
    id: int,
    childs: List[pay_schemas.PaymentChildren],
    bg_tasks: BackgroundTasks,
):
    token = token.strip()
    """Распил платежа по ID"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = payments.select().filter(
                payments.c.cashbox == user.cashbox_id,
                payments.c.is_deleted == False,
                payments.c.id == id,
            )
            parent_pay = await database.fetch_one(query)

            up_dict = {"raspilen": True}

            if parent_pay:
                if parent_pay.project_id:
                    up_dict["project_id"] = None
                if not parent_pay.raspilen:
                    q = (
                        payments.update()
                        .where(
                            payments.c.cashbox == user.cashbox_id,
                            payments.c.is_deleted == False,
                            payments.c.id == id,
                        )
                        .values(up_dict)
                    )
                    await database.execute(q)

                    q = payments.select().where(
                        payments.c.cashbox == user.cashbox_id,
                        payments.c.is_deleted == False,
                        payments.c.id == id,
                    )
                    pay = await database.fetch_one(q)

                    await manager.send_message(
                        token,
                        {"action": "edit", "target": "payments", "result": dict(pay)},
                    )

                    for item in childs:
                        child = item.dict()
                        parent_pay_dict = dict(parent_pay)
                        del parent_pay_dict["id"]
                        parent_pay_dict["name"] = parent_pay_dict["name"]
                        parent_pay_dict["amount"] = float(child["amount"])
                        parent_pay_dict["amount_without_tax"] = float(child["amount"])
                        parent_pay_dict["contragent"] = child["ca"]
                        parent_pay_dict["project_id"] = child["project"]
                        parent_pay_dict["parent_id"] = parent_pay.id
                        parent_pay_dict["created_at"] = int(
                            datetime.utcnow().timestamp()
                        )
                        parent_pay_dict["updated_at"] = int(
                            datetime.utcnow().timestamp()
                        )

                        q = payments.insert().values(parent_pay_dict)
                        await database.execute(q)

                    # bg_tasks.add_task(raschet, user, token)
                    await asyncio.gather(
                        asyncio.create_task(_raschet_debounced(user, token))
                    )

    else:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.put("/payments_split/")
async def update_childs_payments(
    token: str, payments_list: List[pay_schemas.ChildrenEdit], bg_tasks: BackgroundTasks
):
    """Обновление дочерних платежей если родительский распилен"""
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if user:
        if user.status:
            for payment in payments_list:
                payment_q = payments.select().where(
                    payments.c.id == payment.id, payments.c.cashbox == user.cashbox_id
                )
                payment_db = await database.fetch_one(payment_q)

                update_dict = {
                    i: j
                    for i, j in payment.dict().items()
                    if getattr(payment_db, i) != j
                }

                if update_dict:

                    update_dict["updated_at"] = int(datetime.utcnow().timestamp())
                    if "project_id" in update_dict and update_dict["project_id"] == 0:
                        update_dict["project_id"] = None

                    if "project_id" in update_dict and update_dict["project_id"] != 0:
                        payment_q = (
                            payments.update()
                            .where(
                                payments.c.id == payment_db.parent_id,
                                payments.c.cashbox == user.cashbox_id,
                            )
                            .values({"project_id": None})
                        )
                        await database.execute(payment_q)

                    if "contragent" in update_dict and update_dict["contragent"] == 0:
                        update_dict["contragent"] = None

                    q = (
                        payments.update()
                        .where(
                            payments.c.id == payment.id,
                            payments.c.cashbox == user.cashbox_id,
                        )
                        .values(update_dict)
                    )
                    await database.execute(q)

            # bg_tasks.add_task(raschet, user, token)
            await asyncio.gather(asyncio.create_task(_raschet_debounced(user, token)))

    else:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.delete("/payments_split/{id}/", response_model=pay_schemas.PaymentInList)
async def split_payment(token: str, id: int, bg_tasks: BackgroundTasks):
    """Отмена распила платежа"""
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if user:
        if user.status:
            payment_q = payments.select().where(
                payments.c.id == id, payments.c.cashbox == user.cashbox_id
            )
            payment_db = await database.fetch_one(payment_q)

            if payment_db:

                if payment_db.raspilen:
                    payment_q = (
                        payments.update()
                        .where(
                            payments.c.id == id, payments.c.cashbox == user.cashbox_id
                        )
                        .values({"raspilen": False})
                    )
                    await database.execute(payment_q)

                    payment_q = (
                        payments.update()
                        .where(payments.c.parent_id == id)
                        .values({"is_deleted": True})
                    )
                    await database.execute(payment_q)

                    q = payments.select().where(
                        payments.c.id == id, payments.c.cashbox == user.cashbox_id
                    )
                    pay = await database.fetch_one(q)

                    await manager.send_message(
                        token,
                        {"action": "edit", "target": "payments", "result": dict(pay)},
                    )

                    # bg_tasks.add_task(raschet, user, token)
                    await asyncio.gather(
                        asyncio.create_task(_raschet_debounced(user, token))
                    )

    else:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/payments/{id}/childs/", response_model=pay_schemas.GetPayments)
async def read_payments_childs(token: str, id: int, offset: int = 0, limit: int = 100):
    """Получение дочерних платежей по ID родительского"""
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = (
                payments.select()
                .filter(payments.c.cashbox == user.cashbox_id)
                .filter(payments.c.is_deleted == False)
                .filter(payments.c.parent_id == id)
                .order_by(desc(payments.c.id))
            )
            pays = await database.fetch_all(query.offset(offset).limit(limit))

            if pays:
                pays_list = [dict(pay) for pay in pays]

                c = (
                    select(func.count(distinct(payments.c.id)))
                    .filter(payments.c.is_deleted == False)
                    .filter(payments.c.cashbox == user.cashbox_id)
                    .filter(payments.c.parent_id == id)
                )
                count = await database.fetch_one(c)

                return {"result": pays_list, "count": count.count_1}
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Вы ввели несуществующий платеж или он не распилен или он не принадлежит вам!",
                )
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/payments_meta/")
async def read_payments_meta(token: str, limit: int = 100, offset: int = 0):
    """Мета платежей"""
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            q = "SELECT DISTINCT payments.name, payments.tags, payments.article FROM payments WHERE payments.cashbox = :cashbox LIMIT :limit OFFSET :offset"
            payment = await database.fetch_all(
                q, {"cashbox": user.cashbox_id, "limit": limit, "offset": offset}
            )

            return payment

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.put("/payments/{id}/attachment/", response_model=pay_schemas.PaymentInList)
async def attach_payment(token: str, id: int, sale_id: int = 0, purchase_id: int = 0):
    """Прикрепление платежа к документам продажи и закупки"""
    token = token.strip()
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    if bool(sale_id) == bool(purchase_id):
        raise HTTPException(
            status_code=400,
            detail="Укажите только одно значение: либо sale_id, либо purchase_id",
        )

    query = payments.select().where(
        payments.c.id == id, payments.c.cashbox == user.cashbox_id
    )
    payment_db = await database.fetch_one(query)
    if not payment_db:
        raise HTTPException(status_code=404, detail="Платёж не найден")

    if sale_id:
        doc_query = docs_sales.select().where(
            docs_sales.c.id == sale_id, docs_sales.c.cashbox == user.cashbox_id
        )
        doc = await database.fetch_one(doc_query)
        if not doc:
            raise HTTPException(status_code=404, detail="Документ продажи не найден")
    elif purchase_id:
        doc_query = docs_purchases.select().where(
            docs_purchases.c.id == purchase_id,
            docs_purchases.c.cashbox == user.cashbox_id,
        )
        doc = await database.fetch_one(doc_query)
        if not doc:
            raise HTTPException(status_code=404, detail="Документ закупки не найден")

    entity_values = {
        "cashbox_id": user.cashbox_id,
        "type": "payments_docs_sales" if sale_id else "payments_docs_purchases",
        "from_entity": 5,
        "to_entity": 7 if sale_id else 1,
        "from_id": id,
        "to_id": sale_id or purchase_id,
        "status": True,
        "delinked": False,
    }

    existing = await database.fetch_one(
        entity_to_entity.select().where(
            entity_to_entity.c.from_id == id,
            entity_to_entity.c.to_id == entity_values["to_id"],
            entity_to_entity.c.type == entity_values["type"],
            entity_to_entity.c.cashbox_id == user.cashbox_id,
        )
    )

    if existing:
        raise HTTPException(status_code=400, detail="Эта связь уже существует")

    await database.execute(entity_to_entity.insert().values(entity_values))

    await database.execute(
        payments.update()
        .where(payments.c.id == id)
        .values(updated_at=int(datetime.utcnow().timestamp()))
    )

    payment_db = await database.fetch_one(query)

    entity_links = await database.fetch_all(
        entity_to_entity.select().where(
            entity_to_entity.c.from_id == id,
            entity_to_entity.c.cashbox_id == user.cashbox_id,
            entity_to_entity.c.type.in_(
                ["payments_docs_sales", "payments_docs_purchases"]
            ),
        )
    )

    docs_sales_id = None
    docs_purchases_id = None
    for e in entity_links:
        if e["type"] == "payments_docs_sales":
            docs_sales_id = e["to_id"]
        elif e["type"] == "payments_docs_purchases":
            docs_purchases_id = e["to_id"]

    notification_data = {
        "type": "payment_attachment",
        "payment_id": id,
        "user_token": token,
        "action": "attach",
        "docs_sales_id": docs_sales_id,
        "docs_purchases_id": docs_purchases_id,
        "timestamp": payment_db["updated_at"],
    }
    await queue_notification(notification_data)

    payment_dict = dict(payment_db)
    payment_dict["docs_sales_id"] = docs_sales_id
    payment_dict["docs_purchases_id"] = docs_purchases_id

    await manager.send_message(
        token,
        {"action": "edit", "target": "payments", "result": payment_dict},
    )

    return payment_dict


@router.delete("/payments/{id}/attachment/", response_model=pay_schemas.PaymentInList)
async def detach_payment(token: str, id: int, sale_id: int):
    """Открепление платежа от документа продажи"""
    token = token.strip()
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    query = (
        payments.update()
        .where(payments.c.id == id, payments.c.cashbox == user.cashbox_id)
        .values(
            {"docs_sales_id": None, "updated_at": int(datetime.utcnow().timestamp())}
        )
    )
    await database.execute(query)

    query = payments.select().where(
        payments.c.id == id, payments.c.cashbox == user.cashbox_id
    )
    payment_db = await database.fetch_one(query)

    await manager.send_message(
        token,
        {"action": "edit", "target": "payments", "result": dict(payment_db)},
    )

    return payment_db
