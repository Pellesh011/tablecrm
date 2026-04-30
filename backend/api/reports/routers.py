from database.db import (
    database,
    docs_sales,
    docs_sales_goods,
    nomenclature,
    payments,
    pboxes,
    users_cboxes_relation,
)
from fastapi import APIRouter
from functions.helpers import get_user_by_token
from sqlalchemy import and_, asc, func, select, text

from . import schemas

router = APIRouter(tags=["reports"])


@router.post("/reports/sales/")
async def get_sales_report(token: str, report_data: schemas.ReportData):
    user = await get_user_by_token(token)

    filters_all_sales = [
        docs_sales.c.cashbox == user.cashbox_id,
        text(f"docs_sales.dated >= {report_data.datefrom}"),
        text(f"docs_sales.dated <= {report_data.dateto}"),
        docs_sales.c.is_deleted.is_not(True),
    ]
    filter_user = []
    if report_data.user:
        filter_user.append(users_cboxes_relation.c.user == report_data.user)

    query_all_sales = (
        select(
            docs_sales_goods.c.nomenclature.label("nom_id"),
            docs_sales.c.id.label("doc_sales_id"),
            docs_sales_goods.c.price,
            docs_sales_goods.c.quantity,
            docs_sales.c.created_by,
            docs_sales.c.cashbox,
        )
        .join(docs_sales, docs_sales.c.id == docs_sales_goods.c.docs_sales_id)
        .where(*filters_all_sales)
        .subquery("query_all_sales")
    )

    query_group = (
        select(
            query_all_sales.c.nom_id,
            query_all_sales.c.created_by,
            func.sum(query_all_sales.c.quantity).label("count"),
            func.sum(query_all_sales.c.quantity * query_all_sales.c.price).label("sum"),
        )
        .group_by(query_all_sales.c.nom_id, query_all_sales.c.created_by)
        .subquery("query_group")
    )

    query = (
        select(
            query_group.c.nom_id,
            nomenclature.c.name.label("nomenclature_name"),
            query_group.c.count,
            query_group.c.sum,
        )
        .join(
            users_cboxes_relation,
            users_cboxes_relation.c.id == query_group.c.created_by,
        )
        .join(nomenclature, nomenclature.c.id == query_group.c.nom_id)
        .where(*filter_user)
        .order_by(asc(nomenclature.c.name))
    )

    result = await database.fetch_all(query)

    return result


@router.post("/reports/balances/")
async def get_balances_report(token: str, report_data: schemas.ReportData):
    user = await get_user_by_token(token)
    if len(report_data.paybox) < 1:
        report_data.paybox = [
            item.id
            for item in await database.fetch_all(
                select(pboxes.c.id).where(
                    pboxes.c.cashbox == user.cashbox_id,
                    pboxes.c.deleted_at.is_(None),
                )
            )
        ]
    report = []
    for paybox in report_data.paybox:
        filters = [
            payments.c.paybox == paybox,
            text(f"payments.date >= {report_data.datefrom}"),
            text(f"payments.date <= {report_data.dateto}"),
            payments.c.is_deleted.is_not(True),
        ]

        if report_data.user:
            filters.append(payments.c.account == report_data.user)

        query_incoming = (
            select(
                payments.c.paybox,
                pboxes.c.name,
                payments.c.type,
                func.sum(payments.c.amount).label("incoming"),
            )
            .where(
                *filters,
                payments.c.type == "incoming",
            )
            .join(
                pboxes,
                and_(
                    pboxes.c.id == payments.c.paybox,
                    pboxes.c.deleted_at.is_(None),
                ),
            )
            .group_by(payments.c.paybox, payments.c.type, pboxes.c.name)
        )

        query_outgoing = (
            select(
                payments.c.paybox,
                pboxes.c.name,
                payments.c.type,
                func.sum(payments.c.amount).label("outgoing"),
            )
            .where(
                *filters,
                payments.c.type == "outgoing",
            )
            .join(pboxes, pboxes.c.id == payments.c.paybox)
            .group_by(payments.c.paybox, payments.c.type, pboxes.c.name)
        )

        report_db_in = [dict(item) for item in await database.fetch_all(query_incoming)]
        report_db_out = [
            dict(item) for item in await database.fetch_all(query_outgoing)
        ]

        query = select(pboxes.c.name, pboxes.c.balance).where(
            pboxes.c.id == paybox,
            pboxes.c.deleted_at.is_(None),
        )
        report_db = await database.fetch_one(query)
        if not report_db:
            continue
        report_db = dict(report_db)
        report_db["incoming"] = (
            report_db_in[0]["incoming"] if len(report_db_in) > 0 else 0
        )
        report_db["outgoing"] = (
            report_db_out[0]["outgoing"] if len(report_db_out) > 0 else 0
        )

        if report_db:
            report.append(report_db)
    return report
