from datetime import datetime

from const import PaymentType
from database.db import (
    contracts,
    contragents,
    database,
    docs_purchases,
    docs_reconciliation,
    docs_sales,
    entity_to_entity,
    organizations,
    payments,
)
from fastapi import APIRouter, HTTPException
from functions.helpers import datetime_to_timestamp, get_user_by_token
from sqlalchemy import func, select
from ws_manager import manager

from . import schemas

router = APIRouter(tags=["docs_reconciliation"])

contragents_cache = set()
organizations_cache = set()
contracts_cache = set()
warehouses_cache = set()
users_cache = set()
price_types_cache = set()
units_cache = set()


@router.get("/docs_reconciliation/{idx}/", response_model=schemas.View)
async def get_by_id(token: str, idx: int):
    """Получение документа по ID"""
    await get_user_by_token(token)
    query = docs_reconciliation.select().where(
        docs_reconciliation.c.id == idx, docs_reconciliation.c.is_deleted.is_not(True)
    )
    instance_db = await database.fetch_one(query)

    if not instance_db:
        raise HTTPException(status_code=404, detail="Не найдено.")

    instance_db = datetime_to_timestamp(instance_db)
    return instance_db


@router.get("/docs_reconciliation/", response_model=schemas.ListViewGet)
async def get_list(token: str, limit: int = 100, offset: int = 0):
    """Получение списка документов"""
    await get_user_by_token(token)
    query = (
        docs_reconciliation.select()
        .where(docs_reconciliation.c.is_deleted.is_not(True))
        .limit(limit)
        .offset(offset)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    query = select(func.count(docs_reconciliation.c.id)).where(
        docs_reconciliation.c.is_deleted.is_not(True)
    )
    items_db_c = await database.fetch_one(query)
    return {"result": items_db, "count": items_db_c.count_1}


@router.post("/docs_reconciliation/", response_model=schemas.CreateListView)
async def create(token: str, docs_reconciliation_data: schemas.CreateMass):
    """Создание документов"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    exceptions = []
    for request_values in docs_reconciliation_data.dict()["__root__"]:
        # check if organization's and contragent's ids or inns are provided
        if (
            not request_values.get("contragent_id")
            and not request_values.get("contragent_inn")
        ) or (
            not request_values.get("organization_id")
            and not request_values.get("organization_inn")
        ):
            request_values["error"] = "Не указана организация или контрагент"
            exceptions.append(request_values)
            continue

        # get organization and contragent from db and get its ids
        if request_values.get("contragent_id"):
            query = contragents.select().where(
                contragents.c.id == request_values.get("contragent_id")
            )
        else:
            query = contragents.select().where(
                contragents.c.inn == request_values.get("contragent_inn")
            )
        contragent_db = await database.fetch_one(query)
        if contragent_db:
            contragent_id = contragent_db.id
        else:
            request_values["error"] = "Не найден контрагент в базе"
            exceptions.append(request_values)
            continue
        if request_values.get("organization_id"):
            query = organizations.select().where(
                organizations.c.id == request_values.get("organization_id")
            )
        else:
            query = organizations.select().where(
                organizations.c.inn == request_values.get("organization_inn")
            )
        organization_db = await database.fetch_one(query)
        if organization_db:
            organization_id = organization_db.id
        else:
            request_values["error"] = "Не найдена организация в базе"
            exceptions.append(request_values)
            continue

        if request_values.get("contract_id"):
            query = contracts.select().where(
                contracts.c.id == request_values.get("contract_id")
            )
            if not await database.fetch_one(query):
                request_values["error"] = "Контракта не существует!"
                exceptions.append(request_values)
                continue

        # get sales and purchase docs and payments for specified contract
        query = docs_sales.select().where(
            docs_sales.c.organization == organization_id,
            docs_sales.c.contragent == contragent_id,
        )
        if request_values.get("contract_id"):
            query = query.where(docs_sales.c.contract == request_values["contract_id"])
        sales = await database.fetch_all(query)

        query = docs_purchases.select().where(
            docs_purchases.c.organization == organization_id,
            docs_purchases.c.contragent == contragent_id,
        )
        if request_values.get("contract_id"):
            query = query.where(
                docs_purchases.c.contract == request_values["contract_id"]
            )
        purchases = await database.fetch_all(query)

        query = entity_to_entity.select().where(
            entity_to_entity.c.to_entity == "organizations",
            entity_to_entity.c.from_entity == "payments",
            entity_to_entity.c.to_id == organization_id,
        )
        entities_rel = await database.fetch_all(query)
        payment_ids_for_org = {entity.from_id for entity in entities_rel}
        query_payment = payments.select().where(
            payments.c.id.in_(payment_ids_for_org),
            payments.c.contragent == contragent_id,
            payments.c.type.in_((PaymentType.incoming, PaymentType.outgoing)),
        )
        if request_values.get("contract_id"):
            query = entity_to_entity.select().where(
                entity_to_entity.c.to_entity == "contracts",
                entity_to_entity.c.from_entity == "payments",
                entity_to_entity.c.to_id == request_values.get("contract_id"),
            )
            entities_rel = await database.fetch_all(query)
            payment_ids_for_contract = {entity.from_id for entity in entities_rel}

            payment_ids_intersection = payment_ids_for_contract.intersection(
                payment_ids_for_org
            )
            query_payment = query_payment.where(
                payments.c.id.in_(payment_ids_intersection)
            )

        payments_list = await database.fetch_all(query_payment)

        organization_period_debt = 0
        organization_period_credit = 0
        contragent_period_debt = 0
        contragent_period_credit = 0
        organization_initial_balance = 0
        contragent_initial_balance = 0
        documents = []

        for doc in purchases:
            doc_summary = {
                "type": "purchase",
                "number": doc.number,
                "dated": doc.dated,
                "organization_debt": doc.sum,
                "organization_credit": None,
                "contragent_debt": None,
                "contragent_credit": doc.sum,
                "contract_id": doc.contract,
            }
            if (
                not request_values.get("period_from")
                or doc.dated >= request_values.get("period_from")
            ) and (
                not request_values.get("period_to")
                or doc.dated <= request_values.get("period_to")
            ):
                documents.append(doc_summary)
                organization_period_debt += doc.sum
                contragent_period_credit += doc.sum
            elif (
                request_values.get("period_from")
                and doc.dated < request_values["period_from"]
            ):
                organization_initial_balance -= doc.sum
                contragent_initial_balance += doc.sum
        for doc in sales:
            doc_summary = {
                "type": "sale",
                "number": doc.number,
                "dated": doc.dated,
                "organization_debt": None,
                "organization_credit": doc.sum,
                "contragent_debt": doc.sum,
                "contragent_credit": None,
                "contract_id": doc.contract,
            }
            if (
                not request_values.get("period_from")
                or doc.dated >= request_values.get("period_from")
            ) and (
                not request_values.get("period_to")
                or doc.dated <= request_values.get("period_to")
            ):
                documents.append(doc_summary)
                organization_period_credit += doc.sum
                contragent_period_debt += doc.sum
            elif (
                request_values.get("period_from")
                and doc.dated < request_values["period_from"]
            ):
                organization_initial_balance -= doc.sum
                contragent_initial_balance += doc.sum
        for pay in payments_list:
            query = entity_to_entity.select().where(
                entity_to_entity.c.to_entity == "contracts",
                entity_to_entity.c.from_entity == "payments",
                entity_to_entity.c.from_id == pay.id,
            )
            contract_rel = await database.fetch_one(query)
            contract_id = contract_rel.to_id if contract_rel else None

            pay_summary = {
                "type": "payment",
                "number": str(pay.id),
                "dated": pay.date,
                "organization_debt": None,
                "organization_credit": pay.amount,
                "contragent_debt": pay.amount,
                "contragent_credit": None,
                "contract_id": contract_id,
            }
            if (
                not request_values.get("period_from")
                or pay.date >= request_values.get("period_from")
            ) and (
                not request_values.get("period_to")
                or pay.date <= request_values.get("period_to")
            ):
                if pay.type == PaymentType.outgoing:
                    documents.append(pay_summary)
                    organization_period_credit += pay.amount
                    contragent_period_debt += pay.amount
                else:
                    pay_summary["organiztion_debt"] = pay.amount
                    pay_summary["organiztion_credit"] = None
                    pay_summary["contragent_debt"] = None
                    pay_summary["contragent_credit"] = pay.amount
                    documents.append(pay_summary)
                    organization_period_debt += pay.amount
                    contragent_period_credit += pay.amount

            elif (
                request_values.get("period_from")
                and pay.date < request_values["period_from"]
            ):
                if pay.type == PaymentType.outgoing:
                    organization_initial_balance -= pay.amount
                    contragent_initial_balance += pay.amount
                else:
                    organization_initial_balance += pay.amount
                    contragent_initial_balance -= pay.amount

        organization_closing_balance = (
            organization_initial_balance
            + organization_period_credit
            - organization_period_debt
        )
        contragent_closing_balance = (
            contragent_initial_balance
            + contragent_period_credit
            - contragent_period_debt
        )

        grouped_docs = None
        if request_values.get("group_by_contract"):
            grouped_docs = {}
            for doc in documents:
                doc_name = doc["contract_id"]
                if doc_name not in grouped_docs.keys():
                    grouped_docs[doc_name] = []
                grouped_docs[doc_name].append(doc)
            documents = None

        reconciliation = {
            "created_by": user.id,
            "dated": int(datetime.now().timestamp()),
            "organization": organization_id,
            "contragent": contragent_id,
            "contract": (
                request_values["contract_id"]
                if request_values.get("contract_id")
                else None
            ),
            "organization_name": organization_db.short_name,
            "contragent_name": contragent_db.name,
            "period_from": request_values["period_from"],
            "period_to": request_values["period_to"],
            "organization_period_debt": organization_period_debt or None,
            "organization_period_credit": organization_period_credit or None,
            "contragent_period_debt": contragent_period_debt or None,
            "contragent_period_credit": contragent_period_credit or None,
            "organization_initial_balance": organization_initial_balance,
            "contragent_initial_balance": contragent_initial_balance,
            "organization_closing_balance": organization_closing_balance,
            "contragent_closing_balance": contragent_closing_balance,
            "documents": documents,
            "documents_grouped": grouped_docs,
        }

        query = docs_reconciliation.insert().values(reconciliation)
        instance_id = await database.execute(query)
        query = (
            docs_reconciliation.update()
            .where(docs_reconciliation.c.id == instance_id)
            .values({"number": str(instance_id)})
        )
        await database.execute(query)
        inserted_ids.add(instance_id)

    query = docs_reconciliation.select().where(
        docs_reconciliation.c.id.in_(inserted_ids)
    )
    docs_reconciliation_db = await database.fetch_all(query)
    docs_reconciliation_db = [*map(datetime_to_timestamp, docs_reconciliation_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "docs_reconciliation",
            "result": docs_reconciliation_db,
        },
    )

    return {"results": docs_reconciliation_db, "errors": exceptions}


@router.delete("/docs_reconciliation/{idx}/", response_model=schemas.ListView)
async def delete(token: str, ids: list[int]):
    """Удаление документов"""
    await get_user_by_token(token)

    query = docs_reconciliation.select().where(
        docs_reconciliation.c.id.in_(ids), docs_reconciliation.c.is_deleted.is_not(True)
    )
    items_db = await database.fetch_all(query)
    items_db = [*map(datetime_to_timestamp, items_db)]

    if items_db:
        query = (
            docs_reconciliation.update()
            .where(
                docs_reconciliation.c.id.in_(ids),
                docs_reconciliation.c.is_deleted.is_not(True),
            )
            .values({"is_deleted": True})
        )
        await database.execute(query)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "docs_reconciliation",
                "result": items_db,
            },
        )

    return items_db
