import logging
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import func, select

logger = logging.getLogger(__name__)

BASE_URL = "https://app.tablecrm.com"


from api.balances.services.balance_service import make_deposit
from api.balances.services.tinkoff_service import TinkoffApiService
from api.balances.transactions_schemas import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    TinkoffCallbackData,
    TransactionCreate,
    TransactionResponse,
)
from const import SUCCESS, PaymentType
from database.db import (
    accounts_balances,
    database,
    tariffs,
    transactions,
    users,
    users_cboxes_relation,
    users_cboxes_relation as ucr,
)

router = APIRouter(prefix="/transactions", tags=["transactions"])
tinkoff_router = APIRouter(prefix="/payments/tinkoff", tags=["tinkoff"])


def get_user_from_token(token: str):
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    return query


async def verify_user(token: str):
    """Verify user token and return user relation"""
    query = get_user_from_token(token)
    user = await database.fetch_one(query)
    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Invalid or inactive token")
    return user


@router.post("/deposit", response_model=TransactionResponse)
async def create_manual_deposit(
    token: str,
    transaction_data: TransactionCreate,
    admin: Optional[int] = Query(None, description="Admin ID for manual deposits"),
):
    user = await verify_user(token)

    if not admin:
        raise HTTPException(
            status_code=403, detail="Manual deposits require admin parameter in URL"
        )

    balance_query = accounts_balances.select().where(
        accounts_balances.c.cashbox == user.cashbox_id
    )
    balance = await database.fetch_one(balance_query)

    if not balance:
        raise HTTPException(
            status_code=404, detail="Balance not found for this cashbox"
        )

    tariff_id = transaction_data.tariff_id or balance.tariff

    users_count_query = select(func.count(ucr.c.id)).where(
        ucr.c.cashbox_id == user.cashbox_id
    )
    users_quantity = await database.execute(users_count_query)

    transaction_id = await make_deposit(
        cashbox_id=user.cashbox_id,
        amount=transaction_data.amount,
        tariff_id=tariff_id,
        users_quantity=users_quantity,
        is_manual=True,
        status=SUCCESS,
    )

    transaction_query = transactions.select().where(transactions.c.id == transaction_id)
    transaction = await database.fetch_one(transaction_query)

    return TransactionResponse(**dict(transaction))


@router.post("/payment/create", response_model=PaymentCreateResponse)
async def create_payment(
    token: str,
    payment_data: PaymentCreateRequest,
    request: Request,
):
    user = await verify_user(token)

    balance_query = accounts_balances.select().where(
        accounts_balances.c.cashbox == user.cashbox_id
    )
    balance = await database.fetch_one(balance_query)

    if not balance:
        raise HTTPException(
            status_code=404, detail="Balance not found for this cashbox"
        )

    tariff_id = payment_data.tariff_id or balance.tariff
    tariff = await database.fetch_one(tariffs.select().where(tariffs.c.id == tariff_id))

    if not tariff:
        raise HTTPException(status_code=404, detail="Tariff not found")

    final_amount = payment_data.amount
    if tariff.offer_hours and tariff.discount_percent:
        cashbox_created_at = datetime.fromtimestamp(balance.created_at)
        hours_since_creation = (
            datetime.utcnow() - cashbox_created_at
        ).total_seconds() / 3600

        if hours_since_creation <= tariff.offer_hours:
            first_payment_query = select(func.count(transactions.c.id)).where(
                transactions.c.cashbox == user.cashbox_id,
                transactions.c.type == PaymentType.incoming,
                transactions.c.status == SUCCESS,
            )
            first_payment_count = await database.execute(first_payment_query)

            if first_payment_count == 0:
                discount_amount = final_amount * (tariff.discount_percent / 100)
                final_amount = final_amount - discount_amount

    timestamp = int(datetime.utcnow().timestamp())
    short_uuid = uuid.uuid4().hex[:6]
    payment_id = f"pay_{user.cashbox_id}_{timestamp}_{short_uuid}"
    if len(payment_id) > 50:
        payment_id = payment_id[:50]

    transaction_query = transactions.insert().values(
        {
            "cashbox": user.cashbox_id,
            "tariff": tariff_id,
            "users": None,
            "amount": final_amount,
            "status": "pending",
            "type": PaymentType.incoming,
            "is_manual_deposit": False,
            "external_id": payment_id,
            "created_at": int(datetime.utcnow().timestamp()),
            "updated_at": int(datetime.utcnow().timestamp()),
        }
    )
    transaction_id = await database.execute(transaction_query)

    tinkoff_service = TinkoffApiService()

    return_url = f"{BASE_URL}/?token={token}&use_as_is=true"

    notification_url = f"{BASE_URL}/api/v1/payments/tinkoff/callback"

    amount_kopecks = int(final_amount * 100)

    description = f"Пополнение баланса для кассы {user.cashbox_id}"
    user_data = None
    if user.user:
        user_query = users.select().where(users.c.id == user.user)
        user_data = await database.fetch_one(user_query)

    email = ""
    phone = ""
    customer_name = ""
    if user_data:
        phone = user_data.phone_number or ""
        email = "noreply@tablecrm.com"
        if user_data.first_name or user_data.last_name:
            customer_name = (
                f"{user_data.first_name or ''} {user_data.last_name or ''}".strip()
            )

    receipt_item = {
        "name": description[:128],
        "price": final_amount,
        "quantity": 1,
        "tax": "none",
        "payment_object": "service",
        "payment_method": "full_prepayment",
        "measurement_unit": "шт",
    }

    receipt = tinkoff_service.create_receipt(
        email=email,
        phone=phone,
        items=[receipt_item],
        taxation="usn_income",
        ffd_version="1.2",
        customer=customer_name if customer_name else None,
    )

    response = await tinkoff_service.init_payment(
        amount=amount_kopecks,
        order_id=payment_id,
        description=description,
        return_url=return_url,
        notification_url=notification_url,
        data={
            "cashbox_id": str(user.cashbox_id),
            "transaction_id": str(transaction_id),
        },
        receipt=receipt,
    )

    if not response.get("Success"):
        error_message = response.get("Message", "Unknown error")
        await database.execute(
            transactions.update()
            .where(transactions.c.id == transaction_id)
            .values(status="failed")
        )
        raise HTTPException(
            status_code=400, detail=f"Failed to create payment: {error_message}"
        )

    payment_url = response.get("PaymentURL")
    if not payment_url:
        raise HTTPException(
            status_code=500, detail="Payment URL not received from Tinkoff"
        )

    return PaymentCreateResponse(
        payment_id=payment_id,
        payment_url=payment_url,
        status="pending",
        message="Payment created successfully",
    )


@tinkoff_router.get("/test")
async def tinkoff_test():
    return {
        "status": "ok",
        "message": "Tinkoff router is working",
        "path": "/payments/tinkoff/test",
    }


@tinkoff_router.post("/callback")
@tinkoff_router.get("/callback")
async def tinkoff_callback(
    request: Request,
):
    try:
        body = await request.body()

        raw_data = None

        if body:
            try:
                raw_data = await request.json()
            except Exception:
                pass

        if not raw_data:
            try:
                form_data = await request.form()
                if form_data:
                    raw_data = dict(form_data)
            except Exception:
                pass

        if not raw_data and request.query_params:
            raw_data = dict(request.query_params)

        if not raw_data:
            return {
                "error": "No data received",
                "body": body.decode("utf-8") if body else "Empty",
                "query": dict(request.query_params),
            }

        processed_data = {}
        for key, value in raw_data.items():
            if key == "Success":
                if isinstance(value, bool):
                    processed_data[key] = value
                else:
                    processed_data[key] = str(value).lower() in ("true", "1", "yes")
            elif key in ("Amount", "ErrorCode") and value:
                try:
                    processed_data[key] = int(value)
                except (ValueError, TypeError):
                    processed_data[key] = value
            else:
                processed_data[key] = value

        callback_data = TinkoffCallbackData(**processed_data)

    except Exception as e:
        import traceback

        error_msg = f"Error processing callback: {e}\n{traceback.format_exc()}"
        logger.error(error_msg)
        return {"error": "Error processing callback", "details": str(e)}

    order_id = callback_data.OrderId
    status = callback_data.Status
    success = callback_data.Success
    amount = callback_data.Amount

    if not order_id.startswith("pay_"):
        return {"error": "Invalid order_id format"}

    parts = order_id.split("_")
    if len(parts) < 3:
        return {"error": "Invalid order_id format"}

    cashbox_id = int(parts[1])

    tinkoff_service = TinkoffApiService()
    internal_status = tinkoff_service.map_tinkoff_status_to_internal(status)

    if success and status != "CONFIRMED":
        internal_status = "success"

    transaction_query = transactions.select().where(
        transactions.c.external_id == order_id,
        transactions.c.cashbox == cashbox_id,
        transactions.c.type == PaymentType.incoming,
    )
    transaction = await database.fetch_one(transaction_query)

    if not transaction:
        target_timestamp = int(parts[2]) if len(parts) > 2 else None
        if target_timestamp:
            all_transactions_query = (
                transactions.select()
                .where(
                    transactions.c.cashbox == cashbox_id,
                    transactions.c.type == PaymentType.incoming,
                )
                .order_by(transactions.c.created_at.desc())
                .limit(20)
            )
            all_transactions = await database.fetch_all(all_transactions_query)

            for trans in all_transactions:
                time_diff = abs(trans.created_at - target_timestamp)
                if time_diff <= 60:
                    transaction = trans
                    break

    if not transaction:
        logger.error(f"Transaction not found for order_id={order_id}")
        return {"error": "Transaction not found"}

    was_already_success = transaction.status == SUCCESS

    update_query = (
        transactions.update()
        .where(transactions.c.id == transaction.id)
        .values(
            status=internal_status,
            updated_at=int(datetime.utcnow().timestamp()),
        )
    )
    await database.execute(update_query)

    if internal_status == SUCCESS and not was_already_success:
        amount_rubles = amount / 100.0 if amount else transaction.amount

        balance_query = accounts_balances.select().where(
            accounts_balances.c.cashbox == cashbox_id
        )
        balance = await database.fetch_one(balance_query)

        if balance:
            new_balance = balance.balance + amount_rubles

            update_balance_query = (
                accounts_balances.update()
                .where(accounts_balances.c.id == balance.id)
                .values(
                    balance=new_balance,
                    tariff_type="paid",
                    updated_at=int(datetime.utcnow().timestamp()),
                    last_transaction=transaction.id,
                )
            )
            await database.execute(update_balance_query)

            from database.db import cboxes

            await database.execute(
                cboxes.update()
                .where(cboxes.c.id == cashbox_id)
                .values(
                    {
                        "balance": new_balance,
                        "updated_at": int(datetime.utcnow().timestamp()),
                    }
                )
            )

    return {"success": True, "status": internal_status}


@router.get("/transaction/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(
    token: str,
    transaction_id: int,
):
    user = await verify_user(token)

    transaction_query = transactions.select().where(
        transactions.c.id == transaction_id, transactions.c.cashbox == user.cashbox_id
    )
    transaction = await database.fetch_one(transaction_query)

    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return TransactionResponse(**dict(transaction))


@router.post("/payment/check-status")
async def check_payment_status(
    token: str,
    order_id: str = Query(
        ..., description="Order ID платежа (payment_id из ответа create_payment)"
    ),
):
    user = await verify_user(token)

    if not order_id.startswith("pay_"):
        raise HTTPException(status_code=400, detail="Invalid order_id format")

    parts = order_id.split("_")
    if len(parts) < 3:
        raise HTTPException(status_code=400, detail="Invalid order_id format")

    cashbox_id = int(parts[1])

    if cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=403, detail="Access denied")

    target_timestamp = int(parts[2]) if len(parts) > 2 else None

    transaction_query = (
        transactions.select()
        .where(
            transactions.c.cashbox == cashbox_id,
            transactions.c.type == PaymentType.incoming,
        )
        .order_by(transactions.c.created_at.desc())
        .limit(20)
    )
    recent_transactions = await database.fetch_all(transaction_query)

    matching_transaction = None
    if target_timestamp:
        for trans in recent_transactions:
            time_diff = abs(trans.created_at - target_timestamp)
            if time_diff <= 60:
                matching_transaction = trans
                break

    if not matching_transaction:
        raise HTTPException(
            status_code=404, detail="Transaction not found for this order_id"
        )

    return {
        "transaction_id": matching_transaction.id,
        "order_id": order_id,
        "status": matching_transaction.status,
        "amount": matching_transaction.amount,
        "created_at": matching_transaction.created_at,
        "updated_at": matching_transaction.updated_at,
        "message": f"Transaction status: {matching_transaction.status}",
    }
