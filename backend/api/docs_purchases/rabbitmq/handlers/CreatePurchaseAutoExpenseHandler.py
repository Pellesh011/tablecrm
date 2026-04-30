from datetime import datetime

from database.db import (
    database,
    docs_purchases,
    docs_purchases_goods,
    nomenclature,
    payments,
    pboxes,
    users_cboxes_relation,
)
from sqlalchemy import and_, text


class CreatePurchaseAutoExpenseHandler:
    async def __call__(self, event=None, message=None, *args, **kwargs):
        payload = None
        for c in (event, message, *args, kwargs.get("payload"), kwargs.get("message")):
            if isinstance(c, dict):
                payload = c
                break

        if not payload:
            print(
                f"[purchase-auto-expense] bad payload types: event={type(event)} message={type(message)}"
            )
            return

        purchase_id = payload.get("purchase_id") or payload.get("purchaseId")
        cashbox_id = (
            payload.get("cashbox_id")
            or payload.get("cashboxId")
            or payload.get("cashbox")
        )
        token = payload.get("token")

        if purchase_id is None or cashbox_id is None:
            print(f"[purchase-auto-expense] bad payload: {payload}")
            return

        await self.handle(int(purchase_id), int(cashbox_id), token)

    async def handle(self, purchase_id: int, cashbox_id: int, token=None):
        purchase = await database.fetch_one(
            docs_purchases.select().where(
                docs_purchases.c.id == purchase_id,
                docs_purchases.c.cashbox == cashbox_id,
                docs_purchases.c.is_deleted.is_not(True),
            )
        )
        if not purchase:
            print(f"[purchase-auto-expense] purchase not found: {purchase_id}")
            return

        goods_rows = await database.fetch_all(
            docs_purchases_goods.select().where(
                docs_purchases_goods.c.docs_purchases_id == purchase_id
            )
        )
        if not goods_rows:
            print(f"[purchase-auto-expense] no goods for purchase: {purchase_id}")
            return

        goods_res = []
        for g in goods_rows:
            nom_id = int(g["nomenclature"])
            nom = await database.fetch_one(
                nomenclature.select().where(nomenclature.c.id == nom_id)
            )
            if not nom:
                continue

            try:
                nom_type = nom["type"]
            except Exception:
                nom_type = getattr(nom, "type", None)

            if nom_type != "product":
                continue

            unit_id = g["unit"] or (
                nom["unit"] if "unit" in nom else getattr(nom, "unit", None)
            )
            goods_res.append(
                {
                    "price_type": 1,
                    "price": 0,
                    "quantity": g["quantity"],
                    "unit": unit_id,
                    "nomenclature": nom_id,
                }
            )

        if not goods_res:
            print(
                f"[purchase-auto-expense] only services/empty goods for purchase: {purchase_id}"
            )
            return

        if not token:
            row = await database.fetch_one(
                text("select token from relation_tg_cashboxes where id = :id"),
                {"id": cashbox_id},
            )
            token = row["token"] if row and row.get("token") else None
        if not token:
            print(
                f"[purchase-auto-expense] token not found for cashbox_id={cashbox_id}, purchase={purchase_id}"
            )
            return

        # ---- AUTO PAYMENT EXPENSE (NOT warehouse outgoing) ----
        external_id = f"purchase:{purchase_id}:auto_expense"
        amount = float(purchase["sum"] or 0)
        now_ts = int(datetime.utcnow().timestamp())
        should_post = bool(purchase["status"])

        # account берём так же, как create_payment(): user.user из users_cboxes_relation по token
        user_row = await database.fetch_one(
            users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
        )
        if not user_row or not user_row.get("status"):
            print(
                f"[purchase-auto-expense] user by token not found/disabled for purchase={purchase_id}"
            )
            return
        account_id = user_row["user"]

        # paybox: берём "default" если есть, иначе первый по id
        paybox = await database.fetch_one(
            pboxes.select().where(
                and_(
                    pboxes.c.cashbox == cashbox_id,
                    pboxes.c.name == "default",
                )
            )
        )
        if not paybox:
            paybox = await database.fetch_one(
                pboxes.select()
                .where(pboxes.c.cashbox == cashbox_id)
                .order_by(pboxes.c.id.asc())
            )
        if not paybox:
            print(
                f"[purchase-auto-expense] paybox not found for cashbox={cashbox_id} purchase={purchase_id}"
            )
            return

        paybox_id = int(paybox["id"])
        paybox_balance = float(paybox["balance"] or 0)
        balance_date = paybox["balance_date"]

        existing = await database.fetch_one(
            payments.select()
            .where(
                and_(
                    payments.c.cashbox == cashbox_id,
                    payments.c.external_id == external_id,
                    payments.c.is_deleted.is_not(True),
                )
            )
            .order_by(payments.c.id.desc())
        )

        def _should_affect_balance() -> bool:
            if balance_date is None:
                return False
            try:
                return int(balance_date) <= int(purchase["dated"])
            except Exception:
                return False

        if existing:
            old_status = bool(existing["status"])
            old_amount = float(existing["amount"] or 0)

            upd = (
                payments.update()
                .where(payments.c.id == existing["id"])
                .values(
                    {
                        "type": "outgoing",
                        "amount": amount,
                        "amount_without_tax": amount,
                        "status": should_post,
                        "updated_at": now_ts,
                        "docs_purchases_id": purchase_id,
                        "contragent": purchase["contragent"],
                        "contract_id": purchase["contract"],
                        "date": int(purchase["dated"]),
                        "paybox": paybox_id,
                        "account": account_id,
                        "tags": existing["tags"] or "",
                    }
                )
            )
            await database.execute(upd)

            if _should_affect_balance() and old_status != should_post:
                delta = 0.0
                if should_post and not old_status:
                    delta = -amount
                elif old_status and not should_post:
                    delta = +old_amount

                if delta != 0.0:
                    new_balance = round(paybox_balance + delta, 2)
                    await database.execute(
                        pboxes.update()
                        .where(
                            and_(
                                pboxes.c.id == paybox_id,
                                pboxes.c.cashbox == cashbox_id,
                            )
                        )
                        .values({"balance": new_balance, "updated_at": now_ts})
                    )

            # Если платеж уже был проведён и остаётся проведённым, но сумма изменилась —
            # корректируем баланс на разницу (чтобы не было дрейфа)
            if (
                _should_affect_balance()
                and old_status
                and should_post
                and amount != old_amount
            ):
                delta = -(
                    amount - old_amount
                )  # было списано old_amount, стало нужно списать amount
                new_balance = round(paybox_balance + delta, 2)
                await database.execute(
                    pboxes.update()
                    .where(
                        and_(
                            pboxes.c.id == paybox_id,
                            pboxes.c.cashbox == cashbox_id,
                        )
                    )
                    .values({"balance": new_balance, "updated_at": now_ts})
                )

            print(
                f"[purchase-auto-expense] OK (updated) purchase={purchase_id} payment={existing['id']}"
            )
            return

        payment_dict = {
            "type": "outgoing",
            "name": None,
            "external_id": external_id,
            "article": None,
            "project_id": None,
            "article_id": None,
            "tags": "",
            "amount": amount,
            "amount_without_tax": amount,
            "description": purchase["comment"],
            "date": int(purchase["dated"]),
            "repeat_freq": None,
            "parent_id": None,
            "repeat_parent_id": None,
            "repeat_period": None,
            "repeat_first": None,
            "repeat_last": None,
            "repeat_number": None,
            "repeat_day": None,
            "repeat_month": None,
            "repeat_seconds": None,
            "repeat_weekday": None,
            "stopped": False,
            "status": should_post,
            "tax": None,
            "tax_type": None,
            "deb_cred": False,
            "raspilen": False,
            "contragent": purchase["contragent"],
            "cashbox": cashbox_id,
            "paybox": paybox_id,
            "paybox_to": None,
            "account": account_id,
            "is_deleted": False,
            "cheque": None,
            "docs_sales_id": None,
            "contract_id": purchase["contract"],
            "docs_purchases_id": purchase_id,
            "created_at": now_ts,
            "updated_at": now_ts,
        }

        pay_id = await database.execute(payments.insert(values=payment_dict))

        if should_post and _should_affect_balance():
            new_balance = round(paybox_balance - amount, 2)
            await database.execute(
                pboxes.update()
                .where(and_(pboxes.c.id == paybox_id, pboxes.c.cashbox == cashbox_id))
                .values({"balance": new_balance, "updated_at": now_ts})
            )

        print(f"[purchase-auto-expense] OK purchase={purchase_id} payment={pay_id}")
        # ---- end AUTO PAYMENT EXPENSE ----
