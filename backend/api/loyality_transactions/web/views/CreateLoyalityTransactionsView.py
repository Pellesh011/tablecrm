import asyncio
from datetime import datetime
from typing import List, Union

from api.loyality_transactions import schemas
from api.loyality_transactions.routers import raschet_bonuses
from database.db import database, loyality_cards, loyality_transactions
from fastapi import HTTPException
from functions.helpers import (
    clear_phone_number,
    datetime_to_timestamp,
    get_user_by_token,
)
from sqlalchemy import and_, select
from ws_manager import manager


class CreateLoyalityTransactionsView:

    async def __call__(
        self,
        token: str,
        loyality_transaction_data: Union[
            schemas.LoyalityTransactionCreate, List[schemas.LoyalityTransactionCreate]
        ],
    ):
        user = await get_user_by_token(token)

        items: List[schemas.LoyalityTransactionCreate] = (
            loyality_transaction_data
            if isinstance(loyality_transaction_data, list)
            else [loyality_transaction_data]
        )

        prepared, card_numbers_set = [], set()
        for itm in items:
            num = self._normalize_card_number(itm.loyality_card_number)
            if num is not None:
                card_numbers_set.add(num)
            prepared.append({"raw": itm, "number": num})

        cards_rows = await database.fetch_all(
            select(loyality_cards).where(
                and_(
                    loyality_cards.c.card_number.in_(card_numbers_set),
                    loyality_cards.c.cashbox_id == user.cashbox_id,
                    loyality_cards.c.is_deleted.is_(False),
                )
            )
        )
        cards_map = {row.card_number: row for row in cards_rows}

        errors = []
        for p in prepared:
            card = cards_map.get(p["number"])
            if not card:
                errors.append(f"Карта {p['raw'].loyality_card_number} не найдена")
            elif not card.status_card:
                errors.append(f"Карта {p['raw'].loyality_card_number} заблокирована")
        if errors:
            raise HTTPException(400, "; ".join(errors))

        insert_values = []
        balance_delta = {}

        for p in prepared:
            payload = p["raw"].dict()
            card = cards_map[p["number"]]

            if payload.get("preamount") and payload.get("percentamount"):
                payload["amount"] = round(
                    float(payload["preamount"]) * float(payload["percentamount"]), 2
                )
            payload.pop("preamount", None)
            payload.pop("percentamount", None)

            if payload.get("dated"):
                payload["dated"] = datetime.fromtimestamp(payload["dated"])
            else:
                payload["dated"] = datetime.now().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

            payload.update(
                loyality_card_id=card.id,
                loyality_card_number=p["number"],
                card_balance=card.balance,
                created_by_id=user.id,
                cashbox=user.cashbox_id,
            )

            insert_values.append(payload)

            amt = float(payload["amount"])
            delta = amt if payload["type"] == "accrual" else -amt
            balance_delta[card.id] = balance_delta.get(card.id, 0.0) + delta

        query = (
            loyality_transactions.insert()
            .values(insert_values)
            .returning(loyality_transactions.c.id)
        )

        ids = await database.fetch_all(query=query)

        if balance_delta:
            update_query = """
                    UPDATE loyality_cards
                    SET balance = balance + :delta
                    WHERE id = :id
                """
            await database.execute_many(
                query=update_query,
                values=[
                    {"id": cid, "delta": round(d, 2)}
                    for cid, d in balance_delta.items()
                ],
            )

        lt_rows = await database.fetch_all(
            select(loyality_transactions).where(
                loyality_transactions.c.id.in_([k.id for k in ids])
            )
        )
        lt_rows_ts = [datetime_to_timestamp(r) for r in lt_rows]

        for row in lt_rows_ts:
            await manager.send_message(
                token,
                {"action": "create", "target": "loyality_transactions", "result": row},
            )
        for card_id in balance_delta:
            asyncio.create_task(raschet_bonuses(card_id))

        def with_success(payload: dict) -> dict:
            return {**payload, "data": {"status": "success"}}

        if isinstance(loyality_transaction_data, list):
            return [with_success(r) for r in lt_rows_ts]

        return with_success(lt_rows_ts[0]) if lt_rows_ts else None

    @staticmethod
    def _normalize_card_number(card_number) -> Union[int, None]:
        cleaned = clear_phone_number(card_number)
        if cleaned is None:
            return None
        if isinstance(cleaned, int):
            return cleaned
        if isinstance(cleaned, str) and cleaned.isdigit():
            try:
                return int(cleaned)
            except ValueError:
                return None
        return None
