from datetime import datetime, timedelta
from typing import List

from api.loyality_transactions.routers import raschet_bonuses
from common.decorators import ensure_db_connection
from database.db import database, loyality_cards, loyality_transactions
from databases.backends.postgres import Record
from sqlalchemy import and_, select, text


class AutoBurn:
    def __init__(self, card: Record) -> None:
        self.card: Record = card
        self.card_balance: float = card.balance
        self.expired_accruals: List[dict] = []
        self.burned_list: List[int] = []
        self.autoburn_operation_list: List[dict] = []

    @staticmethod
    async def get_cards() -> List[Record]:
        """Получаем только те карты, у которых есть истёкшие начисления"""

        # Подзапрос для нахождения карт с истёкшими начислениями
        expired_subquery = (
            select(loyality_transactions.c.loyality_card_id)
            .select_from(
                loyality_transactions.join(
                    loyality_cards,
                    loyality_cards.c.id == loyality_transactions.c.loyality_card_id,
                )
            )
            .where(
                and_(
                    loyality_transactions.c.type == "accrual",
                    loyality_transactions.c.amount > 0,
                    loyality_transactions.c.autoburned == False,
                    # Сравниваем created_at + lifetime с текущим временем
                    # Используем выражение INTERVAL
                    loyality_transactions.c.created_at
                    + text("INTERVAL '1 second' * loyality_cards.lifetime")
                    < datetime.utcnow(),
                )
            )
            .distinct()
        )

        # Основной запрос
        cards_query = loyality_cards.select().where(
            and_(
                loyality_cards.c.balance > 0,
                loyality_cards.c.lifetime.is_not(None),
                loyality_cards.c.lifetime > 0,
                loyality_cards.c.is_deleted == False,
                loyality_cards.c.id.in_(expired_subquery),
            )
        )

        return await database.fetch_all(cards_query)

    async def _get_expired_accruals(self) -> None:
        """Получаем все истёкшие начисления по карте"""
        expiry_threshold = datetime.utcnow() - timedelta(seconds=self.card.lifetime)

        query = (
            loyality_transactions.select()
            .where(
                and_(
                    loyality_transactions.c.loyality_card_id == self.card.id,
                    loyality_transactions.c.type == "accrual",
                    loyality_transactions.c.amount > 0,
                    loyality_transactions.c.autoburned == False,
                    loyality_transactions.c.created_at <= expiry_threshold,
                )
            )
            .order_by(loyality_transactions.c.created_at)
        )

        expired_transactions = await database.fetch_all(query)
        self.expired_accruals = [dict(tx) for tx in expired_transactions]

    @database.transaction()
    async def _burn(self) -> None:
        """Выполняем автосгорание"""
        if not self.burned_list:
            return

        # Помечаем транзакции как сожженные
        update_query = (
            loyality_transactions.update()
            .where(loyality_transactions.c.id.in_(self.burned_list))
            .values({"autoburned": True})
        )
        await database.execute(update_query)

        # Создаем транзакции автосгорания
        if self.autoburn_operation_list:
            insert_query = loyality_transactions.insert()
            await database.execute_many(insert_query, self.autoburn_operation_list)

    def _get_autoburned_operation_dict(self, amount: float, original_tx: dict) -> dict:
        return {
            "type": "withdraw",
            "amount": amount,
            "loyality_card_id": self.card.id,
            "loyality_card_number": self.card.card_number,
            "created_by_id": self.card.created_by_id,
            "cashbox": self.card.cashbox_id,
            "tags": "",
            "name": f"Автосгорание от {original_tx['created_at'].strftime('%d.%m.%Y')} по сумме {original_tx['amount']}",
            "description": None,
            "status": True,
            "external_id": None,
            "cashier_name": None,
            "dead_at": None,
            "is_deleted": False,
            "autoburned": True,
            "card_balance": self.card_balance,
        }

    @database.transaction()
    async def start(self) -> None:
        """Выполняем автосгорание в одной транзакции"""
        print(
            f"[AutoBurn] Обрабатываю карту {self.card.id}, баланс: {self.card_balance}"
        )
        await self._get_expired_accruals()
        total_burned: float = 0.0
        for accrual in self.expired_accruals:
            self.burned_list.append(accrual["id"])
            total_burned += accrual["amount"]
            # Создаем запись об автосгорании
            self.autoburn_operation_list.append(
                self._get_autoburned_operation_dict(accrual["amount"], accrual)
            )
        # Уменьшаем баланс карты
        self.card_balance = max(0.0, self.card_balance - total_burned)
        # Выполняем сжигание
        await self._burn()
        print(
            f"[AutoBurn] Сожжено {len(self.burned_list)} транзакций, новый баланс: {self.card_balance}"
        )


@ensure_db_connection
async def autoburn():
    print("Запуск AutoBurn")
    card_list = await AutoBurn.get_cards()
    for card in card_list:
        try:
            auto_burn = AutoBurn(card=card)
            await auto_burn.start()
            await raschet_bonuses(card.id)
        except Exception as e:
            print(f"Ошибка при обработке карты {card.id}: {e}")
            # Можно добавить логирование в БД
