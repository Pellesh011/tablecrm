from datetime import datetime

from database.db import database, loyality_cards, loyality_transactions
from sqlalchemy import select


class PromocodeService:
    @staticmethod
    async def apply_promo_bonus(
        card_id: int,
        card_number: int,
        amount: float,
        promo_id: int,
        cashbox_id: int,
        user_id: int,
    ) -> int:
        """
        Начисление баллов по промокоду.
        Выполняет бизнес-логику:
            - Проверка существования карты
            - Обновление баланса и дохода карты
            - Создание записи транзакции
        """
        now = datetime.now()

        # Поиск карты лояльности
        card_query = select(loyality_cards).where(loyality_cards.c.id == card_id)
        card = await database.fetch_one(card_query)

        if not card:
            raise ValueError("Карта лояльности не найдена")

        # Расчет новых показателей
        new_balance = (card.balance or 0) + amount
        new_income = (card.income or 0) + amount

        # Обновление карты
        await database.execute(
            loyality_cards.update()
            .where(loyality_cards.c.id == card_id)
            .values({"balance": new_balance, "income": new_income, "updated_at": now})
        )

        # Создание транзакции
        transaction_query = loyality_transactions.insert().values(
            {
                "type": "accrual",
                "amount": amount,
                "loyality_card_id": card_id,
                "loyality_card_number": card_number,
                "created_by_id": user_id,
                "card_balance": new_balance,
                "cashbox": cashbox_id,
                "name": "Активация промокода",
                "description": "Начисление баллов",
                "status": True,
                "external_id": str(promo_id),
                "is_deleted": False,
                "created_at": now,
                "updated_at": now,
            }
        )
        transaction_id = await database.execute(transaction_query)

        return transaction_id


promocode_service = PromocodeService()
