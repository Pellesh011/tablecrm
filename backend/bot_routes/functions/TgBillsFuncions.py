from aiogram.dispatcher.fsm.context import FSMContext
from database.db import cboxes, database, pboxes, tochka_bank_accounts, users
from sqlalchemy import select
from sqlalchemy.orm import aliased


async def get_tochka_bank_accounts_by_chat_owner(chat_owner_id: int):
    query = (
        select([tochka_bank_accounts])
        .select_from(tochka_bank_accounts)
        .join(pboxes, tochka_bank_accounts.c.payboxes_id == pboxes.c.id)
        .join(cboxes, pboxes.c.cashbox == cboxes.c.id)
        .join(users, cboxes.c.admin == users.c.id)
        .where(cboxes.c.admin == chat_owner_id)
    )
    return await database.fetch_all(query)


async def get_user_from_db_by_username(username: str):
    query = users.select().where(users.c.username == username)
    return await database.fetch_one(query)


async def get_chat_owner(chat_id: str):
    u1 = aliased(users)
    u2 = aliased(users)

    query = (
        select(u2.c.id)
        .where(
            u1.c.chat_id == chat_id,
            u1.c.owner_id == u2.c.owner_id,
            u2.c.chat_id == u2.c.owner_id,
        )
        .select_from(u1, u2)
    )
    result = await database.fetch_one(query)
    return result


async def get_user_from_db(user_id: str):
    """Fetches a user from the database based on user_id."""
    query = users.select().where(users.c.chat_id == user_id)
    return await database.fetch_one(query)


async def delete_previous_bill_message(bot, chat_id, state: FSMContext):
    """Deletes the previous message related to a bill, if it exists."""
    data = await state.get_data()
    last_message_id = data.get("last_bill_message_id")

    if last_message_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=last_message_id)
        except Exception as e:
            print(f"Error deleting message: {e}")
