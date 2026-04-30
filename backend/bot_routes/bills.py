import logging
from datetime import datetime

import pytz
from aiogram import F, Router, types
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.fsm.context import FSMContext
from bot_routes.functions.callbacks import (
    bills_callback,
    change_payment_date_bill_callback,
    create_select_account_payment_callback,
)
from bot_routes.functions.keyboards import *
from bot_routes.functions.TgBillsFuncions import (
    get_chat_owner,
    get_tochka_bank_accounts_by_chat_owner,
)
from bot_routes.repositories.impl.TgBillApproversRepository import (
    TgBillApproversRepository,
)
from bot_routes.repositories.impl.TgBillsRepository import TgBillsRepository
from bot_routes.services.TgBillApproversService import TgBillApproversService
from bot_routes.services.TgBillsService import TgBillsService, TgBillsUpdateModel
from database.db import (
    database,
    tg_bot_bill_approvers,
    tg_bot_bills,
    tg_bot_settings,
    tochka_bank_accounts,
    users,
)
from sqlalchemy import select, update

logging.basicConfig(level=logging.ERROR)

timezone = pytz.timezone("Europe/Moscow")

is_show_parse_bill_log = True


async def select_bot_setting(tg_account_id: int, setting: str):

    query = select(tg_bot_settings).where(
        tg_bot_settings.c.tg_account_id == tg_account_id
    )
    existing = await database.fetch_one(query)

    if not existing:
        default_settings = {
            "is_show_parse_bill_log": False,
        }
        insert_query = tg_bot_settings.insert().values(
            tg_account_id=tg_account_id, settings=default_settings
        )
        await database.execute(insert_query)
        return default_settings.get(setting, None)

    settings = existing["settings"] or {}
    return settings.get(setting, None)


async def update_bot_setting(tg_account_id: int, setting: str, value):

    query = select(tg_bot_settings).where(
        tg_bot_settings.c.tg_account_id == tg_account_id
    )
    existing = await database.fetch_one(query)

    if not existing:
        return False

    current_settings = existing["settings"] or {}
    current_settings[setting] = value

    update_query = (
        update(tg_bot_settings)
        .where(tg_bot_settings.c.tg_account_id == tg_account_id)
        .values(settings=current_settings)
    )
    await database.execute(update_query)
    return True


class Form(StatesGroup):
    year = State()
    month = State()
    day = State()


# Определение состояний для FSM
class BillDateForm(StatesGroup):
    start = State()
    waiting_for_date = State()


# Функция для получения маршрута
def get_bill_route(bot, s3_client):

    pdf_router = Router()
    tg_bill_repository = TgBillsRepository(database, tg_bot_bills, tochka_bank_accounts)
    tg_bill_approvers_repository = TgBillApproversRepository(
        database, tg_bot_bill_approvers, users
    )
    tg_bill_service = TgBillsService(
        tg_bill_repository,
        tg_bill_approvers_repository,
        s3_client,
        s3_bucket_name="tg-bills",
    )
    tg_bill_approvers_service = TgBillApproversService(tg_bill_approvers_repository)

    @pdf_router.callback_query_handler(
        lambda c: c.data.startswith("year_"), state=Form.year
    )
    async def process_year(callback_query: types.CallbackQuery, state: FSMContext):
        year = int(callback_query.data.split("_")[1])
        await state.update_data(year=year)
        data = await state.get_data()
        bill_id = data["bill_id"]
        inline_keyboard = []
        month_buttons = [
            types.InlineKeyboardButton(
                text=str(month), callback_data=f"month_{year}_{month}"
            )
            for month in range(1, 13)
        ]

        # Split the buttons into rows of 4
        for i in range(0, len(month_buttons), 4):
            inline_keyboard.append(month_buttons[i : i + 4])

        keyboard = types.InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(
            callback_query.message.chat.id,
            f"Счёт № {bill_id}. Выберите месяц:",
            reply_markup=keyboard,
        )
        await state.set_state(Form.month)

    # Обработка выбора месяца
    @pdf_router.callback_query_handler(
        lambda c: c.data.startswith("month_"), state=Form.month
    )
    async def process_month(callback_query: types.CallbackQuery, state: FSMContext):
        year, month = map(int, callback_query.data.split("_")[1:])
        await state.update_data(month=month)
        data = await state.get_data()
        bill_id = data["bill_id"]
        # Определение количества дней в месяце
        if month in [4, 6, 9, 11]:
            days_in_month = 30
        elif month == 2:
            data = await state.get_data()
            year = data.get("year")
            days_in_month = (
                29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
            )
        else:
            days_in_month = 31

        inline_keyboard = []
        days_buttons = [
            types.InlineKeyboardButton(
                text=str(day), callback_data=f"day_{year}_{month}_{day}"
            )
            for day in range(1, days_in_month + 1)
        ]

        # Split the buttons into rows of 4
        for i in range(0, len(days_buttons), 4):
            inline_keyboard.append(days_buttons[i : i + 4])

        keyboard = types.InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(
            callback_query.message.chat.id,
            f"Счёт № {bill_id}. Выберите день:",
            reply_markup=keyboard,
        )
        await state.set_state(Form.day)

    # Обработка выбора дня
    @pdf_router.callback_query_handler(
        lambda c: c.data.startswith("day_"), state=Form.day
    )
    async def process_day(callback_query: types.CallbackQuery, state: FSMContext):
        year, month, day = map(int, callback_query.data.split("_")[1:])
        data = await state.get_data()
        bill_id = data["bill_id"]
        tg_id_updated_by = callback_query.from_user.id
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(
            callback_query.message.chat.id, f"Вы выбрали дату: {day}.{month}.{year}"
        )
        await state.set_state(None)
        user_date = f"{year}-{month}-{day}"
        datetime.strptime(user_date, "%Y-%m-%d")

        bill, msg = await tg_bill_service.change_bill_date(
            bill_id, user_date, str(tg_id_updated_by)
        )
        if not bill:
            await callback_query.message.reply(msg)

        await state.set_state(None)
        await callback_query.message.reply(
            msg,
            reply_markup=create_main_menu(bill_id, bill["status"]),
            parse_mode="HTML",
        )

    @pdf_router.callback_query_handler(
        lambda c: create_select_account_payment_callback.filter()(c)
    )
    async def select_account_payment_handler(callback_query: types.CallbackQuery):
        data = create_select_account_payment_callback.parse(callback_query.data)
        tg_id_updated_by = str(callback_query.from_user.id)
        account_id = data["account_id"]
        bill_id = data["bill_id"]
        bill, msg = await tg_bill_service.update_bill(
            bill_id,
            TgBillsUpdateModel(tochka_bank_account_id=account_id),
            tg_id_updated_by,
        )
        if not bill:
            await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
            await bot.answer_callback_query(callback_query.id)
            return

        await bot.send_message(
            chat_id=callback_query.message.chat.id,
            text=msg,
            reply_markup=create_main_menu(bill_id, bill["status"]),
        )
        await bot.answer_callback_query(callback_query.id)

    @pdf_router.callback_query_handler(
        lambda c: change_payment_date_bill_callback.filter()(c)
    )
    async def change_payment_date_handler(
        callback_query: types.CallbackQuery, state: FSMContext = None
    ):
        callback_data = change_payment_date_bill_callback.parse(callback_query.data)
        bill_id = callback_data["bill_id"]
        tg_id_updated_by = str(callback_query.from_user.id)
        if "data" in callback_data and callback_data["data"]:
            datetime.strptime(callback_data["data"], "%Y-%m-%d")
            bill, msg = await tg_bill_service.change_bill_date(
                bill_id, callback_data["data"], str(tg_id_updated_by)
            )
            if not bill:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
            await bot.send_message(
                chat_id=callback_query.message.chat.id,
                text=msg,
                reply_markup=create_main_menu(bill_id, bill["status"]),
            )
            await bot.answer_callback_query(callback_query.id)
        else:
            await state.set_data({"bill_id": bill_id})
            await bot.answer_callback_query(callback_query.id)

            inline_keyboard = []
            year_buttons = [
                types.InlineKeyboardButton(text=str(year), callback_data=f"year_{year}")
                for year in range(2020, 2031)
            ]

            # Split the buttons into rows of 4
            for i in range(0, len(year_buttons), 4):
                inline_keyboard.append(year_buttons[i : i + 4])

            keyboard = types.InlineKeyboardMarkup(inline_keyboard=inline_keyboard)
            await callback_query.message.answer(
                f"Счёт № {bill_id}. Выберите год:", reply_markup=keyboard
            )
            await state.set_state(Form.year)

    @pdf_router.callback_query_handler(lambda c: bills_callback.filter()(c))
    async def bills_callback_handler(callback_query: types.CallbackQuery):
        data = bills_callback.parse(callback_query.data)
        action = data["action"]
        bill_id = data["bill_id"]
        tg_id_updated_by = str(callback_query.from_user.id)
        user, message = await tg_bill_service.check_user_registration(tg_id_updated_by)
        bill = await tg_bill_service.get_bill(bill_id)
        msg = r"Действие не возможно\не нейдено"
        if not user:
            await bot.answer_callback_query(callback_query.id, text=message)
            return
        res, message = await tg_bill_service.check_user_permissions(
            bill_id, tg_id_updated_by
        )
        if not res:
            await bot.answer_callback_query(callback_query.id, text=message)
            return
        if action == "cancel_bill":
            bill, msg = await tg_bill_service.update_bill(
                bill_id,
                TgBillsUpdateModel(status=TgBillStatus.CANCELED),
                tg_id_updated_by,
            )
            if not bill:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
                return
            msg = await tg_bill_service.format_bill_notification(
                tg_id_updated_by=tg_id_updated_by, old_bill=bill, new_bill=bill
            )
        if action == "send_bill":
            res, msg = await tg_bill_service.send_bill(bill_id, tg_id_updated_by)
            if not res:
                await bot.send_message(chat_id=callback_query.message.chat.id, text=msg)
                return
            bill = await tg_bill_service.update_bill_status_based_on_approvals(
                bill_id, approvers
            )
            msg = await tg_bill_service.format_bill_notification(
                tg_id_updated_by=tg_id_updated_by, old_bill=bill, new_bill=bill
            )
        if action == "approve":
            user_permissions, msg = await tg_bill_service.check_user_permissions(
                bill_id, tg_id_updated_by
            )
            if user_permissions:

                approve = await tg_bill_approvers_service.get_approve_by_bill_id_and_approver_id(
                    bill_id, user.id
                )
                if not approve:
                    await bot.send_message(
                        chat_id=callback_query.message.chat.id,
                        text=f"Не достпно для {user.username}, так как не является утверждающим.",
                    )
                    return
                res, msg = await tg_bill_approvers_service.approve(
                    bill_id, tg_id_updated_by
                )
                if not res:
                    await bot.send_message(
                        chat_id=callback_query.message.chat.id, text=msg
                    )
                    return
                approvers = await tg_bill_approvers_service.get_bill_approvers(bill_id)
                bill = await tg_bill_service.update_bill_status_based_on_approvals(
                    bill_id, approvers
                )

                msg = await tg_bill_service.format_bill_notification(
                    tg_id_updated_by=tg_id_updated_by, old_bill=bill, new_bill=bill
                )

        if action == "reject":
            user_permissions, msg = await tg_bill_service.check_user_permissions(
                bill_id, tg_id_updated_by
            )
            if user_permissions:
                approve = await tg_bill_approvers_service.get_approve_by_bill_id_and_approver_id(
                    bill_id, user.id
                )
                if not approve:
                    await bot.send_message(
                        chat_id=callback_query.message.chat.id,
                        text=f"Не достпно для {user.username}, так как не является утверждающим.",
                    )
                    return
                res, msg = await tg_bill_approvers_service.rejet(
                    bill_id, tg_id_updated_by
                )
                if not res:
                    await bot.send_message(
                        chat_id=callback_query.message.chat.id, text=msg
                    )
                    return
                approvers = await tg_bill_approvers_service.get_bill_approvers(bill_id)
                bill = await tg_bill_service.update_bill_status_based_on_approvals(
                    bill_id, approvers
                )
                new_bill = await tg_bill_service.get_bill(bill.id)
                msg = await tg_bill_service.format_bill_notification(
                    tg_id_updated_by=tg_id_updated_by, old_bill=bill, new_bill=new_bill
                )

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(
            chat_id=callback_query.message.chat.id,
            text=msg,
            reply_markup=create_main_menu(bill_id, bill["status"]),
        )

    @pdf_router.message(state=BillDateForm.waiting_for_date)
    async def process_payment_date(message: types.Message, state: FSMContext):
        user_date = message.text
        tg_id_updated_by = message.from_user.id
        try:
            state_data = await state.get_data()
            datetime.strptime(user_date, "%Y-%m-%d")

            bill, msg = await tg_bill_service.change_bill_date(
                state_data["bill_id"], user_date, str(tg_id_updated_by)
            )
            if not bill:
                await message.reply(msg)

            await state.set_state(None)
            await message.reply(
                msg,
                reply_markup=create_main_menu(state_data["bill_id"], bill["status"]),
                parse_mode="HTML",
            )

        except ValueError:
            await message.reply(
                "Пожалуйста, введите дату в правильном формате (ГГГГ-ММ-ДД):"
            )
            return

    @pdf_router.message(
        lambda message: message.text.isdigit(), state=BillDateForm.waiting_for_date
    )
    async def process_invalid_date(message: types.Message):
        await message.reply(
            "Пожалуйста, введите дату в правильном формате (ГГГГ-ММ-ДД):"
        )

    @pdf_router.message(commands="toggle_parse_bill_log")
    async def toggle_parse_bill_log(message: types.Message, state: FSMContext):
        chat_id = message.chat.id
        user = await database.fetch_one(
            users.select().where(
                users.c.chat_id == str(message.chat.id),
                users.c.owner_id == str(message.from_user.id),
            )
        )
        if not user:
            await bot.send_message(
                chat_id=chat_id,
                text="error: Менять настройки чата может только владелец",
            )
            return
        is_show_parse_bill_log = await select_bot_setting(
            user.id, "is_show_parse_bill_log"
        )
        is_show_parse_bill_log = not is_show_parse_bill_log
        await update_bot_setting(
            user.id, "is_show_parse_bill_log", is_show_parse_bill_log
        )
        await message.reply(
            f"Вывод ошибки распознования счёта: {is_show_parse_bill_log}"
        )

    @pdf_router.message(F.document.mime_type == "application/pdf")
    async def handle_pdf(message: types.Message, state: FSMContext):
        chat_id = message.chat.id
        user_id = message.from_user.id

        user = await database.fetch_one(
            users.select().where(
                users.c.chat_id == str(message.chat.id),
                users.c.owner_id == str(message.from_user.id),
            )
        )
        if not user:
            await bot.send_message(
                chat_id=chat_id, text="error: Пользователь не найден"
            )
            return
        is_show_parse_bill_log = await select_bot_setting(
            user.id, "is_show_parse_bill_log"
        )
        try:
            await state.set_data({})

            file_id = message.document.file_id
            file_name = message.document.file_name
            file_info = await bot.get_file(file_id)
            bill, msg = await tg_bill_service.process_and_save_bill(
                file_id, file_name, str(user_id), bot.token, file_info.file_path
            )
            if bill is None:
                await bot.send_message(chat_id=chat_id, text=f"error: {msg}")
                return
            if bill is False and is_show_parse_bill_log is True:
                await bot.send_document(
                    chat_id=chat_id, document=file_id, caption=file_id
                )
                await bot.send_message(
                    chat_id=chat_id, text=f"Ошибка разбора счёта: {msg}"
                )
                return
            if bill is False and is_show_parse_bill_log is False:
                await bot.send_message(chat_id=chat_id, text="Ошибка разбора счёта")
                return
            result, msg = await tg_bill_approvers_service.create_bill_approvers(
                message, bill.id
            )
            if result is False:
                await bot.send_message(chat_id=chat_id, text=f"Счёт разобран: {msg}")
                return
            new_bill = await tg_bill_service.get_bill(bill.id)
            notification_string = await tg_bill_service.format_bill_notification(
                tg_id_updated_by=str(user_id), old_bill=bill, new_bill=new_bill
            )
            chat_owner = await get_chat_owner(str(chat_id))
            if not chat_owner:
                await message.reply(text=f"Не найден владелец чата {chat_id}")
                return
            accounts = await get_tochka_bank_accounts_by_chat_owner(chat_owner["id"])
            if not accounts:
                await message.reply(
                    text=f"У владельца чата с id в системе {chat_owner['id']} нет привязанных счетов в банке. Пожалуйста, свяжитесь с администратором."
                )
                return
            keyboard = create_select_account_payment_keyboard(bill.id, accounts)
            await message.reply(
                notification_string, reply_markup=keyboard, parse_mode="HTML"
            )

        except Exception as e:
            logging.exception("Ошибка при обработке PDF-файла")

    return pdf_router
