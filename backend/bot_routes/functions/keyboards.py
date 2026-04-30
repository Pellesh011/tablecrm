from datetime import datetime
from typing import Any, List

from aiogram import types
from bot_routes.functions.callbacks import (
    bills_callback,
    change_payment_date_bill_callback,
    create_select_account_payment_callback,
)
from database.db import TgBillStatus


def create_select_account_payment_keyboard(
    bill_id: int, accounts: List[Any]
) -> types.InlineKeyboardMarkup:
    keyboard_buttons = [
        [
            types.InlineKeyboardButton(
                text=str(account.accountId),
                callback_data=create_select_account_payment_callback.new(
                    account_id=account.id, bill_id=bill_id
                ),
            )
        ]
        for account in accounts
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)


def create_main_menu(bill_id: int, status: TgBillStatus) -> types.InlineKeyboardMarkup:
    today = datetime.now().strftime("%Y-%m-%d")

    if status == TgBillStatus.CANCELED:
        return None

    keyboard_buttons: List[List[types.InlineKeyboardButton]] = []

    if status == TgBillStatus.NEW:
        keyboard_buttons.extend(
            [
                [
                    types.InlineKeyboardButton(
                        text="Сместить дату",
                        callback_data=change_payment_date_bill_callback.new(
                            action="change_date", data=None, bill_id=bill_id
                        ),
                    )
                ],
                [
                    types.InlineKeyboardButton(
                        text="Добавить сегодняшним числом",
                        callback_data=change_payment_date_bill_callback.new(
                            action="change_date", bill_id=bill_id, data=today
                        ),
                    )
                ],
            ]
        )

    elif status == TgBillStatus.WAITING_FOR_APPROVAL:
        keyboard_buttons.append(
            [
                types.InlineKeyboardButton(
                    text="👍 Like",
                    callback_data=bills_callback.new(action="approve", bill_id=bill_id),
                ),
                types.InlineKeyboardButton(
                    text="👎 Dislike",
                    callback_data=bills_callback.new(action="reject", bill_id=bill_id),
                ),
            ]
        )

    elif status == TgBillStatus.APPROVED:
        keyboard_buttons.append(
            [
                types.InlineKeyboardButton(
                    text="Отправить в банк",
                    callback_data=bills_callback.new(
                        action="send_bill", bill_id=bill_id
                    ),
                )
            ]
        )

    elif status == TgBillStatus.ERROR:
        keyboard_buttons.extend(
            [
                [
                    types.InlineKeyboardButton(
                        text="Редактировать",
                        callback_data=bills_callback.new(
                            action="edit_bill", bill_id=bill_id
                        ),
                    )
                ],
                [
                    types.InlineKeyboardButton(
                        text="Отправить",
                        callback_data=bills_callback.new(
                            action="send_bill", bill_id=bill_id
                        ),
                    )
                ],
            ]
        )

    elif status == TgBillStatus.REQUESTED:
        keyboard_buttons.append(
            [
                types.InlineKeyboardButton(
                    text="Проверить статус (неактивно)",
                    callback_data=bills_callback.new(
                        action="check_bill", bill_id=bill_id
                    ),
                )
            ]
        )

    if status != TgBillStatus.CANCELED:
        keyboard_buttons.append(
            [
                types.InlineKeyboardButton(
                    text="Отменить платёж",
                    callback_data=bills_callback.new(
                        action="cancel_bill", bill_id=bill_id
                    ),
                )
            ]
        )
    if len(keyboard_buttons) == 0:
        None
    else:
        return types.InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
