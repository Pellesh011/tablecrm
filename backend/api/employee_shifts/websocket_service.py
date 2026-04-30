from datetime import datetime

from common.decorators import ensure_db_connection
from database.db import database, employee_shifts, users_cboxes_relation
from sqlalchemy import and_, select
from ws_manager import manager

from .schemas import ShiftStatus


def serialize_datetime_fields(data: dict) -> dict:
    """
    Преобразует все datetime поля в строки для JSON сериализации
    """
    serialized = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized


@ensure_db_connection
async def send_shift_time_updates():
    """
    Отправляет обновления времени для всех активных смен
    Вызывается периодически (например, каждую минуту)
    """
    try:
        # Получаем все активные смены
        query = (
            select(
                [
                    employee_shifts.c.id,
                    employee_shifts.c.user_id,
                    employee_shifts.c.cashbox_id,
                    employee_shifts.c.shift_start,
                    employee_shifts.c.status,
                    employee_shifts.c.break_start,
                    employee_shifts.c.break_duration,
                    users_cboxes_relation.c.token,
                ]
            )
            .select_from(
                employee_shifts.join(
                    users_cboxes_relation,
                    employee_shifts.c.user_id == users_cboxes_relation.c.id,
                )
            )
            .where(
                and_(
                    employee_shifts.c.shift_end.is_(None),
                    employee_shifts.c.status.in_(
                        [ShiftStatus.on_shift, ShiftStatus.on_break]
                    ),
                )
            )
        )

        active_shifts = await database.fetch_all(query)

        for shift in active_shifts:
            # Вычисляем длительность смены
            shift_duration = datetime.utcnow() - shift.shift_start
            shift_duration_minutes = int(shift_duration.total_seconds() / 60)

            # Данные для отправки
            update_data = {
                "action": "shift_time_update",
                "target": "employee_shifts",
                "result": {
                    "user_id": shift.user_id,
                    "shift_duration_minutes": shift_duration_minutes,
                    "status": shift.status,
                    "cashbox_id": shift.cashbox_id,
                    "shift_start": (
                        shift.shift_start.isoformat() if shift.shift_start else None
                    ),
                    "break_start": (
                        shift.break_start.isoformat() if shift.break_start else None
                    ),
                    "break_duration": shift.break_duration,
                },
                "timestamp": datetime.utcnow().isoformat(),
            }

            # Отправляем обновление пользователю
            if shift.token:
                await manager.send_message(shift.token, update_data)

        if active_shifts:
            print(f"Sent time updates for {len(active_shifts)} active shifts")

    except Exception as e:
        print(f"Error sending shift time updates: {e}")


async def send_shift_update_to_admins(cashbox_id: int, shift_data: dict, action: str):
    """
    Отправляет обновления о сменах всем администраторам кассы

    Args:
        cashbox_id: ID кассы
        shift_data: Данные о смене
        action: Тип действия (start_shift, end_shift, start_break, end_break)
    """
    try:
        # Сериализуем datetime поля
        serialized_data = serialize_datetime_fields(shift_data)

        # Находим всех администраторов кассы
        admin_query = select([users_cboxes_relation.c.token]).where(
            and_(
                users_cboxes_relation.c.cashbox_id == cashbox_id,
                users_cboxes_relation.c.is_owner
                == True,  # Или другое условие для админов
            )
        )

        admins = await database.fetch_all(admin_query)

        # Отправляем уведомление всем админам
        for admin in admins:
            if admin.token:
                await manager.send_message(
                    admin.token,
                    {
                        "action": f"admin_{action}",
                        "target": "employee_shifts",
                        "result": serialized_data,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )

    except Exception as e:
        print(f"Error sending shift update to admins: {e}")


async def send_statistics_update(cashbox_id: int):
    """
    Отправляет обновленную статистику по сменам администраторам

    Args:
        cashbox_id: ID кассы
    """
    try:
        from sqlalchemy import func

        # Подсчитываем людей на смене
        on_shift_query = (
            select([func.count(employee_shifts.c.id)])
            .select_from(
                employee_shifts.join(
                    users_cboxes_relation,
                    employee_shifts.c.user_id == users_cboxes_relation.c.id,
                )
            )
            .where(
                and_(
                    users_cboxes_relation.c.cashbox_id == cashbox_id,
                    employee_shifts.c.shift_end.is_(None),
                    employee_shifts.c.status == "on_shift",
                )
            )
        )

        # Подсчитываем людей на перерыве
        on_break_query = (
            select([func.count(employee_shifts.c.id)])
            .select_from(
                employee_shifts.join(
                    users_cboxes_relation,
                    employee_shifts.c.user_id == users_cboxes_relation.c.id,
                )
            )
            .where(
                and_(
                    users_cboxes_relation.c.cashbox_id == cashbox_id,
                    employee_shifts.c.shift_end.is_(None),
                    employee_shifts.c.status == "on_break",
                )
            )
        )

        on_shift_count = await database.fetch_val(on_shift_query) or 0
        on_break_count = await database.fetch_val(on_break_query) or 0

        # Пользователи с включенными сменами
        shift_enabled_query = select([func.count(users_cboxes_relation.c.id)]).where(
            and_(
                users_cboxes_relation.c.cashbox_id == cashbox_id,
                users_cboxes_relation.c.shift_work_enabled == True,
            )
        )

        shift_enabled_count = await database.fetch_val(shift_enabled_query) or 0

        statistics_data = {
            "on_shift_count": int(on_shift_count),
            "on_break_count": int(on_break_count),
            "total_active": int(on_shift_count + on_break_count),
            "shift_enabled_users": int(shift_enabled_count),
        }

        # Находим всех администраторов кассы
        admin_query = select([users_cboxes_relation.c.token]).where(
            and_(
                users_cboxes_relation.c.cashbox_id == cashbox_id,
                users_cboxes_relation.c.is_owner == True,
            )
        )

        admins = await database.fetch_all(admin_query)

        # Отправляем статистику всем админам
        for admin in admins:
            if admin.token:
                await manager.send_message(
                    admin.token,
                    {
                        "action": "shift_statistics_update",
                        "target": "employee_shifts",
                        "result": statistics_data,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )

    except Exception as e:
        print(f"Error sending statistics update: {e}")
