from datetime import datetime, timedelta
from typing import List, Optional

from database.db import database, employee_shifts, users_cboxes_relation
from sqlalchemy import and_, select

from .schemas import ShiftStatus


async def check_user_on_shift(
    relation_id: int, check_shift_settings: bool = False
) -> bool:
    """
    Проверить, что пользователь на смене (не на перерыве и не завершил смену)

    Args:
        relation_id: ID связи между между пользователем и кассой
        check_shift_settings: Проверять ли настройку shift_work_enabled (по умолчанию False для обратной совместимости)

    Returns:
        bool: True если пользователь на смене, False иначе
    """
    if check_shift_settings:
        user_query = users_cboxes_relation.select().where(
            users_cboxes_relation.c.id == relation_id
        )
        user_settings = await database.fetch_one(user_query)

        if not user_settings or not user_settings.shift_work_enabled:
            return False

    query = employee_shifts.select().where(
        and_(
            employee_shifts.c.user_id == relation_id,
            employee_shifts.c.status == ShiftStatus.on_shift,
            employee_shifts.c.shift_end.is_(None),
        )
    )

    shift = await database.fetch_one(query)
    return shift is not None


async def get_available_workers_on_shift(
    cashbox_id: int, role_filter: str = None, check_shift_settings: bool = True
) -> List[int]:
    """
    Получить список ID сотрудников, которые сейчас на смене

    Args:
        cashbox_id: ID кассы
        role_filter: Фильтр по роли ("picker", "courier", или None для всех)
        check_shift_settings: Проверять ли настройку shift_work_enabled (по умолчанию True)

    Returns:
        List[int]: Список ID пользователей на смене
    """
    query = (
        select([users_cboxes_relation.c.id])
        .select_from(
            users_cboxes_relation.join(
                employee_shifts, users_cboxes_relation.c.id == employee_shifts.c.user_id
            )
        )
        .where(
            and_(
                users_cboxes_relation.c.cashbox_id == cashbox_id,
                employee_shifts.c.status == ShiftStatus.on_shift,
                employee_shifts.c.shift_end.is_(None),
            )
        )
    )

    if check_shift_settings:
        query = query.where(users_cboxes_relation.c.shift_work_enabled == True)

    if role_filter:
        # Правильный способ фильтрации по массиву в PostgreSQL
        query = query.where(users_cboxes_relation.c.tags.any(role_filter))

    workers = await database.fetch_all(query)
    return [worker.id for worker in workers]


async def get_available_pickers_on_shift(
    cashbox_id: int, check_shift_settings: bool = True
) -> List[int]:
    """
    Получить список ID сборщиков на смене

    Args:
        cashbox_id: ID кассы
        check_shift_settings: Проверять ли настройку shift_work_enabled (по умолчанию True)

    Returns:
        List[int]: Список ID сборщиков на смене
    """
    return await get_available_workers_on_shift(
        cashbox_id, "picker", check_shift_settings
    )


async def get_available_couriers_on_shift(
    cashbox_id: int, check_shift_settings: bool = True
) -> List[int]:
    """
    Получить список ID курьеров на смене

    Args:
        cashbox_id: ID кассы
        check_shift_settings: Проверять ли настройку shift_work_enabled (по умолчанию True)

    Returns:
        List[int]: Список ID курьеров на смене
    """
    return await get_available_workers_on_shift(
        cashbox_id, "courier", check_shift_settings
    )


async def auto_end_expired_breaks():
    """
    Автоматически завершить истекшие перерывы
    Эта функция может вызываться периодически (например, через Celery)
    """
    current_time = datetime.utcnow()

    query = employee_shifts.select().where(
        and_(
            employee_shifts.c.status == ShiftStatus.on_break,
            employee_shifts.c.break_start.is_not(None),
            employee_shifts.c.break_duration.is_not(None),
            employee_shifts.c.shift_end.is_(None),
        )
    )

    active_breaks = await database.fetch_all(query)

    expired_shift_ids = []
    for shift in active_breaks:
        break_end_time = shift.break_start + timedelta(minutes=shift.break_duration)
        if current_time >= break_end_time:
            expired_shift_ids.append(shift.id)

    if expired_shift_ids:
        update_data = {
            "status": ShiftStatus.on_shift,
            "break_start": None,
            "break_duration": None,
            "updated_at": current_time,
        }

        await database.execute(
            employee_shifts.update()
            .where(employee_shifts.c.id.in_(expired_shift_ids))
            .values(update_data)
        )

        print(f"Автоматически завершено {len(expired_shift_ids)} перерывов")


async def get_user_shift_info(user_id: int) -> Optional[dict]:
    """
    Получить информацию о текущей смене пользователя

    Args:
        user_id: ID пользователя

    Returns:
        dict или None: Информация о смене
    """
    query = (
        employee_shifts.select()
        .where(
            and_(
                employee_shifts.c.user_id == user_id,
                employee_shifts.c.shift_end.is_(None),
            )
        )
        .order_by(employee_shifts.c.created_at.desc())
    )

    shift = await database.fetch_one(query)
    return dict(shift) if shift else None
