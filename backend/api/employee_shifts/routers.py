from datetime import datetime, timedelta

from api.users.schemas import (
    CBUsersListShortWithShifts,
    ShiftStatistics,
    UserWithShiftInfo,
)
from database.db import (
    database,
    employee_shifts,
    employee_shifts_events,
    users,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException
from sqlalchemy import and_, desc, func, or_, select
from ws_manager import manager

from .schemas import (
    ShiftData,
    ShiftEvent,
    ShiftEventCreate,
    ShiftEventsList,
    ShiftResponse,
    ShiftStatus,
    ShiftStatusResponse,
)
from .websocket_service import send_shift_update_to_admins, send_statistics_update

router = APIRouter(prefix="/employee-shifts", tags=["employee_shifts"])


def serialize_shift_data(shift_data: dict) -> dict:
    """Преобразует datetime поля в строки для JSON сериализации"""
    serialized = {}
    for key, value in shift_data.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized


async def log_shift_event(relation_id: int, cashbox_id: int, status: ShiftStatus):
    """Логируем событие смены по relation_id (users_cboxes_relation.id)."""
    query = employee_shifts_events.insert(
        ShiftEventCreate(
            relation_id=relation_id,
            cashbox_id=cashbox_id,
            shift_status=status,
            event_start=datetime.now(),
        ).dict()
    )
    await database.execute(query)


@router.post("/start", response_model=ShiftResponse)
async def start_shift(token: str):
    """Начать смену"""

    # Получаем пользователя по токену
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    if not user.shift_work_enabled:
        raise HTTPException(
            status_code=400,
            detail="Работа по сменам отключена для данного пользователя",
        )

    # Проверяем, нет ли уже активной смены
    existing_shift = await database.fetch_one(
        employee_shifts.select().where(
            and_(
                employee_shifts.c.user_id == user.id,
                employee_shifts.c.shift_end.is_(None),
            )
        )
    )

    if existing_shift:
        raise HTTPException(status_code=400, detail="Смена уже начата")

    now = datetime.utcnow()
    shift_data = ShiftData(
        user_id=user.id,
        cashbox_id=user.cashbox_id,
        shift_start=now,
        shift_end=None,
        status=ShiftStatus.on_shift,
        break_start=None,
        break_duration=None,
        created_at=now,
        updated_at=now,
    ).dict()

    new_shift = await database.fetch_one(
        employee_shifts.insert()
        .values(shift_data)
        .returning(
            employee_shifts.c.id,
            employee_shifts.c.user_id,
            employee_shifts.c.cashbox_id,
            employee_shifts.c.shift_start,
            employee_shifts.c.shift_end,
            employee_shifts.c.status,
            employee_shifts.c.break_start,
            employee_shifts.c.break_duration,
            employee_shifts.c.created_at,
            employee_shifts.c.updated_at,
        )
    )

    new_shift_dict = dict(new_shift)
    # В БД employee_shifts.user_id хранится relation_id, а наружу хотим отдавать users.id
    new_shift_dict["user_id"] = user.user
    shift_response = ShiftResponse(**new_shift_dict)
    shift_response_dict = shift_response.dict()
    serialized_data = serialize_shift_data(shift_response_dict)

    await log_shift_event(
        relation_id=new_shift.user_id,
        cashbox_id=new_shift.cashbox_id,
        status=new_shift.status,
    )

    # Отправляем веб-сокет уведомление пользователю
    await manager.send_message(
        token,
        {
            "action": "start_shift",
            "target": "employee_shifts",
            "result": serialized_data,
            "user_id": user.user,
            "cashbox_id": user.cashbox_id,
        },
    )

    # Отправляем уведомление администраторам
    await send_shift_update_to_admins(
        cashbox_id=user.cashbox_id, shift_data=serialized_data, action="start_shift"
    )

    # Обновляем статистику для админов
    await send_statistics_update(user.cashbox_id)

    return shift_response


@router.post("/end", response_model=ShiftResponse)
async def end_shift(token: str):
    """Завершить смену"""

    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    # Находим активную смену
    active_shift = await database.fetch_one(
        employee_shifts.select().where(
            and_(
                employee_shifts.c.user_id == user.id,
                employee_shifts.c.shift_end.is_(None),
            )
        )
    )

    if not active_shift:
        raise HTTPException(status_code=400, detail="Активная смена не найдена")

    now = datetime.utcnow()
    updated_shift = await database.fetch_one(
        employee_shifts.update()
        .where(employee_shifts.c.id == active_shift.id)
        .values(
            shift_end=now,
            status=ShiftStatus.off_shift,
            break_start=None,
            break_duration=None,
            updated_at=now,
        )
        .returning(
            employee_shifts.c.id,
            employee_shifts.c.user_id,
            employee_shifts.c.cashbox_id,
            employee_shifts.c.shift_start,
            employee_shifts.c.shift_end,
            employee_shifts.c.status,
            employee_shifts.c.break_start,
            employee_shifts.c.break_duration,
            employee_shifts.c.created_at,
            employee_shifts.c.updated_at,
        )
    )

    updated_shift_dict = dict(updated_shift)
    updated_shift_dict["user_id"] = user.user
    shift_response = ShiftResponse(**updated_shift_dict)
    shift_response_dict = shift_response.dict()
    serialized_data = serialize_shift_data(shift_response_dict)

    await log_shift_event(
        relation_id=updated_shift.user_id,
        cashbox_id=updated_shift.cashbox_id,
        status=updated_shift.status,
    )

    # Отправляем веб-сокет уведомление пользователю
    await manager.send_message(
        token,
        {
            "action": "end_shift",
            "target": "employee_shifts",
            "result": serialized_data,
            "user_id": user.user,
            "cashbox_id": user.cashbox_id,
        },
    )

    # Отправляем уведомление администраторам
    await send_shift_update_to_admins(
        cashbox_id=user.cashbox_id, shift_data=serialized_data, action="end_shift"
    )

    # Обновляем статистику для админов
    await send_statistics_update(user.cashbox_id)

    return shift_response


@router.post("/break", response_model=ShiftResponse)
async def create_break(token: str, duration_minutes: int):
    """Создать перерыв"""

    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    # Проверяем, что пользователь на смене
    active_shift = await database.fetch_one(
        employee_shifts.select().where(
            and_(
                employee_shifts.c.user_id == user.id,
                employee_shifts.c.status == ShiftStatus.on_shift,
                employee_shifts.c.shift_end.is_(None),
            )
        )
    )

    if not active_shift:
        raise HTTPException(
            status_code=400,
            detail="Пользователь должен быть на смене для создания перерыва",
        )

    updated_shift = await database.fetch_one(
        employee_shifts.update()
        .where(employee_shifts.c.id == active_shift.id)
        .values(
            status=ShiftStatus.on_break,
            break_start=datetime.utcnow(),
            break_duration=duration_minutes,
            updated_at=datetime.utcnow(),
        )
        .returning(
            employee_shifts.c.id,
            employee_shifts.c.user_id,
            employee_shifts.c.cashbox_id,
            employee_shifts.c.shift_start,
            employee_shifts.c.shift_end,
            employee_shifts.c.status,
            employee_shifts.c.break_start,
            employee_shifts.c.break_duration,
            employee_shifts.c.created_at,
            employee_shifts.c.updated_at,
        )
    )

    updated_shift_dict = dict(updated_shift)
    updated_shift_dict["user_id"] = user.user
    shift_response = ShiftResponse(**updated_shift_dict)
    shift_response_dict = shift_response.dict()
    serialized_data = serialize_shift_data(shift_response_dict)

    await log_shift_event(
        relation_id=updated_shift.user_id,
        cashbox_id=updated_shift.cashbox_id,
        status=updated_shift.status,
    )

    # Отправляем веб-сокет уведомление пользователю
    await manager.send_message(
        token,
        {
            "action": "start_break",
            "target": "employee_shifts",
            "result": serialized_data,
            "user_id": user.user,
            "cashbox_id": user.cashbox_id,
        },
    )

    # Отправляем уведомление администраторам
    await send_shift_update_to_admins(
        cashbox_id=user.cashbox_id, shift_data=serialized_data, action="start_break"
    )

    # Обновляем статистику для админов
    await send_statistics_update(user.cashbox_id)

    return shift_response


@router.get("/status", response_model=ShiftStatusResponse)
async def get_shift_status(token: str):
    """Получить текущий статус смены пользователя"""

    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    if not user.shift_work_enabled:
        return ShiftStatusResponse(
            is_on_shift=False,
            status=ShiftStatus.off_shift,
            current_shift=None,
            message="Работа по сменам отключена",
        )

    # Ищем активную смену
    active_shift = await database.fetch_one(
        employee_shifts.select().where(
            and_(
                employee_shifts.c.user_id == user.id,
                employee_shifts.c.shift_end.is_(None),
            )
        )
    )

    if not active_shift:
        return ShiftStatusResponse(
            is_on_shift=False,
            status=ShiftStatus.off_shift,
            current_shift=None,
            message="Смена не начата",
        )

    # Проверяем истек ли перерыв и обновляем при необходимости
    current_shift_data = dict(active_shift)
    auto_updated = False

    if (
        active_shift.status == ShiftStatus.on_break
        and active_shift.break_start
        and active_shift.break_duration
    ):

        break_end_time = active_shift.break_start + timedelta(
            minutes=active_shift.break_duration
        )
        if datetime.utcnow() >= break_end_time:
            now = datetime.utcnow()
            updated_shift = await database.fetch_one(
                employee_shifts.update()
                .where(employee_shifts.c.id == active_shift.id)
                .values(
                    status=ShiftStatus.on_shift,
                    break_start=None,
                    break_duration=None,
                    updated_at=now,
                )
                .returning(
                    employee_shifts.c.id,
                    employee_shifts.c.user_id,
                    employee_shifts.c.cashbox_id,
                    employee_shifts.c.shift_start,
                    employee_shifts.c.shift_end,
                    employee_shifts.c.status,
                    employee_shifts.c.break_start,
                    employee_shifts.c.break_duration,
                    employee_shifts.c.created_at,
                    employee_shifts.c.updated_at,
                )
            )
            current_shift_data = dict(updated_shift)
            auto_updated = True

    # Если произошло автоматическое обновление, отправляем уведомление
    if auto_updated:
        # В ответе и уведомлениях возвращаем users.id
        current_shift_data["user_id"] = user.user
        shift_response = ShiftResponse(**current_shift_data)
        shift_response_dict = shift_response.dict()
        serialized_data = serialize_shift_data(shift_response_dict)

        await manager.send_message(
            token,
            {
                "action": "auto_end_break",
                "target": "employee_shifts",
                "result": serialized_data,
                "user_id": user.user,
                "cashbox_id": user.cashbox_id,
            },
        )

        # Уведомляем администраторов об автоматическом завершении перерыва
        await send_shift_update_to_admins(
            cashbox_id=user.cashbox_id,
            shift_data=serialized_data,
            action="auto_end_break",
        )

        # Обновляем статистику для админов
        await send_statistics_update(user.cashbox_id)

    # Перед формированием финального ответа также подменяем user_id на users.id
    current_shift_data["user_id"] = user.user
    return ShiftStatusResponse(
        is_on_shift=current_shift_data["status"]
        in [ShiftStatus.on_shift, ShiftStatus.on_break],
        status=ShiftStatus(current_shift_data["status"]),
        current_shift=ShiftResponse(**current_shift_data),
        message=f"Статус: {current_shift_data['status']}",
    )


# Админские эндпоинты
@router.get("/list-with-shifts/", response_model=CBUsersListShortWithShifts)
async def get_users_list_with_shift_info(
    token: str, name: str = None, limit: int = 100, offset: int = 0
):
    """Получить список пользователей с информацией о сменах (админ)"""

    # Получаем текущего пользователя
    current_user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    current_user = await database.fetch_one(current_user_query)

    if not current_user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    # Фильтры для поиска пользователей
    filters = [users_cboxes_relation.c.cashbox_id == current_user.cashbox_id]

    if name:
        filters.append(
            or_(
                users.c.first_name.ilike(f"%{name}%"),
                users.c.last_name.ilike(f"%{name}%"),
                users.c.username.ilike(f"%{name}%"),
            )
        )

    users_with_shifts_query = (
        select(
            [
                users_cboxes_relation.c.id,
                users_cboxes_relation.c.user,
                users.c.first_name,
                users.c.last_name,
                users.c.username,
                users.c.photo,
                users_cboxes_relation.c.shift_work_enabled,
                employee_shifts.c.status.label("current_shift_status"),
                employee_shifts.c.shift_start,
                employee_shifts.c.break_start,
                employee_shifts.c.break_duration,
            ]
        )
        .select_from(
            users_cboxes_relation.join(
                users, users.c.id == users_cboxes_relation.c.user
            ).outerjoin(
                employee_shifts,
                and_(
                    employee_shifts.c.user_id == users_cboxes_relation.c.id,
                    employee_shifts.c.shift_end.is_(None),
                ),
            )
        )
        .where(and_(*filters))
        .limit(limit)
        .offset(offset)
    )

    users_data = await database.fetch_all(users_with_shifts_query)

    result_users = []
    on_shift_count = 0

    for user in users_data:
        shift_duration_minutes = None

        # Если есть активная смена, считаем длительность
        if user.current_shift_status and user.shift_start:
            duration = datetime.utcnow() - user.shift_start
            shift_duration_minutes = int(duration.total_seconds() / 60)

            # Считаем людей на смене
            if user.current_shift_status in ["on_shift", "on_break"]:
                on_shift_count += 1

        result_users.append(
            UserWithShiftInfo(
                id=user.user,
                first_name=user.first_name or "",
                last_name=user.last_name or "",
                username=user.username or "",
                photo=user.photo or "",
                shift_work_enabled=user.shift_work_enabled or False,
                current_shift_status=user.current_shift_status,
                shift_duration_minutes=shift_duration_minutes,
            )
        )

    # Один запрос для count
    count_query = (
        select([func.count(users_cboxes_relation.c.id)])
        .select_from(
            users_cboxes_relation.join(users, users.c.id == users_cboxes_relation.c.id)
        )
        .where(and_(*filters))
    )

    total_count = await database.fetch_val(count_query) or 0

    return CBUsersListShortWithShifts(
        result=result_users, count=total_count, on_shift_total=on_shift_count
    )


@router.get("/shifts-statistics/", response_model=ShiftStatistics)
async def get_shifts_statistics(token: str):
    """Получить статистику по сменам (админ)"""

    # Получаем текущего пользователя
    current_user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    current_user = await database.fetch_one(current_user_query)

    if not current_user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    stats_query = select(
        func.count()
        .filter(employee_shifts.c.status == ShiftStatus.on_shift)
        .label("on_shift_count"),
        func.count()
        .filter(employee_shifts.c.status == ShiftStatus.on_break)
        .label("on_break_count"),
        func.count(employee_shifts.c.id).label("total_active_shifts"),
    ).where(
        and_(
            employee_shifts.c.cashbox_id == current_user.cashbox_id,
            employee_shifts.c.shift_end.is_(None),
        )
    )

    stats = await database.fetch_one(stats_query)

    # Отдельный запрос для пользователей с включенными сменами
    shift_enabled_query = select([func.count(users_cboxes_relation.c.user)]).where(
        and_(
            users_cboxes_relation.c.cashbox_id == current_user.cashbox_id,
            users_cboxes_relation.c.shift_work_enabled == True,
        )
    )

    shift_enabled_count = await database.fetch_val(shift_enabled_query) or 0

    return ShiftStatistics(
        on_shift_count=int(stats.on_shift_count or 0),
        on_break_count=int(stats.on_break_count or 0),
        total_active=int(stats.total_active_shifts or 0),
        shift_enabled_users=shift_enabled_count,
    )


@router.post("/break/end", response_model=ShiftResponse)
async def end_break_early(token: str):
    """Завершить перерыв досрочно"""

    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    #  Проверяем, что пользователь на перерыве
    active_shift = await database.fetch_one(
        employee_shifts.select().where(
            and_(
                employee_shifts.c.user_id == user.id,
                employee_shifts.c.status == ShiftStatus.on_break,
                employee_shifts.c.shift_end.is_(None),
            )
        )
    )

    if not active_shift:
        raise HTTPException(status_code=400, detail="Пользователь не на перерыве")

    now = datetime.utcnow()
    updated_shift = await database.fetch_one(
        employee_shifts.update()
        .where(employee_shifts.c.id == active_shift.id)
        .values(
            status=ShiftStatus.on_shift,
            break_start=None,
            break_duration=None,
            updated_at=now,
        )
        .returning(
            employee_shifts.c.id,
            employee_shifts.c.user_id,
            employee_shifts.c.cashbox_id,
            employee_shifts.c.shift_start,
            employee_shifts.c.shift_end,
            employee_shifts.c.status,
            employee_shifts.c.break_start,
            employee_shifts.c.break_duration,
            employee_shifts.c.created_at,
            employee_shifts.c.updated_at,
        )
    )

    shift_response = ShiftResponse(**dict(updated_shift))
    shift_response_dict = shift_response.dict()
    serialized_data = serialize_shift_data(shift_response_dict)

    await log_shift_event(
        relation_id=updated_shift.user_id,
        cashbox_id=updated_shift.cashbox_id,
        status=updated_shift.status,
    )

    # Отправляем веб-сокет уведомление пользователю
    await manager.send_message(
        token,
        {
            "action": "end_break",
            "target": "employee_shifts",
            "result": serialized_data,
            "user_id": user.id,
            "cashbox_id": user.cashbox_id,
        },
    )

    # Отправляем уведомление администраторам
    await send_shift_update_to_admins(
        cashbox_id=user.cashbox_id, shift_data=serialized_data, action="end_break"
    )

    # Обновляем статистику для админов
    await send_statistics_update(user.cashbox_id)

    return shift_response


@router.get("/get_shifts_events", response_model=ShiftEventsList)
async def get_shifts_events(token: str, limit: int = 30, offset: int = 0):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    shifts_events_query = (
        select(
            employee_shifts_events.c.id,
            employee_shifts_events.c.relation_id,
            employee_shifts_events.c.cashbox_id,
            employee_shifts_events.c.shift_status,
            employee_shifts_events.c.event_start,
            employee_shifts_events.c.event_end,
            users.c.id.label("user_id"),
            users.c.first_name,
            users.c.last_name,
            users.c.username,
            users.c.photo,
            users.c.phone_number,
        )
        .select_from(
            employee_shifts_events.join(
                users_cboxes_relation,
                employee_shifts_events.c.relation_id == users_cboxes_relation.c.id,
            ).join(users, users_cboxes_relation.c.user == users.c.id)
        )
        .where(employee_shifts_events.c.cashbox_id == user.cashbox_id)
        .order_by(desc(employee_shifts_events.c.event_start))
        .limit(limit)
        .offset(offset)
    )
    total_count_query = select(func.count(employee_shifts_events.c.id)).where(
        employee_shifts_events.c.cashbox_id == user.cashbox_id
    )

    shifts_events = await database.fetch_all(shifts_events_query)
    total_count = await database.execute(total_count_query)

    return ShiftEventsList(
        result=[ShiftEvent.from_orm(i) for i in shifts_events], total_count=total_count
    )
