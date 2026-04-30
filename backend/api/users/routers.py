from datetime import datetime

from api.users import schemas as schemas
from database.db import (
    database,
    employee_shifts,
    pboxes,
    user_permissions,
    users,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException
from functions import users as func
from functions.helpers import raise_wrong_token
from sqlalchemy import (
    and_,
    func as fsql,
    or_,
    select,
)

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/", response_model=schemas.CBUsers)
async def get_user_by_token_route(token: str):
    res = await func.get_user_by_token(token=token)
    if res:
        return res
    raise_wrong_token()


@router.get("/list/", response_model=schemas.CBUsersListShort)
async def get_user_list(
    token: str, name: str = None, limit: int = 100, offset: int = 0
):
    cashbox_query = (
        select(users_cboxes_relation.c.cashbox_id)
        .where(users_cboxes_relation.c.token == token)
        .subquery("cashbox_query")
    )

    filters = [users_cboxes_relation.c.cashbox_id == cashbox_query.c.cashbox_id]

    if name:
        filters.append(
            or_(
                users.c.first_name.ilike(f"%{name}%"),
                users.c.last_name.ilike(f"%{name}%"),
            )
        )

    users_cashbox = (
        select(
            users.c.id,
            users.c.external_id,
            users.c.first_name,
            users.c.last_name,
            users_cboxes_relation.c.status,
            users_cboxes_relation.c.shift_work_enabled,
        )
        .where(*filters)
        .join(users, users.c.id == users_cboxes_relation.c.user)
        .limit(limit)
        .offset(offset)
    )

    users_list = await database.fetch_all(users_cashbox)
    count = await database.fetch_val(select(fsql.count(users_cashbox.c.id)))
    return {"result": users_list, "count": count}


@router.post("/permissions/", response_model=schemas.UserPermissionResponse)
async def set_user_permissions(token: str, data: schemas.UserPermissionUpdate):
    """Установка прав для пользователя"""

    user_relation_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user_relation = await database.fetch_one(user_relation_query)

    if not user_relation or not user_relation.is_owner:
        raise HTTPException(
            status_code=403,
            detail="Только администратор может настраивать права пользователей",
        )

    target_user_query = users_cboxes_relation.select().where(
        and_(
            users_cboxes_relation.c.user == data.user_id,
            users_cboxes_relation.c.cashbox_id == user_relation.cashbox_id,
        )
    )
    target_user = await database.fetch_one(target_user_query)

    if not target_user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    if target_user.is_owner:
        raise HTTPException(
            status_code=400, detail="Невозможно изменить права администратора"
        )

    current_permissions_query = user_permissions.select().where(
        and_(
            user_permissions.c.user_id == target_user.id,
            user_permissions.c.cashbox_id == user_relation.cashbox_id,
            user_permissions.c.section.in_(["payments", "pboxes"]),
        )
    )
    current_permissions = await database.fetch_all(current_permissions_query)

    existing_permissions = {}
    for perm in current_permissions:
        key = f"{perm.section}:{perm.paybox_id if perm.paybox_id else 'all'}"
        existing_permissions[key] = perm

    for perm in data.permissions:
        if perm.section not in ["payments", "pboxes"]:
            continue

        key = f"{perm.section}:{perm.paybox_id if perm.paybox_id else 'all'}"

        if key in existing_permissions:
            update_query = (
                user_permissions.update()
                .where(user_permissions.c.id == existing_permissions[key].id)
                .values(can_view=perm.can_view, can_edit=perm.can_edit)
            )
            await database.execute(update_query)
            del existing_permissions[key]
        else:
            await database.execute(
                user_permissions.insert().values(
                    user_id=target_user.id,
                    section=perm.section,
                    can_view=perm.can_view,
                    can_edit=perm.can_edit,
                    paybox_id=perm.paybox_id,
                    cashbox_id=user_relation.cashbox_id,
                )
            )

    for perm in existing_permissions.values():
        delete_query = user_permissions.delete().where(user_permissions.c.id == perm.id)
        await database.execute(delete_query)

    return {"status": "success", "message": "Права пользователя обновлены"}


@router.get("/permissions/{user_id}", response_model=schemas.UserPermissionsResult)
async def get_user_permissions(token: str, user_id: int):
    """Получение прав пользователя"""

    user_relation_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user_relation = await database.fetch_one(user_relation_query)

    if not user_relation or not user_relation.is_owner:
        raise HTTPException(
            status_code=403,
            detail="Только администратор может просматривать права пользователей",
        )

    target_relation_query = users_cboxes_relation.select().where(
        and_(
            users_cboxes_relation.c.user == user_id,
            users_cboxes_relation.c.cashbox_id == user_relation.cashbox_id,
        )
    )
    target_relation = await database.fetch_one(target_relation_query)

    if not target_relation:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    target_user_query = users.select().where(users.c.id == target_relation.user)
    target_user = await database.fetch_one(target_user_query)

    permissions_query = user_permissions.select().where(
        and_(
            user_permissions.c.user_id == target_relation.id,
            user_permissions.c.cashbox_id == user_relation.cashbox_id,
            user_permissions.c.section.in_(["payments", "pboxes"]),
        )
    )
    permissions_db = await database.fetch_all(permissions_query)

    existing_permissions = {}
    for perm in permissions_db:
        section = perm.section
        if section not in existing_permissions:
            existing_permissions[section] = {}

        if perm.paybox_id is None:
            existing_permissions[section]["all"] = {
                "can_view": perm.can_view,
                "can_edit": perm.can_edit,
            }
        else:
            existing_permissions[section][perm.paybox_id] = {
                "can_view": perm.can_view,
                "can_edit": perm.can_edit,
            }

    sections_list = ["payments", "pboxes"]
    permissions_list = []

    for section in sections_list:
        section_perm = {
            "section": section,
            "can_view": False,
            "can_edit": False,
            "paybox_id": None,
            "paybox_name": None,
        }

        if section in existing_permissions and "all" in existing_permissions[section]:
            section_perm["can_view"] = existing_permissions[section]["all"]["can_view"]
            section_perm["can_edit"] = existing_permissions[section]["all"]["can_edit"]

        permissions_list.append(section_perm)

    payboxes_query = pboxes.select().where(
        pboxes.c.cashbox == user_relation.cashbox_id,
        pboxes.c.deleted_at.is_(None),
    )
    payboxes_list = await database.fetch_all(payboxes_query)

    for paybox in payboxes_list:
        pbox_perm = {
            "section": "pboxes",
            "can_view": False,
            "can_edit": False,
            "paybox_id": paybox.id,
            "paybox_name": paybox.name,
        }

        if "pboxes" in existing_permissions and "all" in existing_permissions["pboxes"]:
            pbox_perm["can_view"] = existing_permissions["pboxes"]["all"]["can_view"]
            pbox_perm["can_edit"] = existing_permissions["pboxes"]["all"]["can_edit"]

        if (
            "pboxes" in existing_permissions
            and paybox.id in existing_permissions["pboxes"]
        ):
            pbox_perm["can_view"] = existing_permissions["pboxes"][paybox.id][
                "can_view"
            ]
            pbox_perm["can_edit"] = existing_permissions["pboxes"][paybox.id][
                "can_edit"
            ]

        permissions_list.append(pbox_perm)

        payment_perm = {
            "section": "payments",
            "can_view": False,
            "can_edit": False,
            "paybox_id": paybox.id,
            "paybox_name": paybox.name,
        }

        if (
            "payments" in existing_permissions
            and "all" in existing_permissions["payments"]
        ):
            payment_perm["can_view"] = existing_permissions["payments"]["all"][
                "can_view"
            ]
            payment_perm["can_edit"] = existing_permissions["payments"]["all"][
                "can_edit"
            ]

        if (
            "payments" in existing_permissions
            and paybox.id in existing_permissions["payments"]
        ):
            payment_perm["can_view"] = existing_permissions["payments"][paybox.id][
                "can_view"
            ]
            payment_perm["can_edit"] = existing_permissions["payments"][paybox.id][
                "can_edit"
            ]

        permissions_list.append(payment_perm)

    return {
        "user_id": user_id,
        "first_name": target_user.first_name,
        "last_name": target_user.last_name,
        "username": target_user.username,
        "permissions": permissions_list,
    }


@router.get("/permissions/me/", response_model=schemas.UserPermissionsResult)
async def get_my_permissions(token: str):
    """Получение прав текущего пользователя"""

    user_relation_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user_relation = await database.fetch_one(user_relation_query)

    if not user_relation or not user_relation.status:
        raise HTTPException(status_code=403, detail="Некорректный токен")

    target_user_query = users.select().where(users.c.id == user_relation.user)
    target_user = await database.fetch_one(target_user_query)

    if user_relation.is_owner:
        payboxes_query = pboxes.select().where(
            pboxes.c.cashbox == user_relation.cashbox_id,
            pboxes.c.deleted_at.is_(None),
        )
        payboxes_list = await database.fetch_all(payboxes_query)

        sections_list = ["payments", "pboxes"]
        permissions_list = []

        for section in sections_list:
            permissions_list.append(
                {
                    "section": section,
                    "can_view": True,
                    "can_edit": True,
                    "paybox_id": None,
                    "paybox_name": None,
                }
            )

        for paybox in payboxes_list:
            permissions_list.append(
                {
                    "section": "pboxes",
                    "can_view": True,
                    "can_edit": True,
                    "paybox_id": paybox.id,
                    "paybox_name": paybox.name,
                }
            )

            permissions_list.append(
                {
                    "section": "payments",
                    "can_view": True,
                    "can_edit": True,
                    "paybox_id": paybox.id,
                    "paybox_name": paybox.name,
                }
            )

        return {
            "is_admin": user_relation.is_owner,
            "user_id": user_relation.user,
            "first_name": target_user.first_name,
            "last_name": target_user.last_name,
            "username": target_user.username,
            "permissions": permissions_list,
        }

    permissions_query = user_permissions.select().where(
        and_(
            user_permissions.c.user_id == user_relation.id,
            user_permissions.c.cashbox_id == user_relation.cashbox_id,
            user_permissions.c.section.in_(["payments", "pboxes"]),
        )
    )
    permissions_db = await database.fetch_all(permissions_query)

    existing_permissions = {}
    for perm in permissions_db:
        section = perm.section
        if section not in existing_permissions:
            existing_permissions[section] = {}

        if perm.paybox_id is None:
            existing_permissions[section]["all"] = {
                "can_view": perm.can_view,
                "can_edit": perm.can_edit,
            }
        else:
            existing_permissions[section][perm.paybox_id] = {
                "can_view": perm.can_view,
                "can_edit": perm.can_edit,
            }

    sections_list = ["payments", "pboxes"]
    permissions_list = []

    for section in sections_list:
        section_perm = {
            "section": section,
            "can_view": False,
            "can_edit": False,
            "paybox_id": None,
            "paybox_name": None,
        }

        if section in existing_permissions and "all" in existing_permissions[section]:
            section_perm["can_view"] = existing_permissions[section]["all"]["can_view"]
            section_perm["can_edit"] = existing_permissions[section]["all"]["can_edit"]

        permissions_list.append(section_perm)

    payboxes_query = pboxes.select().where(
        pboxes.c.cashbox == user_relation.cashbox_id,
        pboxes.c.deleted_at.is_(None),
    )
    payboxes_list = await database.fetch_all(payboxes_query)

    for paybox in payboxes_list:
        pbox_perm = {
            "section": "pboxes",
            "can_view": False,
            "can_edit": False,
            "paybox_id": paybox.id,
            "paybox_name": paybox.name,
        }

        if "pboxes" in existing_permissions and "all" in existing_permissions["pboxes"]:
            pbox_perm["can_view"] = existing_permissions["pboxes"]["all"]["can_view"]
            pbox_perm["can_edit"] = existing_permissions["pboxes"]["all"]["can_edit"]

        if (
            "pboxes" in existing_permissions
            and paybox.id in existing_permissions["pboxes"]
        ):
            pbox_perm["can_view"] = existing_permissions["pboxes"][paybox.id][
                "can_view"
            ]
            pbox_perm["can_edit"] = existing_permissions["pboxes"][paybox.id][
                "can_edit"
            ]

        if pbox_perm["can_view"]:
            permissions_list.append(pbox_perm)

            payment_perm = {
                "section": "payments",
                "can_view": False,
                "can_edit": False,
                "paybox_id": paybox.id,
                "paybox_name": paybox.name,
            }

            if (
                "payments" in existing_permissions
                and "all" in existing_permissions["payments"]
            ):
                payment_perm["can_view"] = existing_permissions["payments"]["all"][
                    "can_view"
                ]
                payment_perm["can_edit"] = existing_permissions["payments"]["all"][
                    "can_edit"
                ]

            if (
                "payments" in existing_permissions
                and paybox.id in existing_permissions["payments"]
            ):
                payment_perm["can_view"] = existing_permissions["payments"][paybox.id][
                    "can_view"
                ]
                payment_perm["can_edit"] = existing_permissions["payments"][paybox.id][
                    "can_edit"
                ]

            if payment_perm["can_view"]:
                permissions_list.append(payment_perm)

    return {
        "is_admin": user_relation.is_owner,
        "user_id": user_relation.user,
        "first_name": target_user.first_name,
        "last_name": target_user.last_name,
        "username": target_user.username,
        "permissions": permissions_list,
    }


@router.patch(
    "/{user_id}/shift-settings", response_model=schemas.UserShiftSettingsResponse
)
async def update_user_shift_settings(
    user_id: int, settings: schemas.UserShiftSettings, token: str
):
    """Включить/выключить работу по сменам для пользователя"""

    current_user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    current_user = await database.fetch_one(current_user_query)

    if not current_user or not current_user.is_owner:
        raise HTTPException(
            status_code=403,
            detail="Только администратор может управлять настройками смен",
        )

    # Проверяем что целевой пользователь принадлежит к той же кассе
    target_user_query = users_cboxes_relation.select().where(
        and_(
            users_cboxes_relation.c.user == user_id,
            users_cboxes_relation.c.cashbox_id == current_user.cashbox_id,
        )
    )
    target_user = await database.fetch_one(target_user_query)

    if not target_user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    # Обновляем настройки смены
    await database.execute(
        users_cboxes_relation.update()
        .where(users_cboxes_relation.c.user == user_id)
        .values(shift_work_enabled=settings.shift_work_enabled)
    )

    # Если отключаем смены - завершаем активную смену
    if not settings.shift_work_enabled:
        active_shift_query = employee_shifts.select().where(
            and_(
                employee_shifts.c.user_id == user_id,
                employee_shifts.c.status.in_(["on_shift", "on_break"]),
                employee_shifts.c.shift_end.is_(None),
            )
        )
        active_shift = await database.fetch_one(active_shift_query)

        if active_shift:
            await database.execute(
                employee_shifts.update()
                .where(employee_shifts.c.id == active_shift.id)
                .values(
                    shift_end=datetime.utcnow(),
                    status="off_shift",
                    break_start=None,
                    break_duration=None,
                    updated_at=datetime.utcnow(),
                )
            )

    return schemas.UserShiftSettingsResponse(
        success=True,
        message=f"Настройки смены {'включены' if settings.shift_work_enabled else 'выключены'}",
        shift_work_enabled=settings.shift_work_enabled,
    )
