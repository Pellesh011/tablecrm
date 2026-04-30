import asyncio
from datetime import datetime

from database.db import database, pboxes, projects, users, users_cboxes_relation
from sqlalchemy import select
from ws_manager import manager

_raschet_locks: dict[int, asyncio.Lock] = {}


async def get_user_id_cashbox_id_by_token(token: str):
    user_cbox = await database.fetch_one(
        users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    )

    if user_cbox:
        user = await database.fetch_one(
            users.select().where(users.c.id == user_cbox.user)
        )
        return user.id, user_cbox.cashbox_id
    else:
        return None, None


async def get_user_by_token(token: str):
    user_cbox = await database.fetch_one(
        users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    )

    user_dict = None
    if user_cbox:
        user = await database.fetch_one(
            users.select().where(users.c.id == user_cbox.user)
        )
        user_dict = {
            "id": user.id,
            "external_id": user.external_id,
            "photo": user.photo,
            "phone_number": user.phone_number,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "username": user.username,
            "status": user_cbox.status,
            "is_admin": user_cbox.is_owner,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "tags": user_cbox.tags,
            "timezone": user_cbox.timezone,
            "payment_past_edit_days": user_cbox.payment_past_edit_days,
            "shift_work_enabled": user_cbox.shift_work_enabled,
        }

    return user_dict


async def _raschet_debounced(user, token):
    lock = _raschet_locks.setdefault(user.cashbox_id, asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        await raschet(user, token)


async def raschet(user, token):
    rows = await database.fetch_all(
        "SELECT * FROM public.raschet(:token, :today)",
        {"token": token, "today": int(datetime.utcnow().timestamp())},
    )

    payboxes_data = []
    projects_data = []

    for row in rows:
        if row["pb_id"] is not None:
            payboxes_data.append({"pb_id": row["pb_id"], "balance": row["balance"]})
        if row["pr_id"] is not None:
            projects_data.append(
                {
                    "pr_id": row["pr_id"],
                    "incoming": row["incoming"],
                    "outgoing": row["outgoing"],
                }
            )

    payboxes_list_q = select([pboxes.c.id, pboxes.c.start_balance]).where(
        pboxes.c.cashbox == user.cashbox_id,
        pboxes.c.deleted_at.is_(None),
    )
    projects_list_q = select([projects.c.id]).where(
        projects.c.cashbox == user.cashbox_id
    )

    payboxes_ids = await database.fetch_all(payboxes_list_q)
    projects_ids = await database.fetch_all(projects_list_q)

    # Determine which payboxes/projects were NOT in the result
    updated_pb_ids = {pb.get("pb_id") for pb in payboxes_data if pb.get("pb_id")}
    updated_pr_ids = {pr.get("pr_id") for pr in projects_data if pr.get("pr_id")}

    z_pbs = [dict(pb) for pb in payboxes_ids if pb.id not in updated_pb_ids]
    z_proj = [dict(pr) for pr in projects_ids if pr.id not in updated_pr_ids]
    payboxes_start_balance = {pb.id: pb.start_balance for pb in payboxes_ids}

    # Process Payboxes from result
    payboxes_update_values = []
    payboxes_updated_ids = []
    for i in payboxes_data:
        pb_id = i.get("pb_id")
        balance = i.get("balance")
        if pb_id and pb_id in payboxes_start_balance:
            payboxes_update_values.append(
                {
                    "id": pb_id,
                    "balance": round(
                        float(balance) + payboxes_start_balance[pb_id],
                        2,
                    ),
                    "update_start_balance": int(datetime.utcnow().timestamp()),
                }
            )
            payboxes_updated_ids.append(pb_id)
    if payboxes_update_values:
        q = (
            "UPDATE payboxes "
            "SET balance = :balance, update_start_balance = :update_start_balance "
            "WHERE id = :id"
        )
        await database.execute_many(q, payboxes_update_values)
        q = pboxes.select().where(
            pboxes.c.id.in_(payboxes_updated_ids),
            pboxes.c.deleted_at.is_(None),
        )
        payboxes_result = await database.fetch_all(q)
        payboxes_result_map = {paybox.id: paybox for paybox in payboxes_result}
        for pb_id in payboxes_updated_ids:
            paybox = payboxes_result_map[pb_id]
            await manager.send_message(
                token,
                {"action": "edit", "target": "payboxes", "result": dict(paybox)},
            )

    # Process Projects from result
    projects_update_values = []
    projects_updated_ids = []
    for i in projects_data:
        pr_id = i.get("pr_id")
        incoming = i.get("incoming")
        outgoing = i.get("outgoing")

        if pr_id:
            update_proj = {
                "id": pr_id,
                "incoming": incoming,
                "outgoing": outgoing,
                "updated_at": int(datetime.utcnow().timestamp()),
            }

            if update_proj["outgoing"] == 0 and update_proj["incoming"] != 0:
                update_proj["profitability"] = 100
            elif update_proj["outgoing"] == 0 and update_proj["incoming"] == 0:
                update_proj["profitability"] = 0
            else:
                update_proj["profitability"] = round(
                    (
                        (update_proj["incoming"] - update_proj["outgoing"])
                        / update_proj["outgoing"]
                    )
                    * 100,
                    2,
                )
            projects_update_values.append(update_proj)
            projects_updated_ids.append(pr_id)
    if projects_update_values:
        q = (
            "UPDATE projects "
            "SET incoming = :incoming, outgoing = :outgoing, "
            "profitability = :profitability, updated_at = :updated_at "
            "WHERE id = :id"
        )
        await database.execute_many(q, projects_update_values)
        q = projects.select().where(projects.c.id.in_(projects_updated_ids))
        projects_result = await database.fetch_all(q)
        projects_result_map = {project.id: project for project in projects_result}
        for pr_id in projects_updated_ids:
            project = projects_result_map[pr_id]
            await manager.send_message(
                token, {"action": "edit", "target": "projects", "result": dict(project)}
            )

    # Reset remaining Payboxes
    z_payboxes_update_values = []
    z_payboxes_updated_ids = []
    for z_paybox in z_pbs:
        z_payboxes_update_values.append(
            {
                "id": z_paybox["id"],
                "balance": z_paybox["start_balance"],
                "update_start_balance": int(datetime.utcnow().timestamp()),
            }
        )
        z_payboxes_updated_ids.append(z_paybox["id"])
    if z_payboxes_update_values:
        q = (
            "UPDATE payboxes "
            "SET balance = :balance, update_start_balance = :update_start_balance "
            "WHERE id = :id"
        )
        await database.execute_many(q, z_payboxes_update_values)
        q = pboxes.select().where(
            pboxes.c.id.in_(z_payboxes_updated_ids),
            pboxes.c.deleted_at.is_(None),
        )
        z_payboxes_result = await database.fetch_all(q)
        z_payboxes_result_map = {paybox.id: paybox for paybox in z_payboxes_result}
        for pb_id in z_payboxes_updated_ids:
            paybox = z_payboxes_result_map[pb_id]
            await manager.send_message(
                token,
                {"action": "edit", "target": "payboxes", "result": dict(paybox)},
            )

    # Reset remaining Projects
    z_projects_update_values = []
    z_projects_updated_ids = []
    for z_project in z_proj:
        z_projects_update_values.append(
            {
                "id": z_project["id"],
                "incoming": 0,
                "outgoing": 0,
                "profitability": 0,
                "updated_at": int(datetime.utcnow().timestamp()),
            }
        )
        z_projects_updated_ids.append(z_project["id"])
    if z_projects_update_values:
        q = (
            "UPDATE projects "
            "SET incoming = :incoming, outgoing = :outgoing, "
            "profitability = :profitability, updated_at = :updated_at "
            "WHERE id = :id"
        )
        await database.execute_many(q, z_projects_update_values)
        q = projects.select().where(projects.c.id.in_(z_projects_updated_ids))
        z_projects_result = await database.fetch_all(q)
        z_projects_result_map = {project.id: project for project in z_projects_result}
        for pr_id in z_projects_updated_ids:
            project = z_projects_result_map[pr_id]
            await manager.send_message(
                token, {"action": "edit", "target": "projects", "result": dict(project)}
            )
