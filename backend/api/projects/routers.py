from datetime import datetime

import aiofiles
import api.projects.schemas as proj_schemas
import functions.filter_schemas as filter_schemas
from database.db import database, projects, users_cboxes_relation
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from functions.helpers import get_filters_projects
from sqlalchemy import asc, desc, func, select
from ws_manager import manager

router = APIRouter(tags=["projects"])


@router.get("/projects/{id}/", response_model=proj_schemas.Project)
async def get_project_by_id(token: str, id: int):
    """Получение проекта по ID"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = projects.select().where(
                projects.c.id == id, projects.c.cashbox == user.cashbox_id
            )
            project_db = await database.fetch_one(query)

            if project_db:
                if project_db.cashbox == user.cashbox_id:
                    return project_db

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/projects/", response_model=proj_schemas.GetProjects)
async def get_projects(
    token: str,
    limit: int = 100,
    offset: int = 0,
    sort: str = "created_at:desc",
    filters: filter_schemas.ProjectsFiltersQuery = Depends(),
):
    """Получение проектов кассы"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            filters = get_filters_projects(projects, filters)
            sort_list = sort.split(":")
            if sort_list[0] not in ["created_at", "updated_at"]:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )
            if sort_list[1] == "desc":
                query = (
                    projects.select()
                    .where(projects.c.cashbox == user.cashbox_id)
                    .filter(*filters)
                    .order_by(desc(getattr(projects.c, sort_list[0])))
                    .limit(limit)
                    .offset(offset)
                )

            elif sort_list[1] == "asc":
                query = (
                    projects.select()
                    .where(projects.c.cashbox == user.cashbox_id)
                    .filter(*filters)
                    .order_by(asc(getattr(projects.c, sort_list[0])))
                    .limit(limit)
                    .offset(offset)
                )
            else:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )

            projects_db = await database.fetch_all(query)

            q = (
                select(func.count(projects.c.id))
                .where(projects.c.cashbox == user.cashbox_id)
                .filter(*filters)
            )
            count = await database.fetch_one(q)

            return {"result": projects_db, "count": count.count_1}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.post("/projects/")
async def new_project(token: str, proj: proj_schemas.ProjectCreate):
    """Создание проекта"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            created = int(datetime.utcnow().timestamp())
            updated = int(datetime.utcnow().timestamp())
            proj_values = {
                "name": proj.name,
                "incoming": 0,
                "outgoing": 0,
                "profitability": 0,
                "external_id": proj.external_id,
                "proj_sum": proj.proj_sum,
                "created_at": created,
                "updated_at": updated,
                "cashbox": user.cashbox_id,
            }

            query = projects.insert().values(proj_values)
            proj_id = await database.execute(query)

            query = projects.select(projects.c.id == proj_id)
            proj_db = await database.fetch_one(query)

            await manager.send_message(
                token,
                {"action": "create", "target": "projects", "result": dict(proj_db)},
            )

            return proj_db

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.put("/projects/")
async def edit_project(token: str, proj: proj_schemas.ProjectEdit):
    """Обновление проекта"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = projects.select().where(
                projects.c.id == proj.id, projects.c.cashbox == user.cashbox_id
            )
            proj_db = await database.fetch_one(query)

            proj_db_dict = proj.dict()

            del proj_db_dict["id"]
            new_values = {}

            for i, j in proj_db_dict.items():
                if j is not None:
                    new_values[i] = j

            if new_values:
                if proj_db.cashbox == user.cashbox_id:
                    query = (
                        projects.update()
                        .where(
                            projects.c.id == proj.id,
                            projects.c.cashbox == user.cashbox_id,
                        )
                        .values(new_values)
                    )
                    await database.execute(query)

                    query = projects.select().where(
                        projects.c.id == proj.id, projects.c.cashbox == user.cashbox_id
                    )
                    proj_db = await database.fetch_one(query)

                    await manager.send_message(
                        token,
                        {
                            "action": "edit",
                            "target": "projects",
                            "result": dict(proj_db),
                        },
                    )

                    return proj_db
            return {"status": "пустое поле"}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.put("/projects/add_icon/")
async def add_icon_to_project(
    token: str, proj_id: int, icon_file: UploadFile = File(...)
):
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if user:
        if user.status:
            query = projects.select().where(
                projects.c.id == proj_id, projects.c.cashbox == user.cashbox_id
            )
            proj_db = await database.fetch_one(query)
            if proj_db.cashbox == user.cashbox_id:
                new_values = {}
                file_link = f"photos/icon_{proj_id}.{icon_file.filename.split('.')[-1]}"
                file_bytes = await icon_file.read()

                try:
                    async with aiofiles.open(file_link, "+wb") as file:
                        await file.write(file_bytes)
                finally:
                    await icon_file.close()

                new_values.update({"icon_file": file_link})

                query = (
                    projects.update()
                    .where(
                        projects.c.id == proj_id, projects.c.cashbox == user.cashbox_id
                    )
                    .values(new_values)
                )
                await database.execute(query)

                query = projects.select().where(
                    projects.c.id == proj_id, projects.c.cashbox == user.cashbox_id
                )
                proj_db = await database.fetch_one(query)

                await manager.send_message(
                    token,
                    {"action": "edit", "target": "projects", "result": dict(proj_db)},
                )
                return proj_db
