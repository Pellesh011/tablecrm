from datetime import datetime
from typing import Union

import api.templates.schemas as schemas
from database.db import areas, database, doc_templates, entity_to_entity, pages
from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from functions.helpers import get_user_by_token
from sqlalchemy import or_, select

router = APIRouter(tags=["doctemplates"])


@router.get("/doctemplates/", response_model=schemas.TemplateList)
async def get_list_template(
    token: str,
    tags: str = None,
    limit: int = 100,
    offset: int = 0,
    page: str = None,
    area: str = None,
):
    """
    Получение списка шаблонов документов

    tags - строка вида "tags1,tags2,tag". Теги указываются через запятую без пробелов. Поиск по включению и вывод
    значений по логике ИЛИ (если есть хотя бы один тег, то запись выводим)

    page - строка, название (name) из таблицы pages.
    area - строка, название (name) из таблицы areas.

    Поиск по area и page происходит по логике И (если есть совпадение по area то выовдим, если есть совпадение по
    page выводим, если оба значения присутвуют, то выводим
    """

    user = await get_user_by_token(token)
    filter_tags = []
    _filter = []
    if page:
        query_pages = (
            select(entity_to_entity.c.from_id)
            .join(pages, pages.c.id == entity_to_entity.c.to_id)
            .where(
                pages.c.name == page,
                entity_to_entity.c.cashbox_id == user.cashbox_id,
                or_(entity_to_entity.c.to_entity == 13),
            )
            .subquery("query_pages")
        )
        _filter.append(doc_templates.c.id.in_(query_pages))

    if area:
        query_areas = (
            select(entity_to_entity.c.from_id)
            .join(areas, areas.c.id == entity_to_entity.c.to_id)
            .where(
                areas.c.name == area,
                entity_to_entity.c.cashbox_id == user.cashbox_id,
                or_(entity_to_entity.c.to_entity == 12),
            )
            .subquery("query_areas")
        )
        _filter.append(doc_templates.c.id.in_(query_areas))

    if tags:
        tags = list(
            map(lambda x: x.strip().lower(), tags.replace(" ", "").strip().split(","))
        )
        filter_tags = list(map(lambda x: doc_templates.c.tags.ilike(f"%{x}%"), tags))

    query = (
        select(doc_templates)
        .where(
            doc_templates.c.is_deleted.is_not(True),
            doc_templates.c.cashbox == user.cashbox_id,
            *_filter,
            *filter_tags,
        )
        .limit(limit)
        .offset(offset)
    )

    result = await database.fetch_all(query)
    return {"result": result, "tags": ",".join(tags) if tags else ""}


@router.get("/doctemplates/{idx}/", response_model=schemas.DocTemplateFull)
async def get_template(token: str, idx: int):
    """Получение шаблона по ID"""

    user = await get_user_by_token(token)
    query_pages = (
        select(pages.c.name, pages.c.id)
        .join(pages, pages.c.id == entity_to_entity.c.to_id)
        .where(
            entity_to_entity.c.from_entity == 10,
            entity_to_entity.c.cashbox_id == user.cashbox_id,
            entity_to_entity.c.to_entity == 13,
            entity_to_entity.c.from_id == idx,
            pages.c.is_deleted.is_not(True),
        )
    )
    pages_db = await database.fetch_all(query_pages)

    query_areas = (
        select(areas.c.name, areas.c.id)
        .join(areas, areas.c.id == entity_to_entity.c.to_id)
        .where(
            entity_to_entity.c.from_entity == 10,
            entity_to_entity.c.cashbox_id == user.cashbox_id,
            entity_to_entity.c.to_entity == 12,
            entity_to_entity.c.from_id == idx,
            areas.c.is_deleted.is_not(True),
        )
    )
    areas_db = await database.fetch_all(query_areas)

    query = doc_templates.select().where(
        doc_templates.c.is_deleted.is_not(True),
        doc_templates.c.id == idx,
        doc_templates.c.cashbox == user.cashbox_id,
    )
    result = await database.fetch_one(query)

    if not result:
        raise HTTPException(status_code=404, detail="У вас нет шаблона с таким id")
    else:
        result = dict(result)
        result["pages"] = pages_db
        result["areas"] = areas_db
        return result


@database.transaction()
@router.post("/doctemplates/", response_model=schemas.DocTemplateCreate)
async def add_template(
    token: str,
    name: str,
    areas_in: list = None,
    pages_in: list = None,
    description: str = None,
    tags: str = None,
    doc_type: int = None,
    file: Union[UploadFile, None] = None,
):
    """
    Добавление нового шаблона

    tags - строка вида "tags1,tags2,tag". Теги указываются через запятую без пробелов

    areas_in - список id из таблицы areas
    pages_in - список id из таблицы pages
    Если значение не указвается, то необходимо ставить флаг "Send empty value"

    file - файл с контентом шаблона документа.
    Если значение не указвается, то необходимо СНЯТЬ флаг "Send empty value"

    doc_type - необязательный параметр
    """

    try:
        user = await get_user_by_token(token)
        template_res = dict(
            {
                "name": name,
                "cashbox": user.cashbox_id,
                "description": description,
                "user_id": user.id,
                "type": doc_type,
                "tags": (
                    ",".join(
                        sorted(
                            list(map(lambda x: x.strip(), tags.strip().split(","))),
                            key=str.lower,
                        )
                    )
                    if tags
                    else None
                ),
                "template_data": (
                    str(file.file.read().decode("UTF-8")) if file else None
                ),
                "is_deleted": False,
            }
        )

        template_res["created_at"] = int(datetime.utcnow().timestamp())
        template_res["updated_at"] = int(datetime.utcnow().timestamp())
        query = doc_templates.insert().values(template_res)
        result_id = await database.execute(query)
        query = doc_templates.select().where(doc_templates.c.id == result_id)
        result = await database.fetch_one(query)

        if areas_in[0] != "":
            await database.execute_many(
                entity_to_entity.insert(),
                values=[
                    {
                        "from_entity": 10,
                        "to_entity": 12,
                        "from_id": result["id"],
                        "to_id": int(item),
                        "status": True,
                        "delinked": False,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_template_areas",
                    }
                    for item in areas_in[0].split(",")
                    if int(item) > 0
                ],
            )

        if pages_in[0] != "":
            await database.execute_many(
                entity_to_entity.insert(),
                values=[
                    {
                        "from_entity": 10,
                        "to_entity": 13,
                        "from_id": result["id"],
                        "to_id": int(item),
                        "status": True,
                        "delinked": False,
                        "cashbox_id": user.cashbox_id,
                        "type": "docs_template_pages",
                    }
                    for item in pages_in[0].split(",")
                    if int(item) > 0
                ],
            )
        return result
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))


@database.transaction()
@router.delete("/doctemplates/{idx}/", response_model=schemas.DocTemplate)
async def delete_template(token: str, idx: int):
    """Удаление шаблона по ID"""
    try:
        user = await get_user_by_token(token)
        query = (
            doc_templates.update()
            .where(
                doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id
            )
            .values(
                dict(is_deleted=True, updated_at=int(datetime.utcnow().timestamp()))
            )
        )
        result = await database.execute(query)
        query = doc_templates.select().where(
            doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id
        )
        result = await database.fetch_one(query)
        if not result:
            raise HTTPException(status_code=404, detail="У вас нет шаблона с таким id")
        return result
    except Exception as error:
        raise HTTPException(status_code=433, detail=str(error))


@router.patch("/doctemplates/{idx}/", response_model=schemas.DocTemplateUpdate)
async def update_template(
    token: str,
    idx: int,
    template: schemas.TemptalePatchBody = Depends(),
    file: Union[UploadFile, None] = None,
):
    """Обновление шаблона по ID"""
    user = await get_user_by_token(token)
    template = template.dict(exclude_unset=True, exclude_none=True)
    template["template_data"] = str(file.file.read().decode("UTF-8")) if file else None
    template["updated_at"] = int(datetime.utcnow().timestamp())
    template_db = await database.fetch_one(
        doc_templates.select().where(
            doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id
        )
    )
    template_db_model = schemas.DocTemplateCreate(**template_db)
    template_item = template_db_model.copy(update=template)
    query = (
        doc_templates.update()
        .where(doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id)
        .values(template_item.dict())
    )
    await database.execute(query)
    query = doc_templates.select().where(
        doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id
    )
    result = await database.fetch_one(query)
    if not result:
        raise HTTPException(status_code=404, detail="У вас нет шаблона с таким id")
    return result


@router.get("/doctemplates/{idx}/template")
async def get_template_data(token: str, idx: int):
    """Получение HTML-шаблона по ID шаблона"""
    user = await get_user_by_token(token)
    query = select(doc_templates.c.template_data).where(
        doc_templates.c.id == idx, doc_templates.c.cashbox == user.cashbox_id
    )
    result = await database.fetch_val(query)
    return HTMLResponse(content=result, media_type="text/html")
