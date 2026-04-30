from datetime import datetime
from typing import List, Union

import api.contragents.schemas as ca_schemas
import functions.filter_schemas as filter_schemas
import phonenumbers
from asyncpg import ForeignKeyViolationError
from database.db import (
    contragents,
    contragents_tags,
    database,
    tags,
    users_cboxes_relation,
)
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from functions.helpers import build_filters, get_filters_ca
from phonenumbers import geocoder
from sqlalchemy import asc, cast, desc, func, literal_column, select
from sqlalchemy.dialects.postgresql import JSONB
from ws_manager import manager

router = APIRouter(tags=["contragents"])


@router.get("/contragents/")
async def read_contragents_meta(
    token: str,
    limit: int = 100,
    offset: int = 0,
    sort: str = "created_at:desc",
    add_tags: bool = False,
    filters: filter_schemas.CAFiltersQuery = Depends(),
    cu_filters: filter_schemas.CUIntegerFilters = Depends(),
):
    """Получение меты контрагентов"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    filters = get_filters_ca(contragents, filters)
    filters += build_filters(contragents, cu_filters)
    if user:
        if user.status:
            sort_list = sort.split(":")
            if sort_list[0] not in ["created_at", "updated_at"]:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )

            q = (
                select(func.count(contragents.c.id))
                .where(
                    contragents.c.cashbox == user.cashbox_id,
                    contragents.c.is_deleted == False,
                )
                .filter(*filters)
            )
            count = await database.fetch_one(q)

            query = contragents.select()
            if add_tags is True:
                query = (
                    select(
                        contragents,
                        func.coalesce(
                            cast(
                                func.jsonb_agg(
                                    func.jsonb_build_object(
                                        literal_column("'id'"),
                                        tags.c.id,
                                        literal_column("'name'"),
                                        tags.c.name,
                                        literal_column("'emoji'"),
                                        tags.c.emoji,
                                        literal_column("'color'"),
                                        tags.c.color,
                                        literal_column("'description'"),
                                        tags.c.description,
                                    )
                                ).filter(tags.c.id.isnot(None)),
                                JSONB,
                            ),
                            literal_column("'[]'::jsonb"),
                        ).label("tags"),
                    )
                    .select_from(
                        contragents.outerjoin(
                            contragents_tags,
                            contragents.c.id == contragents_tags.c.contragent_id,
                        ).outerjoin(tags, contragents_tags.c.tag_id == tags.c.id)
                    )
                    .group_by(contragents.c.id)
                )
            if sort_list[1] == "desc":
                q = (
                    query.where(
                        contragents.c.cashbox == user.cashbox_id,
                        contragents.c.is_deleted == False,
                    )
                    .filter(*filters)
                    .order_by(desc(getattr(contragents.c, sort_list[0])))
                    .offset(offset)
                    .limit(limit)
                )
                payment = await database.fetch_all(q)
                return {"count": count.count_1, "result": payment}
            if sort_list[1] == "asc":
                q = (
                    query.where(
                        contragents.c.cashbox == user.cashbox_id,
                        contragents.c.is_deleted == False,
                    )
                    .filter(*filters)
                    .order_by(asc(getattr(contragents.c, sort_list[0])))
                    .offset(offset)
                    .limit(limit)
                )
                payment = await database.fetch_all(q)
                return {"count": count.count_1, "result": payment}
            else:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


async def create_contragent(
    token: str,
    ca_body: Union[ca_schemas.ContragentCreate, List[ca_schemas.ContragentCreate]],
):
    """Создание контрагента"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            is_multi_create = type(ca_body) is list

            ca_list = []
            ca_body = ca_body if is_multi_create else [ca_body]
            for ca in ca_body:
                update_dict = ca.dict(exclude_unset=True)

                phone_number = update_dict["phone"]
                phone_code = None
                is_phone_formatted = False

                if phone_number:
                    number_phone_parsed = phonenumbers.parse(phone_number, "RU")
                    phone_number = phonenumbers.format_number(
                        number_phone_parsed, phonenumbers.PhoneNumberFormat.E164
                    )
                    phone_code = geocoder.description_for_number(
                        number_phone_parsed, "en"
                    )
                    is_phone_formatted = True
                    if not phone_code:
                        phone_number = update_dict["phone"]
                        is_phone_formatted = False
                    q = contragents.select().where(
                        contragents.c.cashbox == user.cashbox_id,
                        contragents.c.phone == phone_number,
                    )

                    contr = await database.fetch_one(q)
                    if contr:
                        continue

                update_dict["phone"] = phone_number
                update_dict["phone_code"] = phone_code
                update_dict["is_phone_formatted"] = is_phone_formatted
                update_dict["cashbox"] = user.cashbox_id
                update_dict["is_deleted"] = False
                update_dict["updated_at"] = int(datetime.utcnow().timestamp())
                update_dict["created_at"] = int(datetime.utcnow().timestamp())

                ca_list.append(update_dict)

            if not ca_list:
                return Response(status_code=204)

            q = (
                contragents.insert()
                .values(ca_list)
                .returning(
                    contragents.c.id,
                    contragents.c.name,
                    contragents.c.inn,
                    contragents.c.phone,
                    contragents.c.description,
                    contragents.c.contragent_type,
                    contragents.c.birth_date,
                    contragents.c.data,
                )
            )
            new_ca = await database.fetch_all(q)

            for ca in new_ca:
                await manager.send_message(
                    token,
                    {"action": "create", "target": "contragents", "result": dict(ca)},
                )

            return new_ca if is_multi_create else new_ca[0]

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/contragents/{id}/")
async def get_contragent_by_id(token: str, id: int):
    """Получение контрагента по ID"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:

            q = contragents.select().where(
                contragents.c.id == id, contragents.c.cashbox == user.cashbox_id
            )
            ca = await database.fetch_one(q)

            if ca:
                return ca
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Вы ввели несуществующего контрагента либо он не принадлежит вам!",
                )

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.put("/contragents/{id}/")
async def update_contragent_data(
    token: str, ca_body: ca_schemas.ContragentEdit, id: int
):
    """Обновление контрагента"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:

            update_dict = ca_body.dict(exclude_unset=True)
            tag_ids = update_dict.pop("tags_id", None)

            q = (
                contragents.update()
                .where(
                    contragents.c.id == id,
                    contragents.c.cashbox == user.cashbox_id,
                    contragents.c.is_deleted == False,
                )
                .values(update_dict)
            )
            await database.execute(q)

            q = contragents.select().where(
                contragents.c.id == id,
                contragents.c.cashbox == user.cashbox_id,
                contragents.c.is_deleted == False,
            )
            ca = await database.fetch_one(q)

            # Обрабатываем теги и обновляем в бд contragent_tags
            if ca and tag_ids is not None:
                tag_ids = list(set(tag_ids))
                if tag_ids:
                    q = select(tags.c.id).where(
                        tags.c.cashbox_id == user.cashbox_id, tags.c.id.in_(tag_ids)
                    )
                    existed_tags = await database.fetch_all(q)
                    existed_tag_ids = {row.id for row in existed_tags}
                    missing_tag_ids = set(tag_ids) - existed_tag_ids
                    if missing_tag_ids:
                        raise HTTPException(
                            status_code=400,
                            detail={
                                "message": "Теги не найдены",
                                "tags_id": list(missing_tag_ids),
                            },
                        )

                q = contragents_tags.delete().where(
                    contragents_tags.c.contragent_id == id,
                    contragents_tags.c.cashbox_id == user.cashbox_id,
                )
                await database.execute(q)

                if tag_ids:
                    q = contragents_tags.insert().values(
                        [
                            {
                                "tag_id": tag_id,
                                "contragent_id": id,
                                "cashbox_id": user.cashbox_id,
                            }
                            for tag_id in tag_ids
                        ]
                    )
                    await database.execute(q)

            if ca:
                await manager.send_message(
                    token,
                    {"action": "edit", "target": "contragents", "result": dict(ca)},
                )
                return ca
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Вы ввели несуществующего контрагента либо он не принадлежит вам!",
                )

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.delete("/contragents/{id}/")
async def delete_contragent(token: str, id: int):
    """Удаление контрагента"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:

            try:
                q = (
                    contragents.update()
                    .where(
                        contragents.c.id == id, contragents.c.cashbox == user.cashbox_id
                    )
                    .values({"is_deleted": True})
                )
                await database.execute(q)
            except ForeignKeyViolationError:
                return {
                    "error": "К данному контрагенту привязан платеж, удаление невозможно!"
                }

            return {"result": "OK"}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")
