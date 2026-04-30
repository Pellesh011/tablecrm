import os
from datetime import datetime
from typing import Optional

import api.cheques.schemas as cheque_schemas
import functions.filter_schemas as filter_schemas
import requests
from database.db import cheques, database, users_cboxes_relation
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from functions.helpers import get_filters_cheques
from sqlalchemy import func, select

router = APIRouter(tags=["FNS-Check"])


@router.get("/cheques/{id}/", response_model=cheque_schemas.Cheque)
async def get_cheque_by_id(token: str, id: int):
    """Получение чека по ID"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = cheques.select().where(
                cheques.c.id == id, cheques.c.cashbox == user.cashbox_id
            )
            article_db = await database.fetch_one(query)

            if article_db:
                return article_db
            else:
                raise HTTPException(
                    status_code=404, detail="У вас нет чека с таким id."
                )

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/cheques/", response_model=cheque_schemas.ChequesGet)
async def get_cheques(
    token: str,
    limit: int = 100,
    offset: int = 0,
    filters: filter_schemas.ChequesFiltersQuery = Depends(),
):
    """Получение чеков кассы"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            filters = get_filters_cheques(cheques, filters)
            query = (
                cheques.select()
                .where(cheques.c.cashbox == user.cashbox_id)
                .filter(*filters)
                .limit(limit)
                .offset(offset)
            )

            cheques_db = await database.fetch_all(query)

            q = (
                select(func.count(cheques.c.id))
                .where(cheques.c.cashbox == user.cashbox_id)
                .filter(*filters)
            )
            cheques_db_count = await database.fetch_one(q)

            return {"result": cheques_db, "count": cheques_db_count.count_1}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.post("/cheques/", response_model=cheque_schemas.Cheque)
async def new_cheque(
    token: str, raw_qr: str = "", qrfile: Optional[UploadFile] = File(None)
):
    """Создание чека"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            req_data = {"token": os.getenv("CHEQUES_TOKEN")}
            if raw_qr:
                req_data["qrraw"] = raw_qr
                req_files = {}
            elif qrfile:
                file_bytes = await qrfile.read()
                req_files = {"qrfile": file_bytes}
            else:
                raise HTTPException(
                    400, "Значение не получено. Введите или строку, или картинку."
                )
            req = requests.post(
                "https://proverkacheka.com/api/v1/check/get",
                data=req_data,
                files=req_files,
            )
            data = req.json()
            if data["code"] != 1:
                raise HTTPException(
                    400,
                    f"Не удалось получить данные по чеку. Код ответа: {data['code']}",
                )
            cheque_data = data["data"]["json"]

            created = int(datetime.utcnow().timestamp())
            cheque_values = {
                "data": cheque_data,
                "created_at": created,
                "cashbox": user.cashbox_id,
                "user": user.user,
            }

            query = cheques.insert().values(cheque_values)
            cheque_id = await database.execute(query)

            return {
                "id": cheque_id,
                "data": cheque_data,
                "created_at": created,
                "user": user.user,
            }

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")
