import io
from datetime import datetime
from os import environ
from uuid import uuid4
from zoneinfo import ZoneInfo

import aioboto3
from common.utils.url_helper import get_app_url_for_environment
from database.db import database, files, users_cboxes_relation
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import RedirectResponse
from functions.helpers import datetime_to_timestamp, get_user_by_token
from sqlalchemy import and_, func, or_, select
from ws_manager import manager

from .schemas import (
    FileCreate,
    FileFiltersQuery,
    FileListResponse,
    FileResponse,
    FileUpdate,
)

s3_session = aioboto3.Session()
s3_data = {
    "service_name": "s3",
    "endpoint_url": environ.get("S3_URL"),
    "aws_access_key_id": environ.get("S3_ACCESS"),
    "aws_secret_access_key": environ.get("S3_SECRET"),
}
bucket_name = "5075293c-docs_generated"

ALLOWED_CONTENT_TYPES = {
    # Документы
    "application/pdf": "pdf",
    "application/msword": "doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "text/plain": "txt",
    "text/csv": "csv",
    "application/json": "json",
    "application/zip": "zip",
    # Изображения (чеки, сканы счетов и т.д.)
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    # Медиа
    "text/x-diff": "diff",
    "audio/mpeg": "mp3",
    "video/mp4": "mp4",
}


def build_public_url(file_id: int) -> str:
    base = get_app_url_for_environment()
    if not base:
        raise ValueError("APP_URL не настроен для текущего окружения")
    base = base.rstrip("/")
    # Добавляем протокол, если его нет
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    return f"{base}/api/v1/files/{file_id}/content"


router = APIRouter(tags=["files"])


def _get_sort_column(sort: str):
    if sort == "updated_at_asc":
        return files.c.updated_at.asc()
    elif sort == "updated_at_desc":
        return files.c.updated_at.desc()
    elif sort == "created_at_asc":
        return files.c.created_at.asc()
    else:  # created_at_desc
        return files.c.created_at.desc()


@router.post("/files/", response_model=FileResponse)
async def upload_file(
    token: str,
    file: UploadFile = File(...),
    data: str = Query(..., description="JSON with title, description, tags"),
):
    """
    Загрузить файл с метаданными.
    Пример: ?data={"title":"Счёт","tags":["invoice","2025"]}
    """
    user = await get_user_by_token(token)

    if not file.content_type or file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(422, "Неподдерживаемый тип файла")

    try:
        file_meta = FileCreate.parse_raw(data)
    except Exception as e:
        raise HTTPException(422, f"Некорректные метаданные: {str(e)}")

    file_bytes = await file.read()

    # Проверка размера
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
    if len(file_bytes) > MAX_FILE_SIZE:
        raise HTTPException(413, "Файл слишком большой")

    # Генерация пути
    moscow_tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(moscow_tz)
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    cashbox_id = str(user.cashbox_id)
    ext = ALLOWED_CONTENT_TYPES[file.content_type]
    filename = f"{uuid4().hex}.{ext}"
    file_key = f"files/{year}/{month}/{day}/{cashbox_id}/{filename}"

    # Загрузка в S3
    try:
        async with s3_session.client(**s3_data) as s3:
            await s3.upload_fileobj(io.BytesIO(file_bytes), bucket_name, file_key)
    except Exception as e:
        print(f"S3 upload failed: {e}")
        raise HTTPException(502, "Не удалось сохранить файл")

    # Сохранение в БД
    tags_list = file_meta.tags.__root__ if file_meta.tags else None
    values = {
        "title": file_meta.title,
        "description": file_meta.description,
        "tags": tags_list,
        "url": file_key,
        "size": len(file_bytes),
        "mime_type": file.content_type,
        "extension": ext,
        "owner": user.id,
        "cashbox": user.cashbox_id,
        "is_deleted": False,
        "created_at": now,
        "updated_at": now,
    }

    try:
        query = files.insert().values(values)
        file_id = await database.execute(query)

        query = files.select().where(files.c.id == file_id)
        file_db = await database.fetch_one(query)
        result = datetime_to_timestamp(file_db)
        result["public_url"] = build_public_url(file_id)

        await manager.send_message(
            token, {"action": "create", "target": "files", "result": result}
        )
        return result

    except Exception:
        # Откат из S3
        try:
            async with s3_session.client(**s3_data) as s3:
                await s3.delete_object(Bucket=bucket_name, Key=file_key)
        except Exception as cleanup_err:
            print(f"Cleanup error: {cleanup_err}")
        raise HTTPException(500, "Ошибка сохранения метаданных")


@router.get("/files/", response_model=FileListResponse)
async def list_files(
    token: str,
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    filters: FileFiltersQuery = Depends(),
):
    user = await get_user_by_token(token)

    conditions = [
        files.c.owner == user.id,
        files.c.is_deleted.is_not(True),
    ]

    if filters.search:
        pattern = f"%{filters.search}%"
        conditions.append(
            or_(files.c.title.ilike(pattern), files.c.description.ilike(pattern))
        )

    if filters.tags:
        for tag in [t.strip().lower() for t in filters.tags.split(",") if t.strip()]:
            conditions.append(files.c.tags.contains([tag]))

    def ts_to_dt(ts: int):
        return datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))

    if filters.created_from:
        conditions.append(files.c.created_at >= ts_to_dt(filters.created_from))
    if filters.created_to:
        conditions.append(files.c.created_at <= ts_to_dt(filters.created_to))
    if filters.updated_from:
        conditions.append(files.c.updated_at >= ts_to_dt(filters.updated_from))
    if filters.updated_to:
        conditions.append(files.c.updated_at <= ts_to_dt(filters.updated_to))

    query = (
        select(files)
        .where(and_(*conditions))
        .order_by(_get_sort_column(filters.sort))
        .limit(limit)
        .offset(offset)
    )
    db_files = await database.fetch_all(query)
    db_files = [
        {**datetime_to_timestamp(f), "public_url": build_public_url(f["id"])}
        for f in db_files
    ]

    count_query = select(func.count(files.c.id)).where(and_(*conditions))
    total = await database.fetch_one(count_query)

    return FileListResponse(result=db_files, count=total.count_1)


@router.get("/files/{file_id}/", response_model=FileResponse)
async def get_file(token: str, file_id: int):
    user = await get_user_by_token(token)
    query = files.select().where(
        files.c.id == file_id, files.c.owner == user.id, files.c.is_deleted.is_not(True)
    )
    file = await database.fetch_one(query)
    if not file:
        raise HTTPException(404, "Файл не найден")
    result = datetime_to_timestamp(file)
    result["public_url"] = build_public_url(file_id)
    return result


@router.patch("/files/{file_id}/", response_model=FileResponse)
async def update_file(token: str, file_id: int, update: FileUpdate):
    user = await get_user_by_token(token)
    existing = await database.fetch_one(
        files.select().where(
            files.c.id == file_id,
            files.c.owner == user.id,
        )
    )
    if not existing:
        raise HTTPException(404, "Файл не найден")

    update_data = {}
    if update.title is not None:
        update_data["title"] = update.title
    if update.description is not None:
        update_data["description"] = update.description
    if update.tags is not None:
        update_data["tags"] = update.tags.__root__
    if update_data:
        update_data["updated_at"] = datetime.now(ZoneInfo("Europe/Moscow"))
        await database.execute(
            files.update().where(files.c.id == file_id).values(update_data)
        )

    updated = await database.fetch_one(files.select().where(files.c.id == file_id))
    result = datetime_to_timestamp(updated)
    await manager.send_message(
        token, {"action": "update", "target": "files", "result": result}
    )
    result["public_url"] = build_public_url(file_id)
    return result


@router.delete("/files/{file_id}/", response_model=FileResponse)
async def delete_file(token: str, file_id: int):
    user = await get_user_by_token(token)
    file = await database.fetch_one(
        files.select().where(
            files.c.id == file_id,
            files.c.owner == user.id,
        )
    )
    if not file:
        raise HTTPException(404, "Файл не найден")

    await database.execute(
        files.update().where(files.c.id == file_id).values(is_deleted=True)
    )

    result = datetime_to_timestamp({**dict(file), "is_deleted": True})
    await manager.send_message(
        token, {"action": "delete", "target": "files", "result": result}
    )
    return result


from fastapi.responses import RedirectResponse


@router.get("/files/{file_id}/content")
async def get_file_content(token: str, file_id: int):
    """Доступ к файлу с проверкой владельца (требуется токен)"""
    try:
        user = await get_user_by_token(token)
    except HTTPException as e:
        # Логируем ошибку для диагностики
        print(f"Token validation failed for file_id={file_id}, error: {e.detail}")
        raise
    query = files.select().where(
        files.c.id == file_id,
        files.c.is_deleted.is_not(True),
    )
    file = await database.fetch_one(query)

    owner_query = (
        users_cboxes_relation.select()
        .where(
            users_cboxes_relation.c.id == file.owner,
        )
        .limit(1)
    )
    file_owner = await database.fetch_one(owner_query)

    if file_owner.cashbox_id != user.cashbox_id:
        raise HTTPException(404, "Файл не найден или доступ запрещён")

    async with s3_session.client(**s3_data) as s3:
        presigned_url = await s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": file["url"]},
            ExpiresIn=3600,  # 1 час
        )
    return RedirectResponse(presigned_url)
