import io
import logging
import re
from collections import defaultdict
from datetime import datetime
from os import environ
from time import perf_counter
from typing import Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

import aioboto3
import api.pictures.schemas as schemas
from common.utils.url_helper import get_app_url_for_environment
from database import db
from database.db import database, pictures
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import RedirectResponse, Response
from functions.filter_schemas import PicturesFiltersQuery
from functions.helpers import (
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
)
from PIL import Image
from sqlalchemy import func, select
from ws_manager import manager

# Поддерживаемые MIME-типы и соответствующие расширения
ALLOWED_CONTENT_TYPES = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "application/pdf": "pdf",
}


def build_public_url(picture_id: int) -> str:
    base = get_app_url_for_environment()
    if not base:
        raise ValueError("APP_URL не настроен для текущего окружения")
    base = base.rstrip("/")
    # Добавляем протокол, если его нет
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    return f"{base}/api/v1/pictures/{picture_id}/content"


logger = logging.getLogger(__name__)
router = APIRouter(tags=["pictures"])

s3_session = aioboto3.Session()

s3_data = {
    "service_name": "s3",
    "endpoint_url": environ.get("S3_URL"),
    "aws_access_key_id": environ.get("S3_ACCESS"),
    "aws_secret_access_key": environ.get("S3_SECRET"),
}

bucket_name = "5075293c-docs_generated"


def _photo_extension_from_bytes(data: bytes) -> str:
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    if data[:4] == b"\x89PNG\r\n":
        return "png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    return "jpg"


async def create_picture_from_bytes(
    file_bytes: bytes,
    entity: str,
    entity_id: int,
    owner_id: int,
    cashbox_id: int,
    is_main: bool = False,
    content_type: Optional[str] = None,
):
    """Единый метод: загрузка в S3 + запись в БД. Возвращает (picture_id, file_key)."""
    ext = ALLOWED_CONTENT_TYPES.get(content_type or "") or _photo_extension_from_bytes(
        file_bytes
    )
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    path = f"{now:%Y/%m/%d}/{cashbox_id}/{uuid4().hex}.{ext}"
    file_key = f"photos/{path}"

    async with s3_session.client(**s3_data) as s3:
        await s3.upload_fileobj(io.BytesIO(file_bytes), bucket_name, file_key)

    picture_values = {
        "entity": entity,
        "entity_id": entity_id,
        "is_main": is_main,
        "owner": owner_id,
        "url": file_key,
        "size": len(file_bytes),
        "cashbox": cashbox_id,
        "is_deleted": False,
    }
    query = pictures.insert().values(picture_values).returning(pictures.c.id)
    picture_id = await database.execute(query)
    return picture_id, file_key


async def get_picture_by_filename(filename: str, cashbox_id: int):
    """Найти картинку по имени файла и cashbox_id"""
    safe_filename = filename.replace("%", "\\%").replace("_", "\\_")
    query = pictures.select().where(
        pictures.c.url.like(f"%/{safe_filename}", escape="\\"),
        pictures.c.cashbox == cashbox_id,
        pictures.c.is_deleted.is_not(True),
    )
    return await database.fetch_one(query)


@router.get("/pictures/{idx}/", response_model=schemas.Picture)
async def get_picture_by_id(token: str, idx: int):
    """Получение картинки по ID"""
    user = await get_user_by_token(token)
    picture_db = await get_entity_by_id(pictures, idx, user.cashbox_id)
    picture_db = datetime_to_timestamp(picture_db)
    picture_db["public_url"] = build_public_url(idx)
    return picture_db


@router.get("/photos-tilda/{filename:path}")
async def get_picture_by_filename_for_tilda(filename: str):
    """Публичный доступ к фото для Tilda.

    Отдаёт то же самое, что и `/photos/{filename:path}`, но с принудительной
    оптимизацией (1680px, <3MB). Это нужно, т.к. некоторые импортеры могут
    отбрасывать query-параметры (например `?optimize=true`).
    """

    return await get_picture_by_filename(filename=filename, optimize=True)


@router.get("/photos/{filename:path}")
async def get_picture_by_filename(
    filename: str,
    optimize: bool = Query(
        False, description="Оптимизировать для Tilda (1680px, <3MB)"
    ),
):
    """Публичный доступ к фото по имени файла (обратная совместимость со старыми URL)"""
    # Поддерживаем как старый формат (nomenclature_39663_78d9b9b5.jpg),
    # так и новый формат с путями (2025/12/21/4/938bd650df9248aabc21c0be8edc35e2.jpg)

    # Убираем начальный и завершающий слэш, если есть
    filename = filename.strip("/")

    # Проверяем, что файл имеет допустимое расширение
    if not re.search(r"\.(jpg|jpeg|png|gif|pdf)$", filename, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Недопустимое расширение файла")

    # Если путь уже содержит "photos/", используем его как есть
    if filename.startswith("photos/"):
        file_key = filename
    else:
        file_key = f"photos/{filename}"

    async with s3_session.client(**s3_data) as s3:
        try:
            s3_obj = await s3.get_object(Bucket=bucket_name, Key=file_key)
            body = await s3_obj["Body"].read()
        except Exception as e:
            print(f"S3 error for {file_key}: {e}")
            raise HTTPException(status_code=404, detail="Файл не найден")

    # Определяем MIME-type
    if filename.lower().endswith(".png"):
        media_type = "image/png"
    elif filename.lower().endswith(".gif"):
        media_type = "image/gif"
    elif filename.lower().endswith(".pdf"):
        media_type = "application/pdf"
    else:
        media_type = "image/jpeg"

    # tablecrm #999: Resize/compress image if it exceeds Tilda limits (3MB or 1680px)
    if optimize and media_type in ("image/jpeg", "image/png"):
        try:
            # Check limits
            is_too_large_size = len(body) > 3 * 1024 * 1024  # > 3MB

            # We open image to check dimensions
            img = Image.open(io.BytesIO(body))
            width, height = img.size
            max_side = max(width, height)

            if max_side > 1680 or is_too_large_size:
                logger.info(
                    f"Resizing image {filename}: size={len(body)}, dims={width}x{height}"
                )

                # Calculate new dimensions if needed
                if max_side > 1680:
                    ratio = 1680 / max_side
                    new_size = (int(width * ratio), int(height * ratio))
                    img = img.resize(
                        new_size, getattr(Image, "Resampling", Image).LANCZOS
                    )

                # Convert RGBA to RGB for JPEG
                if media_type == "image/jpeg" and img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")

                # Save to buffer
                out_buffer = io.BytesIO()
                format_name = "PNG" if media_type == "image/png" else "JPEG"

                # Optimize to reduce file size
                save_kwargs = {"optimize": True}
                if format_name == "JPEG":
                    save_kwargs["quality"] = 85

                img.save(out_buffer, format=format_name, **save_kwargs)
                processed_body = out_buffer.getvalue()

                # Only use processed result if we actually reduced size or were forced to resize dimensions
                # (sometimes optimization might increase size for already optimized small files, though unlikely with resize)
                if max_side > 1680 or len(processed_body) < len(body):
                    body = processed_body
                    logger.info(f"Resized complete {filename}: new_size={len(body)}")

        except Exception as e:
            logger.error(
                f"Failed to resize/optimize image {filename}: {e}", exc_info=True
            )
            # Fallback to original body
            pass

    return Response(content=body, media_type=media_type)


@router.get("/photos/link/{filename}/")
async def get_picture_link_by_id(filename: str):
    """Публичная пресайгнутая ссылка по имени файла (обратная совместимость)"""
    if not re.match(
        r"^[a-zA-Z0-9_\-\.]+\.(jpg|jpeg|png|gif|pdf)$", filename, re.IGNORECASE
    ):
        raise HTTPException(400, "Недопустимое имя файла")

    file_key = f"photos/{filename}"

    async with s3_session.client(**s3_data) as s3:
        try:
            url = await s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": file_key},
                ExpiresIn=3600,
            )
        except Exception as e:
            print(f"S3 presign error for {file_key}: {e}")
            raise HTTPException(500, "Не удалось сгенерировать ссылку")

    return {"data": {"url": url}}


@router.get("/pictures/", response_model=schemas.PictureListGet)
async def get_pictures(
    token: str,
    limit: int = 100,
    offset: int = 0,
    filters: PicturesFiltersQuery = Depends(),
):
    """Получение списка картинок"""
    user = await get_user_by_token(token)

    filters_list = []
    if filters.entity:
        filters_list.append(pictures.c.entity == filters.entity)
    if filters.entity_id:
        filters_list.append(pictures.c.entity_id == filters.entity_id)

    query = (
        pictures.select()
        .where(
            pictures.c.cashbox == user.cashbox_id,
            pictures.c.is_deleted.is_not(True),
            *filters_list,
        )
        .limit(limit)
        .offset(offset)
    )

    pictures_db = await database.fetch_all(query)
    pictures_db = [
        {**datetime_to_timestamp(p), "public_url": build_public_url(p["id"])}
        for p in pictures_db
    ]

    query = select(func.count(pictures.c.id)).where(
        pictures.c.cashbox == user.cashbox_id,
        pictures.c.is_deleted.is_not(True),
        *filters_list,
    )

    pictures_db_c = await database.fetch_one(query)

    return {"result": pictures_db, "count": pictures_db_c.count_1}


@router.post("/pictures/", response_model=schemas.Picture)
async def new_picture(
    token: str,
    entity: str,
    entity_id: int,
    is_main: bool = False,
    file: UploadFile = File(None),
):
    """Создание картинки с организацией по дате и cashbox_id"""
    if not file:
        raise HTTPException(status_code=422, detail="Вы не загрузили картинку.")
    if file.content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=422,
            detail="Неподдерживаемый тип файла. Разрешены: JPEG, PNG, GIF, PDF.",
        )

    if entity not in dir(db):
        raise HTTPException(status_code=422, detail="Такой entity не существует")

    user = await get_user_by_token(token)
    file_bytes = await file.read()

    try:
        picture_id, file_key = await create_picture_from_bytes(
            file_bytes,
            entity=entity,
            entity_id=entity_id,
            owner_id=user.id,
            cashbox_id=user.cashbox_id,
            is_main=is_main,
            content_type=file.content_type,
        )
    except Exception as e:
        print(f"Ошибка загрузки в S3/БД: {e}")
        raise HTTPException(
            status_code=502,
            detail="Не удалось сохранить изображение на сервере. Повторите попытку позже.",
        )

    try:
        query = pictures.select().where(
            pictures.c.id == picture_id,
            pictures.c.cashbox == user.cashbox_id,
            pictures.c.is_deleted.is_not(True),
        )
        picture_db = await database.fetch_one(query)

        if not picture_db:
            # Теоретически не должно происходить, но на всякий случай
            raise HTTPException(
                status_code=500, detail="Ошибка при сохранении метаданных изображения."
            )

        picture_db = datetime_to_timestamp(picture_db)
        picture_db["public_url"] = build_public_url(picture_id)

        await manager.send_message(
            token,
            {
                "action": "create",
                "target": "pictures",
                "result": picture_db,
            },
        )

        return picture_db

    except Exception as db_err:
        # попытка удалить файл из S3 при ошибке БД (желательно)
        try:
            async with s3_session.client(**s3_data) as s3:
                await s3.delete_object(Bucket=bucket_name, Key=file_key)
        except Exception as cleanup_err:
            print(f"Не удалось удалить файл после ошибки БД: {cleanup_err}")
        raise HTTPException(
            status_code=500,
            detail="Ошибка при сохранении данных изображения. Файл отменён.",
        )


@router.patch("/pictures/{idx}/", response_model=schemas.Picture)
async def edit_picture(
    token: str,
    idx: int,
    picture: schemas.PictureEdit,
):
    """Редактирование картинки"""
    user = await get_user_by_token(token)
    picture_db = await get_entity_by_id(pictures, idx, user.cashbox_id)
    picture_values = picture.dict(exclude_unset=True)

    if picture_values:
        query = (
            pictures.update()
            .where(pictures.c.id == idx, pictures.c.cashbox == user.cashbox_id)
            .values(picture_values)
        )
        await database.execute(query)
        picture_db = await get_entity_by_id(pictures, idx, user.cashbox_id)

    picture_db = datetime_to_timestamp(picture_db)
    picture_db["public_url"] = build_public_url(idx)

    await manager.send_message(
        token,
        {"action": "edit", "target": "pictures", "result": picture_db},
    )

    return picture_db


@router.get("/pictures/{picture_id}/content")
async def get_picture_content(picture_id: int):
    """Публичный доступ к фото товара (без токена). URL в БД — S3 key или внешний http(s)."""
    query = pictures.select().where(
        pictures.c.id == picture_id,
        pictures.c.is_deleted.is_not(True),
    )
    picture = await database.fetch_one(query)
    if not picture:
        raise HTTPException(404, "Фотография не найдена")

    url = (picture.get("url") or "").strip()
    if url.startswith(("http://", "https://")):
        return RedirectResponse(url)
    if not url.startswith("photos/"):
        raise HTTPException(404, "Фотография не найдена")

    async with s3_session.client(**s3_data) as s3:
        presigned_url = await s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": url},
            ExpiresIn=3600,
        )
    return RedirectResponse(presigned_url)


@router.delete("/pictures/{idx}/", response_model=schemas.Picture)
async def delete_picture(token: str, idx: int):
    """Удаление картинки"""
    user = await get_user_by_token(token)

    await get_entity_by_id(pictures, idx, user.cashbox_id)

    query = (
        pictures.update()
        .where(pictures.c.id == idx, pictures.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = pictures.select().where(
        pictures.c.id == idx, pictures.c.cashbox == user.cashbox_id
    )
    picture_db = await database.fetch_one(query)
    picture_db = datetime_to_timestamp(picture_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "pictures",
            "result": picture_db,
        },
    )

    return picture_db


@router.post("/pictures/batch/", response_model=schemas.PictureBatchResponse)
async def get_pictures_batch(token: str, payload: schemas.PictureBatchRequest):
    """Получение картинок для нескольких entity_id пачкой"""
    started_at = perf_counter()

    if not payload.entity_ids:
        return {
            "result": {},
            "count": 0,
            "processing_time_ms": int((perf_counter() - started_at) * 1000),
        }

    user = await get_user_by_token(token)

    # Убираем дубликаты, сохраняем порядок
    entity_ids = list(dict.fromkeys(payload.entity_ids))

    query = (
        pictures.select()
        .where(
            pictures.c.cashbox == user.cashbox_id,
            pictures.c.entity == payload.entity,
            pictures.c.entity_id.in_(entity_ids),
            pictures.c.is_deleted.is_not(True),
        )
        .order_by(
            pictures.c.entity_id,
            pictures.c.is_main.desc(),
            pictures.c.id.asc(),
        )
    )

    rows = await database.fetch_all(query)

    grouped = defaultdict(list)

    for row in rows:
        row = datetime_to_timestamp(row)
        row["public_url"] = build_public_url(row["id"])
        grouped[row["entity_id"]].append(row)

    # возвращаем ВСЕ entity_id, даже без картинок
    result = {entity_id: grouped.get(entity_id, []) for entity_id in entity_ids}

    return {
        "result": result,
        "count": len(rows),
        "processing_time_ms": int((perf_counter() - started_at) * 1000),
    }
