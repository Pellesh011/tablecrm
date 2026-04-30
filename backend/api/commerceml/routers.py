"""Роутеры для управления CommerceML подключениями"""

import logging
import os
import secrets
from typing import List, Optional

import aioboto3
from botocore.exceptions import ClientError
from database.db import commerceml_connections, database
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from functions.helpers import get_user_by_token
from sqlalchemy import and_, func, select

from . import schemas

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter(tags=["commerceml"])


@router.get(
    "/commerceml/connections", response_model=List[schemas.CommerceMLConnection]
)
async def get_connections(token: str):
    """Получить список CommerceML подключений"""
    user = await get_user_by_token(token)

    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )

    rows = await database.fetch_all(query)
    result = []
    for row in rows:
        row_dict = _row_to_connection_response(dict(row))
        result.append(row_dict)
    return result


def _row_to_connection_response(row_dict: dict) -> dict:
    """Приводит строку БД к формату ответа API: datetime → строка, bool-поля гарантированно bool."""
    if row_dict.get("created_at"):
        row_dict["created_at"] = (
            row_dict["created_at"].isoformat()
            if hasattr(row_dict["created_at"], "isoformat")
            else str(row_dict["created_at"])
        )
    if row_dict.get("updated_at"):
        row_dict["updated_at"] = (
            row_dict["updated_at"].isoformat()
            if hasattr(row_dict["updated_at"], "isoformat")
            else str(row_dict["updated_at"])
        )
    for key in (
        "active",
        "import_products",
        "export_products",
        "import_orders",
        "export_orders",
    ):
        if key in row_dict:
            row_dict[key] = bool(row_dict[key])
    return row_dict


def _parse_bool_from_body(body: dict, key: str) -> bool:
    """Берём значение из сырого тела запроса: false остаётся false."""
    v = body.get(key)
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "да")
    return bool(v)


@router.get("/commerceml/connections/{idx}/files")
async def list_commerceml_files(
    idx: int,
    token: str,
    limit: int = Query(100, ge=1, le=1000),
    continuation_token: Optional[str] = None,
    user=Depends(get_user_by_token),
):
    """
    Получить список файлов, загруженных в S3 для CommerceML-подключения.
    Каждый объект содержит presigned URL для скачивания (действует 1 час).
    """
    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == idx,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )
    connection = await database.fetch_one(query)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    s3_access = os.getenv("S3_ACCESS")
    s3_secret = os.getenv("S3_SECRET")
    s3_url = os.getenv("S3_URL")
    bucket = "commerceml-files"

    if not all([s3_access, s3_secret, s3_url]):
        raise HTTPException(status_code=500, detail="S3 configuration is incomplete")

    prefix = f"commerceml/{idx}/"

    try:
        session = aioboto3.Session(
            aws_access_key_id=s3_access,
            aws_secret_access_key=s3_secret,
        )
        async with session.client("s3", endpoint_url=s3_url) as s3:
            list_params = {
                "Bucket": bucket,
                "Prefix": prefix,
                "MaxKeys": limit,
            }
            if continuation_token:
                list_params["ContinuationToken"] = continuation_token

            response = await s3.list_objects_v2(**list_params)

            objects = []
            for obj in response.get("Contents", []):
                key = obj["Key"]
                filename = key.replace(prefix, "", 1) if key.startswith(prefix) else key

                # Генерируем presigned URL (действителен 1 час)
                try:
                    download_url = await s3.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": bucket, "Key": key},
                        ExpiresIn=3600,
                    )
                except Exception as e:
                    logger.warning(f"Failed to generate presigned URL for {key}: {e}")
                    download_url = None

                objects.append(
                    {
                        "key": key,
                        "filename": filename,
                        "size": obj["Size"],
                        "last_modified": (
                            obj["LastModified"].isoformat()
                            if obj.get("LastModified")
                            else None
                        ),
                        "etag": obj.get("ETag", "").strip('"'),
                        "download_url": download_url,
                    }
                )

            result = {
                "objects": objects,
                "is_truncated": response.get("IsTruncated", False),
                "next_continuation_token": response.get("NextContinuationToken"),
                "count": len(objects),
                "prefix": prefix,
                "bucket": bucket,
            }
            return result

    except ClientError as e:
        logger.error(f"S3 list objects error: {e}")
        raise HTTPException(status_code=500, detail="Failed to list files from S3")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/commerceml/connections", response_model=schemas.CommerceMLConnection)
async def create_connection(token: str, request: Request):
    """Создать новое CommerceML подключение."""
    user = await get_user_by_token(token)
    body = await request.json()
    logger.info(
        "commerceml create body: active=%s import_products=%s export_products=%s import_orders=%s export_orders=%s",
        body.get("active"),
        body.get("import_products"),
        body.get("export_products"),
        body.get("import_orders"),
        body.get("export_orders"),
    )

    name = (body.get("name") or "").strip()
    url = (body.get("url") or "").strip()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""

    if not name:
        raise HTTPException(status_code=400, detail="name обязателен")

    max_id_q = select(func.coalesce(func.max(commerceml_connections.c.id), 0))
    new_id = await database.fetch_val(max_id_q)
    new_id = (new_id or 0) + 1

    if not username:
        username = f"commerceml_{new_id}_{secrets.token_hex(4)}"
    if not password:
        password = secrets.token_urlsafe(12)

    values = {
        "id": new_id,
        "cashbox_id": user.cashbox_id,
        "name": name,
        "url": url,
        "username": username,
        "password": password,
        "active": _parse_bool_from_body(body, "active"),
        "import_products": _parse_bool_from_body(body, "import_products"),
        "export_products": _parse_bool_from_body(body, "export_products"),
        "import_orders": _parse_bool_from_body(body, "import_orders"),
        "export_orders": _parse_bool_from_body(body, "export_orders"),
        "products_loaded_count": 0,
        "orders_exported_count": 0,
        "is_deleted": False,
    }
    logger.info(
        "commerceml insert values: active=%s import_products=%s export_orders=%s",
        values["active"],
        values["import_products"],
        values["export_orders"],
    )

    query = commerceml_connections.insert().values(**values)
    await database.execute(query)

    # Достаём созданную запись и возвращаем
    sel = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == new_id,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
        )
    )
    row = await database.fetch_one(sel)
    if not row:
        raise HTTPException(
            status_code=500, detail="Connection created but could not be read"
        )
    return _row_to_connection_response(dict(row))


@router.get(
    "/commerceml/connections/{idx}", response_model=schemas.CommerceMLConnection
)
async def get_connection(token: str, idx: int):
    """Получить CommerceML подключение по ID"""
    user = await get_user_by_token(token)

    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == idx,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )

    row = await database.fetch_one(query)
    if not row:
        raise HTTPException(status_code=404, detail="Connection not found")
    return _row_to_connection_response(dict(row))


@router.patch(
    "/commerceml/connections/{idx}", response_model=schemas.CommerceMLConnection
)
async def update_connection(
    token: str, idx: int, data: schemas.CommerceMLConnectionUpdate
):
    """Обновить CommerceML подключение"""
    user = await get_user_by_token(token)

    # Проверяем существование
    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == idx,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )
    row = await database.fetch_one(query)
    if not row:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Обновляем только переданные поля (PATCH); булевы явно приводим к bool
    # Pydantic v2: model_dump(), v1: dict()
    raw = (
        data.model_dump(exclude_unset=True)
        if hasattr(data, "model_dump")
        else data.dict(exclude_unset=True)
    )
    # Фронт может слать active или is_active — в БД только active
    if "is_active" in raw and "active" not in raw:
        raw["active"] = raw.pop("is_active")
    # Только колонки таблицы (не передаём лишнее из тела запроса)
    allowed_keys = {
        "name",
        "url",
        "username",
        "password",
        "active",
        "import_products",
        "export_products",
        "import_orders",
        "export_orders",
    }
    values = {}
    for k, v in raw.items():
        if k not in allowed_keys or v is None:
            continue
        if k in (
            "active",
            "import_products",
            "export_products",
            "import_orders",
            "export_orders",
        ):
            values[k] = bool(v)
        else:
            values[k] = v
    if not values:
        return _row_to_connection_response(dict(row))

    query = (
        commerceml_connections.update()
        .where(commerceml_connections.c.id == idx)
        .values(**values)
    )
    await database.execute(query)
    # Читаем обновлённую строку
    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == idx,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )
    updated_row = await database.fetch_one(query)
    return _row_to_connection_response(dict(updated_row))


@router.delete("/commerceml/connections/{idx}")
async def delete_connection(token: str, idx: int):
    """Удалить CommerceML подключение (мягкое удаление)"""
    user = await get_user_by_token(token)

    # Проверяем существование
    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == idx,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )
    row = await database.fetch_one(query)
    if not row:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Мягкое удаление
    query = (
        commerceml_connections.update()
        .where(commerceml_connections.c.id == idx)
        .values(is_deleted=True)
    )
    await database.execute(query)

    return {"success": True}


@router.post(
    "/commerceml/connections/{idx}/reset-password",
    response_model=schemas.CommerceMLConnection,
)
async def reset_password(
    token: str, idx: int, data: Optional[schemas.CommerceMLResetPassword] = Body(None)
):
    """Сбросить пароль подключения. Если пароль не передан — генерируется случайный."""
    user = await get_user_by_token(token)
    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == idx,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )
    row = await database.fetch_one(query)
    if not row:
        raise HTTPException(status_code=404, detail="Connection not found")

    new_password = (
        data.password if data and getattr(data, "password", None) else None
    ) or secrets.token_urlsafe(16)
    query = (
        commerceml_connections.update()
        .where(commerceml_connections.c.id == idx)
        .values(password=new_password)
        .returning(commerceml_connections)
    )
    updated_row = await database.fetch_one(query)
    return _row_to_connection_response(dict(updated_row))


def _is_settings_sync_error(error: str) -> bool:
    """Ошибки из‑за настроек (URL не задан, тип не разрешён) — отдаём 400."""
    if not error:
        return False
    err = error.lower()
    return (
        "url not set" in err
        or "not set" in err
        or "not enabled" in err
        or "connection not found" in err
    )


@router.post("/commerceml/connections/{idx}/sync")
async def sync_connection(token: str, idx: int, type: str = "catalog"):
    """Синхронизировать данные CommerceML (отправить на указанный URL или подсказка при пустом URL)."""
    user = await get_user_by_token(token)

    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == idx,
            commerceml_connections.c.cashbox_id == user.cashbox_id,
            commerceml_connections.c.is_deleted == False,
        )
    )
    connection = await database.fetch_one(query)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    # «Загрузить товар» (import_catalog) при пустом URL: каталог к нам заливает 1С, подключаясь к нашему API
    if (
        type == "import_catalog"
        and not (getattr(connection, "url", None) or "").strip()
    ):
        return {
            "success": True,
            "message": (
                "Каталог загружает 1С, подключаясь к нашему API. "
                "Укажите в 1С наш URL и учётные данные этого подключения (логин и пароль из карточки)."
            ),
        }

    from .server import send_commerceml_data_to_url

    result = await send_commerceml_data_to_url(idx, type)

    if not result.get("success"):
        err = result.get("error", "Sync failed")
        logger.warning(
            "commerceml sync failed connection_id=%s type=%s error=%s", idx, type, err
        )
        if _is_settings_sync_error(err):
            raise HTTPException(status_code=400, detail=err)
        raise HTTPException(status_code=500, detail=err)

    return result
