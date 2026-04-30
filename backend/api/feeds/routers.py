import hashlib
import json
import logging
import os
import uuid
from typing import Optional

from database.db import database, feeds
from fastapi import APIRouter, Body, HTTPException, Query
from functions.helpers import get_user_by_token
from starlette.responses import Response

from . import schemas
from .feed_generator.generator import FeedGenerator
from .tilda_sync import send_both_feeds_to_tilda

logger = logging.getLogger(__name__)

router = APIRouter(tags=["feeds"])


def generate_feed_token() -> str:
    # md5 от uuid4 + random salt
    raw = (str(uuid.uuid4()) + os.urandom(8).hex()).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


@router.post("/feeds")
async def create_feed(token: str, data: schemas.FeedCreate):
    user = await get_user_by_token(token)

    url_token = generate_feed_token()

    data_dict = data.dict()
    # Сериализуем tilda_warehouse_id если это список (для хранения в String поле БД)
    if isinstance(data_dict.get("tilda_warehouse_id"), list):
        data_dict["tilda_warehouse_id"] = json.dumps(data_dict["tilda_warehouse_id"])

    data_dict["cashbox_id"] = user.cashbox_id
    data_dict["url_token"] = url_token

    query = feeds.insert().values(data_dict).returning(feeds.c.id)
    feed_id = await database.execute(query)

    # Десериализуем обратно для ответа.
    result_data = dict(data_dict)
    if isinstance(result_data.get("tilda_warehouse_id"), str):
        try:
            result_data["tilda_warehouse_id"] = json.loads(
                result_data["tilda_warehouse_id"]
            )
        except (json.JSONDecodeError, TypeError):
            pass  # Оставляем как строку если не JSON

    return schemas.Feed(
        id=feed_id,
        **result_data,
    )


@router.get("/feeds/{url_token}")
async def get_feed(
    url_token: str,
):
    query = feeds.select().where(feeds.c.url_token == url_token)
    feed = await database.fetch_one(query)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")

    generator = FeedGenerator(url_token)
    result = await generator.generate()
    return result if result is not None else None


@router.get("/feeds")
async def get_feeds(token: str):
    user = await get_user_by_token(token)

    query = feeds.select().where(feeds.c.cashbox_id == user.cashbox_id)

    db_feeds = await database.fetch_all(query)

    # Десериализуем tilda_warehouse_id из JSON строки обратно в список/строку
    processed_feeds = []
    for feed in db_feeds:
        feed_dict = dict(feed)
        if isinstance(feed_dict.get("tilda_warehouse_id"), str):
            try:
                feed_dict["tilda_warehouse_id"] = json.loads(
                    feed_dict["tilda_warehouse_id"]
                )
            except (json.JSONDecodeError, TypeError):
                pass  # Оставляем как строку если не JSON
        processed_feeds.append(feed_dict)

    return schemas.GetFeeds(count=len(processed_feeds), feeds=processed_feeds)


@router.patch("/feeds/{idx}")
async def update_feed(token: str, idx: int, data: schemas.FeedUpdate):
    user = await get_user_by_token(token)
    query = feeds.select().where(
        feeds.c.id == idx, feeds.c.cashbox_id == user.cashbox_id
    )
    feed = await database.fetch_one(query)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    resp = dict(feed)

    upd_data = data.dict(exclude_none=True)
    # Сериализуем tilda_warehouse_id если это список (для хранения в String поле БД)
    if isinstance(upd_data.get("tilda_warehouse_id"), list):
        upd_data["tilda_warehouse_id"] = json.dumps(upd_data["tilda_warehouse_id"])

    update_query = feeds.update().where(feeds.c.id == idx).values(**upd_data)
    await database.execute(update_query)

    for k, v in upd_data.items():
        # Десериализуем обратно для ответа
        if k == "tilda_warehouse_id" and isinstance(v, str):
            try:
                resp[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                resp[k] = v  # Оставляем как строку если не JSON
        else:
            resp[k] = v

    return schemas.Feed(**resp)


@router.delete("/feeds/{idx}")
async def delete_feed(token: str, idx: int):
    user = await get_user_by_token(token)
    query = feeds.delete().where(
        feeds.c.id == idx, feeds.c.cashbox_id == user.cashbox_id
    )
    await database.execute(query)
    return Response(status_code=204)


@router.get("/feeds/{idx}/preview-xml")
async def preview_feed_xml(
    token: str,
    idx: int,
    type: str = Query("catalog", description="Тип XML: catalog или offers"),
):
    """
    Просмотр сгенерированных XML файлов для фида.
    Позволяет увидеть, что именно будет отправлено в Tilda.

    Args:
        token: Токен пользователя
        idx: ID фида
        type: Тип XML (catalog или offers)

    Returns:
        XML содержимое в виде текста
    """
    user = await get_user_by_token(token)

    # Проверяем, что фид принадлежит пользователю
    query = feeds.select().where(
        feeds.c.id == idx, feeds.c.cashbox_id == user.cashbox_id
    )
    feed = await database.fetch_one(query)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")

    feed_cashbox_id = feed.get("cashbox_id") or user.cashbox_id
    generator = FeedGenerator(feed["url_token"])

    if type == "catalog":
        xml_content = await generator.generate_catalog(cashbox_id=feed_cashbox_id)
        if xml_content is None:
            raise HTTPException(
                status_code=500, detail="Failed to generate catalog XML"
            )
    elif type == "offers":
        xml_content = await generator.generate_offers(cashbox_id=feed_cashbox_id)
        if xml_content is None:
            raise HTTPException(status_code=500, detail="Failed to generate offers XML")
    else:
        raise HTTPException(
            status_code=400, detail="Type must be 'catalog' or 'offers'"
        )

    from starlette.responses import Response

    return Response(
        content=xml_content,
        media_type="application/xml; charset=utf-8",
        headers={"Content-Disposition": f'inline; filename="{type}.xml"'},
    )


@router.post("/feeds/{idx}/sync")
async def sync_feed_to_tilda(
    token: str,
    idx: int,
    data: Optional[schemas.TildaSync] = Body(default=None),
):
    """
    Эндпоинт для синхронизации фида с Tilda.
    Может использовать данные из фида (если они сохранены) или из body запроса.

    Args:
        token: Токен пользователя (query параметр)
        idx: ID фида (path параметр)
        data: Данные для синхронизации с Tilda (body, опциональный - если не указан, используются данные из фида)

    Returns:
        Результат отправки с деталями
    """
    user = await get_user_by_token(token)

    # Проверяем, что фид принадлежит пользователю
    query = feeds.select().where(
        feeds.c.id == idx, feeds.c.cashbox_id == user.cashbox_id
    )
    feed = await database.fetch_one(query)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")

    # Определяем параметры Tilda: из body запроса или из сохраненных данных фида
    tilda_url = data.tilda_url if data and data.tilda_url else feed.get("tilda_url")
    tilda_username = (
        data.username if data and data.username else feed.get("tilda_username")
    )
    tilda_password = (
        data.password if data and data.password else feed.get("tilda_password")
    )

    if not tilda_url or not tilda_username or not tilda_password:
        raise HTTPException(
            status_code=400,
            detail="Tilda credentials not found. Please provide tilda_url, username, and password either in feed settings or in request body.",
        )

    # Используем cashbox_id из фида или из пользователя
    feed_cashbox_id = feed.get("cashbox_id") or user.cashbox_id
    logger.info(
        f"sync_feed_to_tilda: feed_id={idx}, feed_cashbox_id={feed_cashbox_id}, user.cashbox_id={user.cashbox_id}"
    )

    # Для Tilda используем формат CommerceML
    # По документации Tilda нужны оба файла: import.xml (каталог) и offers.xml (цены и остатки)
    generator = FeedGenerator(feed["url_token"])

    # Генерируем каталог товаров в формате CommerceML
    try:
        catalog_xml = await generator.generate_catalog(cashbox_id=feed_cashbox_id)
        if not catalog_xml or len(catalog_xml.strip()) == 0:
            raise HTTPException(
                status_code=500, detail="Generated catalog XML is empty"
            )
    except Exception as e:
        logger.error(f"Failed to generate catalog XML: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate catalog XML: {str(e)}"
        )

    # Генерируем предложения (цены и остатки) в формате CommerceML
    try:
        offers_xml = await generator.generate_offers(cashbox_id=feed_cashbox_id)
        if not offers_xml or len(offers_xml.strip()) == 0:
            raise HTTPException(status_code=500, detail="Generated offers XML is empty")
    except Exception as e:
        logger.error(f"Failed to generate offers XML: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate offers XML: {str(e)}"
        )

    logger.info(
        f"sync_feed_to_tilda: catalog_xml length={len(catalog_xml)}, offers_xml length={len(offers_xml)}"
    )

    # ВАЖНО: Tilda требует, чтобы все запросы шли в одной сессии!
    # Используем специальную функцию, которая отправляет оба файла в одной сессии
    sync_result = await send_both_feeds_to_tilda(
        catalog_xml=catalog_xml,
        offers_xml=offers_xml,
        tilda_url=tilda_url,
        username=tilda_username,
        password=tilda_password,
    )

    # Возвращаем результаты обоих запросов
    return {
        "feed_id": idx,
        "feed_name": feed["name"],
        "tilda_url": tilda_url,
        "success": sync_result.get("success", False),
        "catalog_sync_result": sync_result.get("catalog_result", {}),
        "offers_sync_result": sync_result.get("offers_result", {}),
    }


@router.get("/feeds/{idx}/sync")
async def sync_feed_to_tilda_get(
    token: str,
    idx: int,
):
    """
    GET версия эндпоинта для синхронизации фида с Tilda.
    Использует данные из сохраненных настроек фида.
    Удобно для вызова из браузера.
    """
    # Вызываем POST версию с пустым body
    return await sync_feed_to_tilda(token=token, idx=idx, data=None)
