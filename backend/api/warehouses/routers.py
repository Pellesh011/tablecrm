import hashlib
import io
from datetime import datetime
from typing import Optional

import api.warehouses.schemas as schemas
import segno
from common.geocoders.instance import geocoder
from database.db import database, warehouse_hash, warehouses
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from functions.helpers import (
    check_entity_exists,
    create_entity_hash,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    update_entity_hash,
)
from sqlalchemy import func, insert, select
from ws_manager import manager

router = APIRouter(tags=["warehouses"])


@router.get("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def get_warehouse_by_id(token: str, idx: int):
    """Получение склада по ID"""
    user = await get_user_by_token(token)
    warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
    warehouse_db = datetime_to_timestamp(warehouse_db)
    return warehouse_db


@router.get("/warehouses/", response_model=schemas.WarehouseListGet)
async def get_warehouses(
    request: Request,
    token: str,
    name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    with_hash: bool = Query(False, description="Включить QR хеш в ответ"),
):
    """Получение списка складов"""
    base_url = str(request.base_url).rstrip("/")
    user = await get_user_by_token(token)
    filters = [
        warehouses.c.cashbox == user.cashbox_id,
        warehouses.c.is_deleted.is_not(True),
    ]

    if name:
        filters.append(
            warehouses.c.name.ilike(f"%{name}%"),
        )

    query = warehouses.select().where(*filters).limit(limit).offset(offset)

    warehouses_db = await database.fetch_all(query)
    warehouses_db = [*map(datetime_to_timestamp, warehouses_db)]

    if with_hash and warehouses_db:
        warehouse_ids = [wh["id"] for wh in warehouses_db]
        hash_query = select(
            warehouse_hash.c.warehouses_id, warehouse_hash.c.hash
        ).where(warehouse_hash.c.warehouses_id.in_(warehouse_ids))
        existing_hashes = await database.fetch_all(hash_query)
        hash_dict = {h["warehouses_id"]: h["hash"] for h in existing_hashes}
        insert_values = []
        for warehouse_info in warehouses_db:
            wh_id = warehouse_info["id"]
            if wh_id not in hash_dict:
                hash_base = f"WH:{wh_id}:{warehouse_info.get('name', '')}"
                hash_string = (
                    "wh_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
                )
                hash_dict[wh_id] = hash_string
                insert_values.append(
                    {
                        "warehouses_id": wh_id,
                        "hash": hash_string,
                        "created_at": datetime.now(),
                    }
                )
            warehouse_info["qr_hash"] = f"WH:{wh_id}:{hash_dict[wh_id]}"
            warehouse_info["qr_url"] = f"{base_url}/warehouses/{wh_id}/qr?token={token}"
        if insert_values:
            await database.execute_many(insert(warehouse_hash), insert_values)

    query = select(func.count(warehouses.c.id)).where(*filters)

    warehouses_db_count = await database.fetch_one(query)

    return {"result": warehouses_db, "count": warehouses_db_count.count_1}


@router.post("/warehouses/", response_model=schemas.WarehouseList)
async def new_warehouse(token: str, warehouses_data: schemas.WarehouseCreateMass):
    """Создание склада"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    warehouses_cache = set()
    exceptions = []
    for warehouse_values in warehouses_data.dict()["__root__"]:
        warehouse_values["owner"] = user.id
        warehouse_values["cashbox"] = user.cashbox_id

        if warehouse_values.get("parent") is not None:
            if warehouse_values["parent"] not in warehouses_cache:
                try:
                    await check_entity_exists(
                        warehouses, warehouse_values["parent"], user.id
                    )
                    warehouses_cache.add(warehouse_values["parent"])
                except HTTPException as e:
                    exceptions.append(str(warehouse_values) + " " + e.detail)
                    continue

        if warehouse_values.get("address") is not None:

            structured_geo = await geocoder.validate_address(
                warehouse_values.get("address")
            )

            if structured_geo is None:
                exceptions.append(str(warehouse_values) + " incorrect address field")
                continue

            warehouse_values.update(
                {
                    "address": ", ".join(
                        filter(
                            None,
                            [
                                structured_geo.country,
                                structured_geo.state,
                                structured_geo.city,
                                structured_geo.street,
                                structured_geo.housenumber,
                                structured_geo.postcode,
                            ],
                        )
                    ),
                    "latitude": structured_geo.latitude,
                    "longitude": structured_geo.longitude,
                    "is_deleted": False,
                }
            )

        query = warehouses.insert().values(warehouse_values)
        warehouse_id = await database.execute(query)
        await create_entity_hash(
            table=warehouses, table_hash=warehouse_hash, idx=warehouse_id
        )
        inserted_ids.add(warehouse_id)

    query = warehouses.select().where(
        warehouses.c.cashbox == user.cashbox_id, warehouses.c.id.in_(inserted_ids)
    )
    warehouses_db = await database.fetch_all(query)
    warehouses_db = [*map(datetime_to_timestamp, warehouses_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "warehouses",
            "result": warehouses_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return warehouses_db


@router.patch("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def edit_warehouse(
    token: str,
    idx: int,
    warehouse: schemas.WarehouseEdit,
):
    """Редактирование склада"""
    user = await get_user_by_token(token)
    warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
    warehouse_values = warehouse.dict(exclude_unset=True)

    if warehouse_values:
        if warehouse_values.get("parent") is not None:
            await check_entity_exists(warehouses, warehouse_values["parent"], user.id)

        query = (
            warehouses.update()
            .where(warehouses.c.id == idx, warehouses.c.cashbox == user.cashbox_id)
            .values(warehouse_values)
        )
        await database.execute(query)
        warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
        await update_entity_hash(
            table=warehouses, table_hash=warehouse_hash, entity=warehouse_db
        )

    warehouse_db = datetime_to_timestamp(warehouse_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "warehouses", "result": warehouse_db},
    )

    return warehouse_db


@router.delete("/warehouses/{idx}/", response_model=schemas.Warehouse)
async def delete_warehouse(token: str, idx: int):
    """Удаление склада"""
    user = await get_user_by_token(token)

    await get_entity_by_id(warehouses, idx, user.id)

    query = (
        warehouses.update()
        .where(warehouses.c.id == idx, warehouses.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = warehouses.select().where(
        warehouses.c.id == idx, warehouses.c.cashbox == user.cashbox_id
    )
    warehouse_db = await database.fetch_one(query)
    warehouse_db = datetime_to_timestamp(warehouse_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "warehouses",
            "result": warehouse_db,
        },
    )

    return warehouse_db


@router.get("/warehouses/{idx}/qr", response_class=StreamingResponse)
async def get_warehouse_qr(
    token: str,
    idx: int,
    size: int = Query(300, description="Размер QR-кода в пикселях", ge=100, le=1000),
):
    """Генерация QR-кода для склада"""
    user = await get_user_by_token(token)
    warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
    warehouse_db_dict = dict(warehouse_db)
    hash_query = select(warehouse_hash.c.hash).where(
        warehouse_hash.c.warehouses_id == idx
    )
    hash_record = await database.fetch_one(hash_query)
    if not hash_record:
        hash_base = f"WH:{warehouse_db_dict['id']}:{warehouse_db_dict.get('name', '')}"
        hash_string = "wh_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
        await database.execute(
            insert(warehouse_hash).values(
                warehouses_id=idx, hash=hash_string, created_at=datetime.now()
            )
        )
    else:
        hash_string = hash_record["hash"]
    qr_content = f"WH:{warehouse_db_dict['id']}:{hash_string}"
    qr = segno.make_qr(qr_content, error="H")
    svg_buffer = io.BytesIO()
    scale = max(3, size // 50)
    qr.save(svg_buffer, kind="svg", scale=scale, border=2)
    svg_buffer.seek(0)
    filename = f"warehouse_{idx}_qr.svg"
    return StreamingResponse(
        svg_buffer,
        media_type="image/svg+xml",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/warehouses/{idx}/hash")
async def get_warehouse_hash(token: str, idx: int):
    """Получить хеш для склада"""
    user = await get_user_by_token(token)
    warehouse_db = await get_entity_by_id(warehouses, idx, user.cashbox_id)
    warehouse_db_dict = dict(warehouse_db)
    hash_query = select(warehouse_hash.c.hash).where(
        warehouse_hash.c.warehouses_id == idx
    )
    hash_record = await database.fetch_one(hash_query)
    if not hash_record:
        hash_base = f"WH:{warehouse_db_dict['id']}:{warehouse_db_dict.get('name', '')}"
        hash_string = "wh_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
        await database.execute(
            insert(warehouse_hash).values(
                warehouses_id=idx, hash=hash_string, created_at=datetime.now()
            )
        )
    else:
        hash_string = hash_record["hash"]
    return {
        "warehouse_id": warehouse_db_dict["id"],
        "name": warehouse_db_dict.get("name"),
        "hash": hash_string,
        "qr_content": f"WH:{warehouse_db_dict['id']}:{hash_string}",
        "timestamp": datetime.now().isoformat(),
    }
