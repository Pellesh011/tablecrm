import hashlib
import io
import time
from datetime import datetime
from typing import List, Optional

import api.nomenclature.schemas as schemas
import segno
from api.marketplace.service.public_categories.public_categories_service import (
    MarketplacePublicCategoriesService,
)
from api.nomenclature.utils import (
    auto_link_global_category,
    sync_global_category_for_nomenclature,
    update_category_has_products,
    validate_video_link,
)
from api.nomenclature.video.models import nomenclature_videos
from api.pictures.routers import build_public_url
from database.db import (
    cashbox_settings,
    categories,
    database,
    global_categories,
    manufacturers,
    nomenclature,
    nomenclature_attributes,
    nomenclature_attributes_value,
    nomenclature_barcodes,
    nomenclature_groups_value,
    nomenclature_hash,
    pictures,
    price_types,
    prices,
    units,
    warehouse_register_movement,
    warehouses,
)
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.params import Body, Query
from fastapi.responses import StreamingResponse
from functions.filter_schemas import CUIntegerFilters
from functions.helpers import (
    build_filters,
    check_entity_exists,
    check_unit_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
    update_entity_hash,
)
from sqlalchemy import and_, case, exists, func, insert, or_, select
from starlette import status
from ws_manager import manager

router = APIRouter(tags=["nomenclature"])

public_categories_service = MarketplacePublicCategoriesService()


async def _normalize_duplicate_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip()
    return normalized if normalized else None


async def _get_cashbox_duplicate_settings(cashbox_id: int) -> dict:
    query = select(cashbox_settings).where(
        cashbox_settings.c.cashbox_id == cashbox_id,
        cashbox_settings.c.is_deleted.is_not(True),
    )
    settings = await database.fetch_one(query)
    return dict(settings) if settings else {}


async def _get_nomenclature_duplicate_errors(
    *,
    cashbox_id: int,
    name: Optional[str],
    code: Optional[str],
    exclude_id: Optional[int],
    check_name: bool,
    check_code: bool,
) -> list[str]:
    errors: list[str] = []

    if check_name:
        normalized_name = await _normalize_duplicate_value(name)
        if normalized_name:
            query = select(nomenclature.c.id).where(
                nomenclature.c.cashbox == cashbox_id,
                nomenclature.c.is_deleted.is_not(True),
                func.lower(nomenclature.c.name) == normalized_name.lower(),
            )
            if exclude_id is not None:
                query = query.where(nomenclature.c.id != exclude_id)
            existing = await database.fetch_one(query)
            if existing:
                errors.append("Товар с таким названием уже существует")

    if check_code:
        normalized_code = await _normalize_duplicate_value(code)
        if normalized_code:
            query = select(nomenclature.c.id).where(
                nomenclature.c.cashbox == cashbox_id,
                nomenclature.c.is_deleted.is_not(True),
                func.lower(nomenclature.c.code) == normalized_code.lower(),
            )
            if exclude_id is not None:
                query = query.where(nomenclature.c.id != exclude_id)
            existing = await database.fetch_one(query)
            if existing:
                errors.append("Товар с таким артикулом уже существует")

    return errors


@router.patch("/nomenclature/barcode")
async def patch_nomenclature_barcodes(
    token: str, barcodes: List[schemas.NomenclaturesListPatch]
):
    """Изменение штрихкодов категории по ID"""
    user = await get_user_by_token(token)

    errors = []

    for barcode in barcodes:
        query = nomenclature.select().where(
            nomenclature.c.id == barcode.idx,
            nomenclature.c.cashbox == user.cashbox_id,
            nomenclature.c.is_deleted.is_not(True),
        )
        nomenclature_db = await database.fetch_one(query)
        if not nomenclature_db:
            errors.append(
                {
                    "idx": barcode.idx,
                    "error_code": 404,
                    "type_error": "Nomenclature not found",
                }
            )
            continue

        query = nomenclature_barcodes.select().where(
            nomenclature_barcodes.c.nomenclature_id == barcode.idx
        )
        barcode_ex_list = await database.fetch_all(query)
        barcodes = [barcode_info.code for barcode_info in barcode_ex_list]

        async with database.transaction():
            if barcode.new_barcode in barcodes:
                query = nomenclature_barcodes.delete().where(
                    and_(
                        nomenclature_barcodes.c.nomenclature_id == barcode.idx,
                        nomenclature_barcodes.c.code == barcode.new_barcode,
                    )
                )
                await database.execute(query)

                query = (
                    nomenclature_barcodes.update()
                    .where(
                        and_(
                            nomenclature_barcodes.c.nomenclature_id == barcode.idx,
                            nomenclature_barcodes.c.code == barcode.old_barcode,
                        )
                    )
                    .values({"code": barcode.new_barcode})
                )
                await database.execute(query)
            elif not barcodes:
                query = nomenclature_barcodes.insert().values(
                    {"nomenclature_id": barcode.idx, "code": barcode.new_barcode}
                )
                await database.execute(query)
            else:
                query = (
                    nomenclature_barcodes.update()
                    .where(
                        and_(
                            nomenclature_barcodes.c.nomenclature_id == barcode.idx,
                            nomenclature_barcodes.c.code == barcode.old_barcode,
                        )
                    )
                    .values({"code": barcode.new_barcode})
                )
                await database.execute(query)
    return {"errors": errors}


@router.get("/nomenclature/{idx}/barcode")
async def get_nomenclature_barcodes(token: str, idx: int):
    """Получение штрихкодов категории по ID"""
    user = await get_user_by_token(token)

    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)

    query = nomenclature_barcodes.select().where(
        nomenclature_barcodes.c.nomenclature_id == idx
    )
    barcodes_list = await database.fetch_all(query)

    return [barcode_info.code for barcode_info in barcodes_list]


@router.post("/nomenclature/{idx}/barcode")
async def add_barcode_to_nomenclature(
    token: str, idx: int, barcode: schemas.NomenclatureBarcodeCreate
):
    """Добавление штрихкода к категории по ID"""
    user = await get_user_by_token(token)

    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    query = nomenclature_barcodes.select().where(
        and_(
            nomenclature_barcodes.c.nomenclature_id == idx,
            nomenclature_barcodes.c.code == barcode.barcode,
        )
    )
    barcode_ex = await database.fetch_one(query)

    if barcode_ex:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Данный штрихкод уже привязан к этому товару",
        )

    query = nomenclature_barcodes.insert().values(
        {"nomenclature_id": idx, "code": barcode.barcode}
    )
    await database.execute(query)


@router.delete("/nomenclature/{idx}/barcode")
async def delete_barcode_to_nomenclature(
    token: str, idx: int, barcode: schemas.NomenclatureBarcodeCreate
):
    """Добавление штрихкода к категории по ID"""
    user = await get_user_by_token(token)

    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    query = nomenclature_barcodes.delete().where(
        and_(
            nomenclature_barcodes.c.nomenclature_id == idx,
            nomenclature_barcodes.c.code == barcode.barcode,
        )
    )
    await database.execute(query)


@router.get("/nomenclature/{idx}/qr", response_class=StreamingResponse)
async def get_nomenclature_qr(
    token: str,
    idx: int,
    size: int = Query(300, description="Размер QR-кода в пикселях", ge=100, le=1000),
):
    """Генерация QR-кода для товара"""
    user = await get_user_by_token(token)
    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    nomenclature_db_dict = dict(nomenclature_db)
    hash_query = select(nomenclature_hash.c.hash).where(
        nomenclature_hash.c.nomenclature_id == idx
    )
    hash_record = await database.fetch_one(hash_query)
    if not hash_record:
        hash_base = f"{nomenclature_db_dict['id']}:{nomenclature_db_dict.get('name', '')}:{nomenclature_db_dict.get('article', '')}"
        hash_string = "nm_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
        await database.execute(
            insert(nomenclature_hash).values(
                nomenclature_id=idx, hash=hash_string, created_at=datetime.now()
            )
        )
    else:
        hash_string = hash_record["hash"]
    qr_content = f"NOM:{nomenclature_db_dict['id']}:{hash_string}"
    qr = segno.make_qr(qr_content, error="H")
    svg_buffer = io.BytesIO()
    scale = max(3, size // 50)
    qr.save(svg_buffer, kind="svg", scale=scale, border=2)
    svg_buffer.seek(0)
    filename = f"nomenclature_{idx}_qr.svg"
    return StreamingResponse(
        svg_buffer,
        media_type="image/svg+xml",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/nomenclature/{idx}/qr/data")
async def get_nomenclature_qr_data(token: str, idx: int):
    """Получить хеш для товара"""
    user = await get_user_by_token(token)
    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    nomenclature_db_dict = dict(nomenclature_db)
    hash_query = select(nomenclature_hash.c.hash).where(
        nomenclature_hash.c.nomenclature_id == idx
    )
    hash_record = await database.fetch_one(hash_query)
    if not hash_record:
        hash_base = f"{nomenclature_db_dict['id']}:{nomenclature_db_dict.get('name', '')}:{nomenclature_db_dict.get('article', '')}"
        hash_string = "nm_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
        await database.execute(
            insert(nomenclature_hash).values(
                nomenclature_id=idx, hash=hash_string, created_at=datetime.now()
            )
        )
    else:
        hash_string = hash_record["hash"]
    return {
        "nomenclature_id": nomenclature_db_dict["id"],
        "name": nomenclature_db_dict.get("name"),
        "article": nomenclature_db_dict.get("article"),
        "hash": hash_string,
        "qr_content": f"NOM:{nomenclature_db_dict['id']}:{hash_string}",
        "timestamp": datetime.now().isoformat(),
    }


@router.post("/nomenclatures/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature_by_ids(
    token: str,
    ids: List[int] = Body(..., example=[1, 2, 3]),
    with_prices: bool = False,
    with_balance: bool = False,
):
    """Получение списка номенклатур по списку ID категорий"""
    user = await get_user_by_token(token)

    if not ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Список ID не должен быть пустым",
        )

    query = (
        select(
            nomenclature,
            units.c.convent_national_view.label("unit_name"),
            func.array_remove(
                func.array_agg(func.distinct(nomenclature_barcodes.c.code)), None
            ).label("barcodes"),
        )
        .select_from(nomenclature)
        .join(units, units.c.id == nomenclature.c.unit, full=True)
        .join(
            nomenclature_barcodes,
            nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id,
            full=True,
        )
        .where(
            nomenclature.c.cashbox == user.cashbox_id,
            nomenclature.c.is_deleted.is_not(True),
            nomenclature.c.category.in_(ids),
        )
        .group_by(nomenclature.c.id, units.c.convent_national_view)
    )

    nomenclature_db = await database.fetch_all(query)
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]

    for nomenclature_info in nomenclature_db:
        if with_prices:
            price = await database.fetch_all(
                select(prices.c.price, price_types.c.name.label("price_type"))
                .where(prices.c.nomenclature == nomenclature_info["id"])
                .select_from(prices)
                .join(price_types, price_types.c.id == prices.c.price_type)
            )
            nomenclature_info["prices"] = price

        if with_balance:
            q = case(
                [
                    (
                        warehouse_register_movement.c.type_amount == "minus",
                        warehouse_register_movement.c.amount * (-1),
                    )
                ],
                else_=warehouse_register_movement.c.amount,
            )
            query = (
                (
                    select(
                        warehouses.c.name.label("warehouse_name"),
                        nomenclature.c.id,
                        warehouse_register_movement.c.nomenclature_id,
                        func.sum(q).label("current_amount"),
                    ).where(
                        nomenclature.c.id == nomenclature_info["id"],
                        warehouse_register_movement.c.cashbox_id == user.cashbox_id,
                    )
                )
                .group_by(
                    warehouses.c.name,
                    nomenclature.c.id,
                    warehouse_register_movement.c.nomenclature_id,
                )
                .select_from(
                    warehouse_register_movement.join(
                        warehouses,
                        warehouse_register_movement.c.warehouse_id == warehouses.c.id,
                    )
                )
            )

            balances_list = await database.fetch_all(query)
            nomenclature_info["balances"] = balances_list

    query = select(func.count(nomenclature.c.id)).where(
        nomenclature.c.cashbox == user.cashbox_id,
        nomenclature.c.is_deleted.is_not(True),
        nomenclature.c.id.in_(ids),
    )
    nomenclature_db_count = await database.fetch_val(query)

    return {"result": nomenclature_db, "count": nomenclature_db_count}


@router.get("/nomenclature/", response_model=schemas.NomenclatureListGetRes)
async def get_nomenclature(
    request: Request,
    token: str,
    name: Optional[str] = None,
    barcode: Optional[str] = None,
    category: Optional[int] = None,
    global_category_id: Optional[int] = None,
    global_category_name: Optional[str] = None,
    description_long: Optional[str] = None,
    description_short: Optional[str] = None,
    has_photos: Optional[bool] = Query(
        None,
        description="Фильтр по наличию фото: true - только с фото, false - только без фото",
    ),
    tags: Optional[List[str]] = Query(
        None, description="Фильтр по тегам (хотя бы один из переданных)"
    ),
    chatting_percent: Optional[float] = Query(
        None, description="Фильтр по комиссии платформы"
    ),
    has_video: Optional[bool] = Query(None, description="Фильтр по наличию видео"),
    limit: int = 100,
    offset: int = 0,
    with_prices: bool = False,
    with_balance: bool = False,
    with_attributes: bool = Query(
        False, description="Включить атрибуты номенклатуры в ответ"
    ),
    with_photos: bool = Query(False, description="Включить фото номенклатуры в ответ"),
    with_hash: bool = Query(False, description="Включить QR хеш в ответ"),
    only_main_from_group: bool = False,
    min_price: Optional[float] = Query(
        None, description="Минимальная цена для фильтрации"
    ),
    max_price: Optional[float] = Query(
        None, description="Максимальная цена для фильтрации"
    ),
    cu_filters: CUIntegerFilters = Depends(),
    sort: Optional[str] = "created_at:desc",
):
    start_time = time.time()
    base_url = str(request.base_url).rstrip("/")
    user = await get_user_by_token(token)

    if name and barcode:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Фильтр может быть задан или по имени или по штрихкоду",
        )

    filters_data = cu_filters.dict(exclude_none=True)
    cu_filter_data = {}
    for f in filters_data.keys():
        if f in [
            "created_at__gte",
            "created_at__lte",
            "updated_at__gte",
            "updated_at__lte",
        ]:
            cu_filter_data[f] = datetime.fromtimestamp(filters_data[f])

    filters = build_filters(nomenclature, cu_filter_data)

    hash_subquery = None
    if with_hash:
        hash_subquery = select(
            nomenclature_hash.c.nomenclature_id,
            nomenclature_hash.c.hash.label("qr_hash"),
        ).alias("hash_subquery")

    price_subquery = None
    if min_price is not None or max_price is not None:
        price_subquery = (
            select(prices.c.nomenclature, func.min(prices.c.price).label("min_price"))
            .where(prices.c.is_deleted.is_not(True))
            .group_by(prices.c.nomenclature)
            .alias("price_subquery")
        )

    # Базовый запрос
    query = (
        select(
            nomenclature,
            units.c.convent_national_view.label("unit_name"),
            func.array_remove(
                func.array_agg(func.distinct(nomenclature_barcodes.c.code)), None
            ).label("barcodes"),
        )
        .select_from(nomenclature)
        .join(units, units.c.id == nomenclature.c.unit, full=True)
        .join(
            nomenclature_barcodes,
            nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id,
            full=True,
        )
    )
    query = query.group_by(nomenclature.c.id, units.c.convent_national_view)

    if with_hash and hash_subquery is not None:
        query = query.join(
            hash_subquery,
            hash_subquery.c.nomenclature_id == nomenclature.c.id,
            isouter=True,
        )
        query = query.add_columns(hash_subquery.c.qr_hash)
        query = query.group_by(hash_subquery.c.qr_hash)

    if price_subquery is not None:
        query = query.join(
            price_subquery,
            price_subquery.c.nomenclature == nomenclature.c.id,
            isouter=True,
        )

    if only_main_from_group:
        subquery = (
            select(nomenclature_groups_value.c.nomenclature_id)
            .distinct()
            .where(nomenclature_groups_value.c.is_main.is_(True))
            .alias("active")
        )
        query = query.join(
            subquery, subquery.c.nomenclature_id == nomenclature.c.id, isouter=True
        )
        query = query.where(
            or_(
                subquery.c.nomenclature_id == nomenclature.c.id,
                ~nomenclature.c.id.in_(
                    select(nomenclature_groups_value.c.nomenclature_id)
                ),
            )
        )

    # Условия фильтрации
    conditions = [
        nomenclature.c.cashbox == user.cashbox_id,
        nomenclature.c.is_deleted.is_not(True),
    ]

    if category:
        conditions.append(nomenclature.c.category == category)
    if tags:
        flattened = []
        for t in (tags if isinstance(tags, list) else [tags]):
            flattened.extend([x.strip() for x in t.split(",") if x.strip()])
        tags = list(set(flattened))
        conditions.append(nomenclature.c.tags.op("&&")(tags))
    if has_photos is not None:
        photos_exists = exists(
            select(1).where(
                and_(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id == nomenclature.c.id,
                    pictures.c.is_deleted.is_not(True),
                )
            )
        )
        if has_photos:
            conditions.append(photos_exists)
        else:
            conditions.append(~photos_exists)
    if name:
        conditions.append(nomenclature.c.name.ilike(f"%{name}%"))
    if barcode:
        join_barcode = select(nomenclature_barcodes.c.nomenclature_id).where(
            nomenclature_barcodes.c.code.ilike(f"%{barcode}%")
        )
        conditions.append(nomenclature.c.id.in_(join_barcode))
    if global_category_id is not None:
        conditions.append(nomenclature.c.global_category_id == global_category_id)
    if global_category_name:
        query = query.join(
            global_categories,
            global_categories.c.id == nomenclature.c.global_category_id,
            isouter=False,
        )
        conditions.append(global_categories.c.name.ilike(f"%{global_category_name}%"))
    if description_long:
        conditions.append(
            nomenclature.c.description_long.ilike(f"%{description_long}%")
        )
    if description_short:
        conditions.append(
            nomenclature.c.description_short.ilike(f"%{description_short}%")
        )
    if chatting_percent is not None:
        conditions.append(nomenclature.c.chatting_percent == chatting_percent)
    if min_price is not None and price_subquery is not None:
        conditions.append(price_subquery.c.min_price >= min_price)
    if max_price is not None and price_subquery is not None:
        conditions.append(price_subquery.c.min_price <= max_price)
    if has_video is not None:
        video_exists = exists(
            select(1).where(nomenclature_videos.c.nomenclature_id == nomenclature.c.id)
        )
        if has_video:
            conditions.append(video_exists)
        else:
            conditions.append(~video_exists)

    query = query.where(and_(*conditions)).filter(*filters)

    # Сортировка
    if sort:
        order_fields = {"created_at", "updated_at", "name"}
        directions = {"asc", "desc"}
        if (
            len(sort.split(":")) != 2
            or sort.split(":")[1].lower() not in directions
            or sort.split(":")[0].lower() not in order_fields
        ):
            raise HTTPException(
                status_code=400, detail="Вы ввели некорректный параметр сортировки!"
            )
        order_by, direction = sort.split(":")
        column = nomenclature.c[order_by]
        if direction.lower() == "desc":
            column = column.desc()
        query = query.order_by(column)

    query = query.group_by(nomenclature.c.id, units.c.convent_national_view)

    # Подсчёт количества (отдельный запрос)
    count_query = (
        select(func.count(func.distinct(nomenclature.c.id)))
        .select_from(nomenclature)
        .filter(*filters)
    )
    if barcode:
        count_query = count_query.join(
            nomenclature_barcodes,
            nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id,
        )
    if price_subquery is not None:
        count_query = count_query.join(
            price_subquery,
            price_subquery.c.nomenclature == nomenclature.c.id,
            isouter=True,
        )
    if only_main_from_group:
        subquery = (
            select(nomenclature_groups_value.c.nomenclature_id)
            .distinct()
            .where(nomenclature_groups_value.c.is_main.is_(True))
            .alias("active_count")
        )
        count_query = count_query.join(
            subquery, subquery.c.nomenclature_id == nomenclature.c.id, isouter=True
        )
        count_query = count_query.where(
            or_(
                subquery.c.nomenclature_id == nomenclature.c.id,
                ~nomenclature.c.id.in_(
                    select(nomenclature_groups_value.c.nomenclature_id)
                ),
            )
        )
    if global_category_name:
        count_query = count_query.join(
            global_categories,
            global_categories.c.id == nomenclature.c.global_category_id,
            isouter=False,
        )
    count_query = count_query.where(and_(*conditions))

    # Выполнение основного запроса и подсчёта
    nomenclature_db = await database.fetch_all(query.limit(limit).offset(offset))
    nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]
    nomenclature_db_count = await database.fetch_val(count_query)

    # ========== БАТЧ-ЗАГРУЗКА СВЯЗАННЫХ ДАННЫХ ==========
    if nomenclature_db:
        ids = [item["id"] for item in nomenclature_db]

        # 1. Цены
        prices_map = {}
        if with_prices:
            price_rows = await database.fetch_all(
                select(
                    prices.c.nomenclature,
                    prices.c.price,
                    price_types.c.name.label("price_type"),
                )
                .select_from(
                    prices.join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(prices.c.nomenclature.in_(ids), prices.c.is_deleted.is_not(True))
            )
            for row in price_rows:
                prices_map.setdefault(row["nomenclature"], []).append(
                    {"price": row["price"], "price_type": row["price_type"]}
                )

        # 2. Атрибуты
        attrs_map = {}
        if with_attributes:
            attr_rows = await database.fetch_all(
                select(
                    nomenclature_attributes_value.c.nomenclature_id,
                    nomenclature_attributes_value.c.id,
                    nomenclature_attributes_value.c.attribute_id,
                    nomenclature_attributes.c.name,
                    nomenclature_attributes.c.alias,
                    nomenclature_attributes_value.c.value,
                )
                .select_from(
                    nomenclature_attributes_value.join(
                        nomenclature_attributes,
                        nomenclature_attributes_value.c.attribute_id
                        == nomenclature_attributes.c.id,
                    )
                )
                .where(nomenclature_attributes_value.c.nomenclature_id.in_(ids))
            )
            for row in attr_rows:
                attrs_map.setdefault(row["nomenclature_id"], []).append(dict(row))

        # 3. Фото
        photos_map = {}
        if with_photos:
            photo_rows = await database.fetch_all(
                select(
                    pictures.c.entity_id,
                    pictures.c.id,
                    pictures.c.url,
                    pictures.c.is_main,
                    pictures.c.created_at,
                    pictures.c.updated_at,
                )
                .where(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id.in_(ids),
                    pictures.c.is_deleted.is_not(True),
                )
                .order_by(
                    pictures.c.entity_id, pictures.c.is_main.desc(), pictures.c.id.asc()
                )
            )
            for row in photo_rows:
                photos_map.setdefault(row["entity_id"], []).append(dict(row))

        # 4. Видео
        videos_map = {}
        video_rows = await database.fetch_all(
            select(nomenclature_videos).where(
                nomenclature_videos.c.nomenclature_id.in_(ids)
            )
        )
        for row in video_rows:
            videos_map.setdefault(row["nomenclature_id"], []).append(dict(row))

        # 5. Остатки (если нужно)
        balances_map = {}
        if with_balance:
            q = case(
                [
                    (
                        warehouse_register_movement.c.type_amount == "minus",
                        warehouse_register_movement.c.amount * (-1),
                    )
                ],
                else_=warehouse_register_movement.c.amount,
            )
            balance_rows = await database.fetch_all(
                select(
                    warehouse_register_movement.c.nomenclature_id,
                    warehouses.c.name.label("warehouse_name"),
                    func.sum(q).label("current_amount"),
                )
                .select_from(
                    warehouse_register_movement.join(
                        warehouses,
                        warehouse_register_movement.c.warehouse_id == warehouses.c.id,
                    )
                )
                .where(
                    warehouse_register_movement.c.nomenclature_id.in_(ids),
                    warehouse_register_movement.c.cashbox_id == user.cashbox_id,
                )
                .group_by(
                    warehouse_register_movement.c.nomenclature_id,
                    warehouses.c.name,
                )
            )
            for row in balance_rows:
                balances_map.setdefault(row["nomenclature_id"], []).append(
                    {
                        "warehouse_name": row["warehouse_name"],
                        "current_amount": row["current_amount"],
                    }
                )

        # Заполнение каждого товара
        for nomenclature_info in nomenclature_db:
            nom_id = nomenclature_info["id"]
            if with_prices:
                nomenclature_info["prices"] = prices_map.get(nom_id, [])
            if with_attributes:
                nomenclature_info["attributes"] = attrs_map.get(nom_id, [])
            if with_photos:
                photos = photos_map.get(nom_id, [])
                for photo in photos:
                    photo["public_url"] = build_public_url(photo["id"])
                    if photo.get("url") and photo["url"].startswith("photos/"):
                        photo["url"] = photo["url"][7:]
                nomenclature_info["photos"] = photos
            if with_balance:
                nomenclature_info["balances"] = balances_map.get(nom_id, [])
            nomenclature_info["videos"] = videos_map.get(nom_id, [])

            # QR-хеш (если требуется)
            if with_hash:
                final_hash_string = None
                if nomenclature_info.get("qr_hash"):
                    final_hash_string = nomenclature_info["qr_hash"]
                else:
                    hash_query = select(nomenclature_hash.c.hash).where(
                        nomenclature_hash.c.nomenclature_id == nom_id
                    )
                    hash_record = await database.fetch_one(hash_query)
                    if hash_record:
                        final_hash_string = hash_record["hash"]
                    else:
                        hash_base = f"{nom_id}:{nomenclature_info.get('name', '')}:{nomenclature_info.get('article', '')}"
                        final_hash_string = (
                            "nm_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
                        )
                        await database.execute(
                            insert(nomenclature_hash).values(
                                nomenclature_id=nom_id,
                                hash=final_hash_string,
                                created_at=datetime.now(),
                            )
                        )
                nomenclature_info["qr_hash"] = f"NOM:{nom_id}:{final_hash_string}"
                nomenclature_info["qr_url"] = (
                    f"{base_url}/nomenclature/{nom_id}/qr?token={token}"
                )

    return {"result": nomenclature_db, "count": nomenclature_db_count}


@router.get("/nomenclature/tags/")
async def get_nomenclature_tags(
    token: str,
    search: Optional[str] = Query(
        None, description="Поиск по тегу (регистронезависимый)"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Возвращает список всех уникальных тегов номенклатуры для текущей кассы.
    """
    user = await get_user_by_token(token)

    subq = (
        select(func.unnest(nomenclature.c.tags).label("tag"))
        .select_from(nomenclature)
        .where(
            and_(
                nomenclature.c.cashbox == user.cashbox_id,
                nomenclature.c.is_deleted.is_not(True),
                nomenclature.c.tags.is_not(None),
                func.array_length(nomenclature.c.tags, 1) > 0,
            )
        )
        .subquery()
    )

    # Подсчёт уникальных тегов с учётом поиска
    count_query = select(func.count(func.distinct(subq.c.tag))).select_from(subq)
    if search:
        count_query = count_query.where(subq.c.tag.ilike(f"%{search}%"))
    total = await database.fetch_val(count_query)

    # Основной запрос
    stmt = (
        select(subq.c.tag).distinct().order_by(subq.c.tag).limit(limit).offset(offset)
    )
    if search:
        stmt = stmt.where(subq.c.tag.ilike(f"%{search}%"))

    rows = await database.fetch_all(stmt)
    tags = [row["tag"] for row in rows]

    return {"tags": tags, "count": total}


@router.post("/nomenclature/", response_model=schemas.NomenclatureList)
async def new_nomenclature(
    token: str, nomenclature_data: schemas.NomenclatureCreateMass
):
    """Создание категории"""
    user = await get_user_by_token(token)

    settings = await _get_cashbox_duplicate_settings(user.cashbox_id)
    check_name_duplicates = settings.get("check_nomenclature_duplicates_by_name", False)
    check_code_duplicates = settings.get("check_nomenclature_duplicates_by_code", False)

    inserted_ids = set()
    categories_cache = set()
    manufacturers_cache = set()
    units_cache = set()
    exceptions = []
    async with database.transaction():
        for nomenclature_values in nomenclature_data.dict()["__root__"]:
            nomenclature_values["cashbox"] = user.cashbox_id
            nomenclature_values["owner"] = user.id
            nomenclature_values["is_deleted"] = False

            if nomenclature_values.get("video_link") is not None:
                await validate_video_link(nomenclature_values["video_link"])

            if nomenclature_values.get("category") is not None:
                if nomenclature_values["category"] not in categories_cache:
                    try:
                        await check_entity_exists(
                            categories, nomenclature_values["category"], user.cashbox_id
                        )
                        categories_cache.add(nomenclature_values["category"])
                    except HTTPException as e:
                        exceptions.append(str(nomenclature_values) + " " + e.detail)
                        continue

            if nomenclature_values.get("manufacturer") is not None:
                if nomenclature_values["manufacturer"] not in manufacturers_cache:
                    try:
                        await check_entity_exists(
                            manufacturers, nomenclature_values["manufacturer"], user.id
                        )
                        manufacturers_cache.add(nomenclature_values["manufacturer"])
                    except HTTPException as e:
                        exceptions.append(str(nomenclature_values) + " " + e.detail)
                        continue

            if nomenclature_values.get("unit") is not None:
                if nomenclature_values["unit"] not in units_cache:
                    try:
                        await check_unit_exists(nomenclature_values["unit"])
                        units_cache.add(nomenclature_values["unit"])
                    except HTTPException as e:
                        exceptions.append(str(nomenclature_values) + " " + e.detail)
                        continue

            if nomenclature_values.get("global_category_id") is not None:
                await public_categories_service.ensure_global_category_exists(
                    nomenclature_values["global_category_id"]
                )
            else:
                # Автоматически связываем с глобальной категорией, если не указано
                if nomenclature_values.get("category") is not None:
                    global_cat_id = await auto_link_global_category(
                        nomenclature_values["category"]
                    )
                    if global_cat_id:
                        nomenclature_values["global_category_id"] = global_cat_id

            duplicate_errors = await _get_nomenclature_duplicate_errors(
                cashbox_id=user.cashbox_id,
                name=nomenclature_values.get("name"),
                code=nomenclature_values.get("code"),
                exclude_id=None,
                check_name=check_name_duplicates,
                check_code=check_code_duplicates,
            )
            if duplicate_errors:
                exceptions.append(
                    f"{nomenclature_values} " + ", ".join(duplicate_errors)
                )
                continue

            query = nomenclature.insert().values(nomenclature_values)
            nomenclature_id = await database.execute(query)
            inserted_ids.add(nomenclature_id)

            # Дополнительная синхронизация после создания
            global_cat_id = None
            if nomenclature_values.get("category") and not nomenclature_values.get(
                "global_category_id"
            ):
                global_cat_id = await sync_global_category_for_nomenclature(
                    nomenclature_id, nomenclature_values["category"]
                )
            elif nomenclature_values.get("global_category_id"):
                global_cat_id = nomenclature_values.get("global_category_id")

            # Обновляем has_products для категории после создания товара
            if global_cat_id:
                try:
                    await update_category_has_products(global_cat_id)
                except Exception:
                    pass  # Игнорируем ошибки обновления, чтобы не ломать создание товара

        query = nomenclature.select().where(
            nomenclature.c.cashbox == user.cashbox_id,
            nomenclature.c.id.in_(inserted_ids),
        )
        nomenclature_db = await database.fetch_all(query)
        nomenclature_db = [*map(datetime_to_timestamp, nomenclature_db)]

    if inserted_ids:
        hash_values_to_insert = []
        hash_dict = {}
        for nom in nomenclature_db:
            nom_id = nom["id"]
            hash_base = f"{nom_id}:{nom.get('name', '')}:{nom.get('article', '')}"
            hash_string = "nm_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
            hash_values_to_insert.append(
                {
                    "nomenclature_id": nom_id,
                    "hash": hash_string,
                    "created_at": datetime.now(),
                }
            )
            hash_dict[nom_id] = hash_string
        if hash_values_to_insert:
            await database.execute_many(
                insert(nomenclature_hash), hash_values_to_insert
            )

        # Добавляем qr_hash в ответ для каждого созданного товара
        for nom in nomenclature_db:
            nom_id = nom["id"]
            if nom_id in hash_dict:
                nom["qr_hash"] = f"NOM:{nom_id}:{hash_dict[nom_id]}"

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "nomenclature",
            "result": nomenclature_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return nomenclature_db


@router.patch("/nomenclature/{idx}/", response_model=schemas.Nomenclature)
async def edit_nomenclature(
    token: str,
    idx: int,
    nomenclature_data: schemas.NomenclatureEdit,
):
    """Редактирование категории"""
    user = await get_user_by_token(token)
    nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
    nomenclature_values = nomenclature_data.dict(exclude_unset=True)

    if nomenclature_values:
        settings = await _get_cashbox_duplicate_settings(user.cashbox_id)
        check_name_duplicates = settings.get(
            "check_nomenclature_duplicates_by_name", False
        )
        check_code_duplicates = settings.get(
            "check_nomenclature_duplicates_by_code", False
        )

        name_to_check = (
            nomenclature_values.get("name") if "name" in nomenclature_values else None
        )
        code_to_check = (
            nomenclature_values.get("code") if "code" in nomenclature_values else None
        )
        duplicate_errors = await _get_nomenclature_duplicate_errors(
            cashbox_id=user.cashbox_id,
            name=name_to_check,
            code=code_to_check,
            exclude_id=idx,
            check_name=check_name_duplicates,
            check_code=check_code_duplicates,
        )
        if duplicate_errors:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=", ".join(duplicate_errors),
            )

        if nomenclature_values.get("category") is not None:
            await check_entity_exists(
                categories, nomenclature_values["category"], user.id
            )

        if nomenclature_values.get("video_link") is not None:
            await validate_video_link(nomenclature_values["video_link"])

        if nomenclature_values.get("manufacturer") is not None:
            await check_entity_exists(
                manufacturers, nomenclature_values["manufacturer"], user.id
            )
        if nomenclature_values.get("unit") is not None:
            await check_unit_exists(nomenclature_values["unit"])

        if nomenclature_values.get("global_category_id") is not None:
            await public_categories_service.ensure_global_category_exists(
                nomenclature_values["global_category_id"]
            )
        else:
            # Автоматически связываем с глобальной категорией, если не указано
            if nomenclature_values.get("category") is not None:
                global_cat_id = await auto_link_global_category(
                    nomenclature_values["category"]
                )
                if global_cat_id:
                    nomenclature_values["global_category_id"] = global_cat_id
            elif nomenclature_db.get("category"):
                # Если категория не меняется, но global_category_id не указан, синхронизируем
                global_cat_id = await sync_global_category_for_nomenclature(
                    idx, nomenclature_db.get("category")
                )
                if global_cat_id:
                    nomenclature_values["global_category_id"] = global_cat_id

        # Сохраняем старую категорию для обновления has_products
        old_global_category_id = nomenclature_db.get("global_category_id")

        query = (
            nomenclature.update()
            .where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
            .values(nomenclature_values)
        )
        await database.execute(query)

        nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
        await update_entity_hash(
            table=nomenclature, table_hash=nomenclature_hash, entity=nomenclature_db
        )

        # Обновляем has_products для старой и новой категории
        categories_to_update = set()
        if old_global_category_id:
            categories_to_update.add(old_global_category_id)
        new_global_category_id = nomenclature_values.get(
            "global_category_id"
        ) or nomenclature_db.get("global_category_id")
        if new_global_category_id:
            categories_to_update.add(new_global_category_id)

        # Также обновляем, если изменился is_deleted
        if "is_deleted" in nomenclature_values:
            if new_global_category_id:
                categories_to_update.add(new_global_category_id)

        for cat_id in categories_to_update:
            try:
                await update_category_has_products(cat_id)
            except Exception:
                pass  # Игнорируем ошибки обновления

    nomenclature_db = datetime_to_timestamp(nomenclature_db)

    # Добавляем qr_hash в ответ - ВСЕГДА возвращаем актуальный хеш
    # После update_entity_hash хеш должен быть в БД, но проверяем еще раз
    hash_query = select(nomenclature_hash.c.hash).where(
        nomenclature_hash.c.nomenclature_id == idx
    )
    hash_record = await database.fetch_one(hash_query)
    if hash_record:
        # Хеш найден в БД - возвращаем его
        nomenclature_db["qr_hash"] = f"NOM:{idx}:{hash_record['hash']}"
    else:
        # Генерируем хеш, если его нет (на случай, если update_entity_hash не сработал)
        hash_base = f"{idx}:{nomenclature_db.get('name', '')}:{nomenclature_db.get('article', '')}"
        hash_string = "nm_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]
        await database.execute(
            insert(nomenclature_hash).values(
                nomenclature_id=idx, hash=hash_string, created_at=datetime.now()
            )
        )
        nomenclature_db["qr_hash"] = f"NOM:{idx}:{hash_string}"

    await manager.send_message(
        token,
        {"action": "edit", "target": "nomenclature", "result": nomenclature_db},
    )

    return nomenclature_db


@router.patch("/nomenclature/", response_model=List[schemas.Nomenclature])
async def edit_nomenclature_mass(
    token: str,
    nomenclature_data: List[schemas.NomenclatureEditMass],
):
    """Редактирование номенклатуры пачкой"""
    user = await get_user_by_token(token)
    response_body = []
    categories_to_update = set()

    settings = await _get_cashbox_duplicate_settings(user.cashbox_id)
    check_name_duplicates = settings.get("check_nomenclature_duplicates_by_name", False)
    check_code_duplicates = settings.get("check_nomenclature_duplicates_by_code", False)

    for nomenclature_in_list in nomenclature_data:
        idx = nomenclature_in_list.id
        nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
        nomenclature_values = nomenclature_in_list.dict(exclude_unset=True)

        del nomenclature_values["id"]

        # Сохраняем старую категорию для обновления has_products
        old_global_category_id = nomenclature_db.get("global_category_id")

        if nomenclature_values:
            name_to_check = (
                nomenclature_values.get("name")
                if "name" in nomenclature_values
                else None
            )
            code_to_check = (
                nomenclature_values.get("code")
                if "code" in nomenclature_values
                else None
            )
            duplicate_errors = await _get_nomenclature_duplicate_errors(
                cashbox_id=user.cashbox_id,
                name=name_to_check,
                code=code_to_check,
                exclude_id=idx,
                check_name=check_name_duplicates,
                check_code=check_code_duplicates,
            )
            if duplicate_errors:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=", ".join(duplicate_errors),
                )

            if nomenclature_values.get("category") is not None:
                await check_entity_exists(
                    categories, nomenclature_values["category"], user.id
                )

            if nomenclature_values.get("video_link") is not None:
                await validate_video_link(nomenclature_values["video_link"])

            if nomenclature_values.get("manufacturer") is not None:
                await check_entity_exists(
                    manufacturers, nomenclature_values["manufacturer"], user.id
                )
            if nomenclature_values.get("unit") is not None:
                await check_unit_exists(nomenclature_values["unit"])

            if nomenclature_values.get("global_category_id") is not None:
                await public_categories_service.ensure_global_category_exists(
                    nomenclature_values["global_category_id"]
                )
            else:
                # Автоматически связываем с глобальной категорией, если не указано
                if nomenclature_values.get("category") is not None:
                    global_cat_id = await auto_link_global_category(
                        nomenclature_values["category"]
                    )
                    if global_cat_id:
                        nomenclature_values["global_category_id"] = global_cat_id
                elif nomenclature_db.get("category"):
                    # Если категория не меняется, но global_category_id не указан, синхронизируем
                    global_cat_id = await sync_global_category_for_nomenclature(
                        idx, nomenclature_db.get("category")
                    )
                    if global_cat_id:
                        nomenclature_values["global_category_id"] = global_cat_id

            query = (
                nomenclature.update()
                .where(
                    nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id
                )
                .values(nomenclature_values)
            )
            await database.execute(query)

            nomenclature_db = await get_entity_by_id(nomenclature, idx, user.cashbox_id)
            await update_entity_hash(
                table=nomenclature, table_hash=nomenclature_hash, entity=nomenclature_db
            )

            # Собираем категории для обновления has_products
            if old_global_category_id:
                categories_to_update.add(old_global_category_id)
            new_global_category_id = nomenclature_values.get(
                "global_category_id"
            ) or nomenclature_db.get("global_category_id")
            if new_global_category_id:
                categories_to_update.add(new_global_category_id)
            # Также обновляем, если изменился is_deleted
            if "is_deleted" in nomenclature_values and new_global_category_id:
                categories_to_update.add(new_global_category_id)

        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        await manager.send_message(
            token,
            {"action": "edit", "target": "nomenclature", "result": nomenclature_db},
        )

        response_body.append(nomenclature_db)

    return response_body


@router.delete("/nomenclature/{idx}/", response_model=schemas.Nomenclature)
async def delete_nomenclature(token: str, idx: int):
    """Удаление категории"""
    user = await get_user_by_token(token)

    await get_entity_by_id(nomenclature, idx, user.cashbox_id)

    query = (
        nomenclature.update()
        .where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = nomenclature.select().where(
        nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id
    )
    nomenclature_db = await database.fetch_one(query)
    nomenclature_db = datetime_to_timestamp(nomenclature_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "nomenclature",
            "result": nomenclature_db,
        },
    )

    return nomenclature_db


@router.delete("/nomenclature/", response_model=List[schemas.Nomenclature])
async def delete_nomenclature_mass(token: str, nomenclature_data: List[int]):
    """Удаление категории пачкой"""
    user = await get_user_by_token(token)

    response_body = []

    for idx in nomenclature_data:
        await get_entity_by_id(nomenclature, idx, user.cashbox_id)

        query = (
            nomenclature.update()
            .where(nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id)
            .values({"is_deleted": True})
        )
        await database.execute(query)

        query = nomenclature.select().where(
            nomenclature.c.id == idx, nomenclature.c.cashbox == user.cashbox_id
        )
        nomenclature_db = await database.fetch_one(query)
        nomenclature_db = datetime_to_timestamp(nomenclature_db)

        await manager.send_message(
            token,
            {
                "action": "delete",
                "target": "nomenclature",
                "result": nomenclature_db,
            },
        )

        response_body.append(nomenclature_db)

    return response_body
