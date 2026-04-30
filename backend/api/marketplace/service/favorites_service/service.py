from collections import defaultdict
from datetime import datetime
from typing import Literal, Optional, Union

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.favorites_service.schemas import (
    CreateFavoritesUtm,
    FavoriteGroup,
    FavoriteGroupedListResponse,
    FavoriteListResponse,
    FavoriteRequest,
    FavoriteResponse,
    FavoritesFilters,
    FavoritesGroupBy,
    FavoritesSortBy,
)
from common.utils.url_helper import get_app_url_for_environment
from database.db import (
    cboxes,
    database,
    marketplace_clients_list,
    marketplace_favorites,
    nomenclature,
    price_types,
    prices,
    users,
)
from fastapi import HTTPException
from sqlalchemy import and_, asc, desc, func, select

ENTITY_TYPE_NOMENCLATURE = "nomenclature"


def _transform_seller_photo(photo_path: Optional[str]) -> Optional[str]:
    if not photo_path:
        return None
    base_url = get_app_url_for_environment()
    if not base_url:
        return None
    photo_url = photo_path.lstrip("/")
    if "seller" in photo_url:
        return f"https://{base_url}/api/v1/{photo_url}"
    return f"https://{base_url}/{photo_url}"


class MarketplaceFavoritesService(BaseMarketplaceService):
    async def get_favorites(
        self,
        contragent_phone: str,
        page: int,
        size: int,
        sort_by: Optional[FavoritesSortBy] = None,
        sort_order: Literal["asc", "desc"] = "desc",
        group_by: Optional[FavoritesGroupBy] = None,
        filters: Optional[FavoritesFilters] = None,
    ) -> Union[FavoriteListResponse, FavoriteGroupedListResponse]:
        normalized_phone = BaseMarketplaceService._validate_phone(contragent_phone)
        client_query = select(marketplace_clients_list.c.id).where(
            marketplace_clients_list.c.phone == normalized_phone
        )
        client = await database.fetch_one(client_query)
        if not client:
            if group_by:
                return FavoriteGroupedListResponse(
                    groups=[], total_count=0, page=page, size=size
                )
            return FavoriteListResponse(result=[], count=0, page=page, size=size)

        # Подзапрос для актуальной цены (chatting, не удалена)
        current_ts = int(datetime.now().timestamp())
        ranked_prices = (
            select(
                prices.c.nomenclature.label("nomenclature_id"),
                prices.c.price,
                func.row_number()
                .over(
                    partition_by=prices.c.nomenclature,
                    order_by=[
                        desc(
                            and_(
                                func.coalesce(prices.c.date_from <= current_ts, True),
                                func.coalesce(current_ts < prices.c.date_to, True),
                            )
                        ),
                        desc(prices.c.created_at),
                        desc(prices.c.id),
                    ],
                )
                .label("rn"),
            )
            .select_from(
                prices.join(price_types, price_types.c.id == prices.c.price_type)
            )
            .where(
                and_(
                    prices.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                )
            )
        ).subquery()

        active_prices = (
            select(ranked_prices.c.nomenclature_id, ranked_prices.c.price)
            .where(ranked_prices.c.rn == 1)
            .subquery()
        )

        query = (
            select(
                marketplace_favorites.c.id,
                marketplace_favorites.c.phone,
                marketplace_favorites.c.entity_id.label("nomenclature_id"),
                marketplace_favorites.c.created_at,
                marketplace_favorites.c.updated_at,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.created_at.label("product_created_at"),
                nomenclature.c.cashbox.label("seller_id"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("seller_name"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),
                active_prices.c.price,
            )
            .select_from(marketplace_favorites)
            .join(
                nomenclature,
                nomenclature.c.id == marketplace_favorites.c.entity_id,
            )
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .outerjoin(
                active_prices,
                active_prices.c.nomenclature_id == nomenclature.c.id,
            )
            .where(
                and_(
                    marketplace_favorites.c.client_id == client.id,
                    marketplace_favorites.c.entity_type == ENTITY_TYPE_NOMENCLATURE,
                    nomenclature.c.is_deleted.is_not(True),
                )
            )
        )

        if filters:
            query = self._apply_filters(query, filters, active_prices)

        if sort_by:
            query = self._apply_sort(query, sort_by, sort_order, active_prices)
        else:
            query = query.order_by(desc(marketplace_favorites.c.created_at))

        if group_by:
            return await self._get_favorites_grouped(
                query, page, size, group_by, client.id
            )

        base_query_for_count = query
        total_count = await database.fetch_val(
            select(func.count()).select_from(base_query_for_count.subquery())
        )

        offset = (page - 1) * size
        query = query.limit(size).offset(offset)
        rows = await database.fetch_all(query)

        result = [self._row_to_favorite_response(row) for row in rows]

        return FavoriteListResponse(
            result=result, count=total_count, page=page, size=size
        )

    def _apply_filters(self, query, filters: FavoritesFilters, price_subq):
        seller_col = func.coalesce(
            func.nullif(cboxes.c.seller_name, ""),
            cboxes.c.name,
        )
        _specs = [
            ("name", nomenclature.c.name, "ilike"),
            ("description", nomenclature.c.description_short, "ilike"),
            ("seller_name", seller_col, "ilike"),
            ("favorite_created_at_from", marketplace_favorites.c.created_at, "gte"),
            ("favorite_created_at_to", marketplace_favorites.c.created_at, "lte"),
            ("product_created_at_from", nomenclature.c.created_at, "gte"),
            ("product_created_at_to", nomenclature.c.created_at, "lte"),
            ("seller_id", nomenclature.c.cashbox, "eq"),
            ("min_price", price_subq.c.price, "gte"),
            ("max_price", price_subq.c.price, "lte"),
        ]
        conditions = []
        for attr, col, op in _specs:
            val = getattr(filters, attr, None)
            if val is None or (isinstance(val, str) and not val.strip()):
                continue
            if op == "ilike":
                conditions.append(
                    func.lower(col).ilike(f"%{str(val).strip().lower()}%")
                )
            elif op == "gte":
                conditions.append(col >= val)
            elif op == "lte":
                conditions.append(col <= val)
            elif op == "eq":
                conditions.append(col == val)
        if conditions:
            query = query.where(and_(*conditions))
        return query

    def _apply_sort(self, query, sort_by: FavoritesSortBy, sort_order: str, price_subq):
        sort_map = {
            FavoritesSortBy.name: nomenclature.c.name,
            FavoritesSortBy.description: nomenclature.c.description_short,
            FavoritesSortBy.favorite_created_at: marketplace_favorites.c.created_at,
            FavoritesSortBy.product_created_at: nomenclature.c.created_at,
            FavoritesSortBy.seller: func.coalesce(
                func.nullif(cboxes.c.seller_name, ""),
                cboxes.c.name,
            ),
            FavoritesSortBy.price: price_subq.c.price,
        }
        col = sort_map.get(sort_by)
        if col is not None:
            order_fn = asc if sort_order == "asc" else desc
            return query.order_by(order_fn(col))
        return query

    async def _fetch_favorite_enriched(self, favorite_id: int):
        """Получить избранное с обогащёнными данными (name, price, seller_name и т.д.)"""
        current_ts = int(datetime.now().timestamp())
        ranked_prices = (
            select(
                prices.c.nomenclature.label("nomenclature_id"),
                prices.c.price,
                func.row_number()
                .over(
                    partition_by=prices.c.nomenclature,
                    order_by=[
                        desc(
                            and_(
                                func.coalesce(prices.c.date_from <= current_ts, True),
                                func.coalesce(current_ts < prices.c.date_to, True),
                            )
                        ),
                        desc(prices.c.created_at),
                        desc(prices.c.id),
                    ],
                )
                .label("rn"),
            )
            .select_from(
                prices.join(price_types, price_types.c.id == prices.c.price_type)
            )
            .where(
                and_(
                    prices.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                )
            )
        ).subquery()
        active_prices = (
            select(ranked_prices.c.nomenclature_id, ranked_prices.c.price)
            .where(ranked_prices.c.rn == 1)
            .subquery()
        )
        query = (
            select(
                marketplace_favorites.c.id,
                marketplace_favorites.c.phone,
                marketplace_favorites.c.entity_id.label("nomenclature_id"),
                marketplace_favorites.c.created_at,
                marketplace_favorites.c.updated_at,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.created_at.label("product_created_at"),
                nomenclature.c.cashbox.label("seller_id"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("seller_name"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),
                active_prices.c.price,
            )
            .select_from(marketplace_favorites)
            .join(
                nomenclature,
                nomenclature.c.id == marketplace_favorites.c.entity_id,
            )
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .outerjoin(
                active_prices,
                active_prices.c.nomenclature_id == nomenclature.c.id,
            )
            .where(marketplace_favorites.c.id == favorite_id)
        )
        return await database.fetch_one(query)

    def _row_to_favorite_response(self, row) -> FavoriteResponse:
        seller_photo = row.get("seller_photo")
        if seller_photo:
            seller_photo = _transform_seller_photo(seller_photo)
        return FavoriteResponse(
            id=row["id"],
            nomenclature_id=row["nomenclature_id"],
            phone=row["phone"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            name=row.get("name"),
            description_short=row.get("description_short"),
            price=row.get("price"),
            product_created_at=row.get("product_created_at"),
            seller_id=row.get("seller_id"),
            seller_name=row.get("seller_name"),
            seller_photo=seller_photo,
        )

    async def _get_favorites_grouped(
        self,
        query,
        page: int,
        size: int,
        group_by: FavoritesGroupBy,
        client_id: int,
    ) -> FavoriteGroupedListResponse:
        rows = await database.fetch_all(query)
        items = [self._row_to_favorite_response(row) for row in rows]

        groups_dict: dict = defaultdict(list)
        for item in items:
            if group_by == FavoritesGroupBy.seller:
                key = item.seller_id if item.seller_id is not None else -1
                label = (
                    item.seller_name or f"Селлер #{item.seller_id}"
                    if item.seller_id
                    else "Без селлера"
                )
            elif group_by == FavoritesGroupBy.favorite_created_at:
                dt = item.created_at
                key = dt.date().isoformat() if dt else ""
                label = key
            elif group_by == FavoritesGroupBy.product_created_at:
                dt = item.product_created_at
                key = dt.date().isoformat() if dt else ""
                label = key
            else:
                key = "default"
                label = "default"
            groups_dict[(key, label)].append(item)

        groups_list = sorted(
            [
                FavoriteGroup(
                    group_key=label,
                    group_value=key,
                    items=grp_items,
                    count=len(grp_items),
                )
                for (key, label), grp_items in groups_dict.items()
            ],
            key=lambda g: g.group_key,
        )

        total_count = sum(g.count for g in groups_list)
        offset = (page - 1) * size
        paginated_groups = groups_list[offset : offset + size]

        return FavoriteGroupedListResponse(
            groups=paginated_groups,
            total_count=total_count,
            page=page,
            size=size,
        )

    async def add_to_favorites(
        self, favorite_request: FavoriteRequest, utm: CreateFavoritesUtm
    ) -> FavoriteResponse:
        phone = BaseMarketplaceService._validate_phone(
            favorite_request.contragent_phone
        )
        nomenclature_id = favorite_request.nomenclature_id
        entity_type = ENTITY_TYPE_NOMENCLATURE
        entity_id = nomenclature_id

        product_query = select(nomenclature.c.id).where(
            and_(
                nomenclature.c.id == nomenclature_id,
                nomenclature.c.is_deleted == False,
            )
        )
        entity = await database.fetch_one(product_query)
        if not entity:
            raise HTTPException(
                status_code=404, detail="Товар не найден или не доступен"
            )

        client_id = await self._ensure_marketplace_client(phone)

        existing_query = select(
            marketplace_favorites.c.id,
            marketplace_favorites.c.phone,
            marketplace_favorites.c.entity_type,
            marketplace_favorites.c.entity_id,
            marketplace_favorites.c.created_at,
            marketplace_favorites.c.updated_at,
        ).where(
            and_(
                marketplace_favorites.c.client_id == client_id,
                marketplace_favorites.c.entity_type == entity_type,
                marketplace_favorites.c.entity_id == entity_id,
            )
        )
        existing_favorite = await database.fetch_one(existing_query)

        if existing_favorite:
            await self._add_utm(existing_favorite.id, utm)
            enriched = await self._fetch_favorite_enriched(existing_favorite.id)
            return (
                self._row_to_favorite_response(enriched)
                if enriched
                else FavoriteResponse(
                    id=existing_favorite.id,
                    nomenclature_id=existing_favorite.entity_id,
                    phone=existing_favorite.phone or phone,
                    created_at=existing_favorite.created_at,
                    updated_at=existing_favorite.updated_at,
                )
            )

        favorite_id = await database.execute(
            marketplace_favorites.insert().values(
                phone=phone,
                client_id=client_id,
                entity_type=entity_type,
                entity_id=entity_id,
            )
        )

        await self._add_utm(favorite_id, utm)
        enriched = await self._fetch_favorite_enriched(favorite_id)
        if enriched:
            return self._row_to_favorite_response(enriched)
        created = await database.fetch_one(
            select(
                marketplace_favorites.c.created_at,
                marketplace_favorites.c.updated_at,
            ).where(marketplace_favorites.c.id == favorite_id)
        )
        return FavoriteResponse(
            id=favorite_id,
            nomenclature_id=entity_id,
            phone=phone,
            created_at=created.created_at if created else datetime.now(),
            updated_at=created.updated_at if created else datetime.now(),
        )

    async def remove_from_favorites(
        self, favorite_id: int, contragent_phone: str
    ) -> None:
        normalized_phone = BaseMarketplaceService._validate_phone(contragent_phone)
        client_query = select(marketplace_clients_list.c.id).where(
            marketplace_clients_list.c.phone == normalized_phone
        )
        client = await database.fetch_one(client_query)
        if not client:
            raise HTTPException(
                status_code=404,
                detail="Клиент не найден",
            )
        delete_query = marketplace_favorites.delete().where(
            and_(
                marketplace_favorites.c.id == favorite_id,
                marketplace_favorites.c.client_id == client.id,
            )
        )
        deleted_count = await database.execute(delete_query)

        if deleted_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Запись в избранном не найдена",
            )
