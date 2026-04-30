from typing import Optional

from common.utils.url_helper import get_app_url_for_environment
from database.db import (
    cboxes,
    database,
    docs_sales,
    marketplace_rating_aggregates,
    nomenclature,
    price_types,
    prices,
    users,
    warehouse_balances_latest,
    warehouses,
)
from sqlalchemy import Integer, and_, cast, func, select

from .schemas import (
    SellerListItem,
    SellersListResponse,
    SellerStatisticsItem,
    SellerStatisticsResponse,
)


class MarketplaceSellerStatisticsService:
    @staticmethod
    def __transform_photo_route(photo_path: str) -> str:
        base_url = get_app_url_for_environment()
        if not base_url:
            raise ValueError("APP_URL не настроен для текущего окружения")
        photo_url = photo_path.lstrip("/")

        if "seller" in photo_url:
            return f"https://{base_url}/api/v1/{photo_path.lstrip('/')}"
        else:
            return f"https://{base_url}/{photo_path.lstrip('/')}"

    async def get_sellers_list(
        self,
        search: Optional[str] = None,
        limit: int = 100,
    ) -> SellersListResponse:
        """
        Возвращает облегчённый список всех продавцов.
        """
        products_count_sq = (
            select(
                nomenclature.c.cashbox.label("seller_id"),
                func.count(nomenclature.c.id).label("total_products"),
            )
            .where(nomenclature.c.is_deleted.is_not(True))
            .group_by(nomenclature.c.cashbox)
            .subquery("products_count")
        )

        seller_name_col = func.coalesce(
            func.nullif(cboxes.c.seller_name, ""),
            cboxes.c.name,
        ).label("seller_name")

        query = (
            select(
                cboxes.c.id.label("id"),
                seller_name_col,
                cboxes.c.seller_description.label("seller_description"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),
                func.coalesce(products_count_sq.c.total_products, 0).label(
                    "total_products"
                ),
            ).select_from(
                cboxes.outerjoin(users, users.c.id == cboxes.c.admin).outerjoin(
                    products_count_sq,
                    products_count_sq.c.seller_id == cboxes.c.id,
                )
            )
            # Убираем все фильтры, чтобы получить всех продавцов
        )

        if search and search.strip():
            query = query.where(
                func.lower(
                    func.coalesce(func.nullif(cboxes.c.seller_name, ""), cboxes.c.name)
                ).contains(search.strip().lower())
            )

        query = query.order_by(seller_name_col.asc()).limit(limit)

        rows = await database.fetch_all(query)

        sellers = []
        for row in rows:
            photo = row["seller_photo"]
            if photo:
                try:
                    photo = self.__transform_photo_route(photo)
                except Exception:
                    pass

            sellers.append(
                SellerListItem(
                    id=row["id"],
                    name=row["seller_name"] or f"Продавец #{row['id']}",
                    description=row["seller_description"],
                    photo=photo,
                    total_products=int(row["total_products"] or 0),
                )
            )

        return SellersListResponse(sellers=sellers, count=len(sellers))

    async def get_sellers_statistics(self) -> SellerStatisticsResponse:
        sellers_with_products_subquery = (
            select(nomenclature.c.cashbox.label("seller_id"))
            .select_from(
                nomenclature.join(
                    prices, prices.c.nomenclature == nomenclature.c.id
                ).join(price_types, price_types.c.id == prices.c.price_type)
            )
            .where(
                and_(
                    nomenclature.c.is_deleted.is_not(True),
                    prices.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                )
            )
            .distinct()
            .subquery("sellers_with_products")
        )

        sellers_query = (
            select(
                cboxes.c.id.label("id"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("seller_name"),
                cboxes.c.seller_description.label("seller_description"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),
                cboxes.c.created_at.label("created_at"),
            )
            .select_from(cboxes.outerjoin(users, users.c.id == cboxes.c.admin))
            .where(
                and_(
                    cboxes.c.balance > 0,
                    cboxes.c.id.in_(select(sellers_with_products_subquery.c.seller_id)),
                )
            )
        )

        seller_rows = await database.fetch_all(sellers_query)

        if not seller_rows:
            return SellerStatisticsResponse(sellers=[])

        seller_ids = [row["id"] for row in seller_rows]

        warehouses_query = (
            select(
                warehouses.c.cashbox.label("seller_id"),
                func.count(warehouses.c.id).label("active_warehouses"),
            )
            .where(
                and_(
                    warehouses.c.cashbox.in_(seller_ids),
                    warehouses.c.status.is_(True),
                    warehouses.c.is_deleted.is_not(True),
                )
            )
            .group_by(warehouses.c.cashbox)
        )
        warehouses_rows = await database.fetch_all(warehouses_query)
        warehouses_map = {
            row["seller_id"]: row["active_warehouses"] or 0 for row in warehouses_rows
        }

        wb_latest = warehouse_balances_latest.alias("wb_latest")

        total_products_query = (
            select(
                nomenclature.c.cashbox.label("seller_id"),
                func.coalesce(
                    func.sum(func.greatest(wb_latest.c.current_amount, 0)),
                    0,
                ).label("total_products"),
            )
            .select_from(
                wb_latest.join(
                    nomenclature,
                    nomenclature.c.id == wb_latest.c.nomenclature_id,
                ).join(
                    warehouses,
                    warehouses.c.id == wb_latest.c.warehouse_id,
                )
            )
            .where(
                and_(
                    nomenclature.c.cashbox.in_(seller_ids),
                    warehouses.c.status.is_(True),
                    warehouses.c.is_deleted.is_not(True),
                )
            )
            .group_by(nomenclature.c.cashbox)
        )

        total_products_rows = await database.fetch_all(total_products_query)
        total_products_map = {
            row["seller_id"]: int(row["total_products"] or 0)
            for row in total_products_rows
        }

        ratings_query = (
            select(
                nomenclature.c.cashbox.label("seller_id"),
                func.avg(marketplace_rating_aggregates.c.avg_rating).label("rating"),
                func.sum(marketplace_rating_aggregates.c.reviews_count).label(
                    "reviews"
                ),
            )
            .select_from(
                marketplace_rating_aggregates.join(
                    nomenclature,
                    nomenclature.c.id == marketplace_rating_aggregates.c.entity_id,
                )
            )
            .where(
                and_(
                    marketplace_rating_aggregates.c.entity_type == "nomenclature",
                    nomenclature.c.cashbox.in_(seller_ids),
                )
            )
            .group_by(nomenclature.c.cashbox)
        )
        ratings_rows = await database.fetch_all(ratings_query)
        ratings_map = {}
        for row in ratings_rows:
            seller_id = row["seller_id"]
            ratings_map[seller_id] = {
                "rating": float(row["rating"]) if row["rating"] is not None else None,
                "reviews_count": (
                    int(row["reviews"]) if row["reviews"] is not None else 0
                ),
            }

        orders_query = (
            select(
                docs_sales.c.cashbox.label("seller_id"),
                func.count(docs_sales.c.id).label("total"),
                func.sum(
                    cast(
                        docs_sales.c.order_status.in_(["delivered", "success"]),
                        Integer,
                    )
                ).label("completed"),
                func.max(docs_sales.c.created_at).label("last_order"),
            )
            .where(
                and_(
                    docs_sales.c.cashbox.in_(seller_ids),
                    docs_sales.c.is_deleted.is_not(True),
                )
            )
            .group_by(docs_sales.c.cashbox)
        )
        orders_rows = await database.fetch_all(orders_query)
        orders_map = {}
        for row in orders_rows:
            seller_id = row["seller_id"]
            orders_map[seller_id] = {
                "orders_total": int(row["total"] or 0),
                "orders_completed": int(row["completed"] or 0),
                "last_order_date": row["last_order"],
            }

        sellers = []
        for row in seller_rows:
            seller_id = row["id"]

            rating_data = ratings_map.get(
                seller_id, {"rating": None, "reviews_count": 0}
            )
            order_data = orders_map.get(
                seller_id,
                {"orders_total": 0, "orders_completed": 0, "last_order_date": None},
            )

            active_warehouses = warehouses_map.get(seller_id, 0)
            total_products = total_products_map.get(seller_id, 0)

            seller_photo = row["seller_photo"]
            if seller_photo:
                seller_photo = self.__transform_photo_route(seller_photo)

            sellers.append(
                {
                    "id": seller_id,
                    "seller_name": row["seller_name"],
                    "seller_description": row["seller_description"],
                    "seller_photo": seller_photo,
                    "rating": rating_data["rating"],
                    "reviews_count": rating_data["reviews_count"],
                    "orders_total": order_data["orders_total"],
                    "orders_completed": order_data["orders_completed"],
                    "registration_date": row["created_at"],
                    "last_order_date": order_data["last_order_date"],
                    "active_warehouses": active_warehouses,
                    "total_products": total_products,
                }
            )

        return SellerStatisticsResponse(
            sellers=[SellerStatisticsItem(**s) for s in sellers]
        )
