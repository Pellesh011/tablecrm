from datetime import datetime

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.qr_service.constants import QrEntityTypes
from api.marketplace.service.qr_service.schemas import QRResolveResponse
from database.db import (
    database,
    nomenclature,
    nomenclature_hash,
    price_types,
    prices,
    warehouse_hash,
    warehouses,
)
from fastapi import HTTPException
from sqlalchemy import and_, select


class MarketplaceQrService(BaseMarketplaceService):
    @staticmethod
    async def resolve_qr(qr_hash: str) -> QRResolveResponse:
        nomenclature_query = select(nomenclature_hash.c.nomenclature_id).where(
            nomenclature_hash.c.hash == qr_hash
        )
        warehouse_query = select(warehouse_hash.c.warehouses_id).where(
            warehouse_hash.c.hash == qr_hash
        )
        nomenclature_db = await database.fetch_one(nomenclature_query)
        warehouse_db = await database.fetch_one(warehouse_query)

        if nomenclature_db:
            product_query = select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.description_long,
                nomenclature.c.code,
                nomenclature.c.cashbox,
                nomenclature.c.production_time_min_from,
                nomenclature.c.production_time_min_to,
            ).where(
                and_(
                    nomenclature.c.id == nomenclature_db.nomenclature_id,
                    nomenclature.c.is_deleted == False,
                )
            )
            product = await database.fetch_one(product_query)
            if not product:
                raise HTTPException(
                    status_code=404, detail="Товар не найден или не доступен"
                )

            price_query = (
                select(prices.c.price, price_types.c.name.label("price_type"))
                .select_from(
                    prices.join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(
                    and_(
                        prices.c.nomenclature == nomenclature_db.nomenclature_id,
                        price_types.c.name == "chatting",
                    )
                )
            )
            price_data = await database.fetch_one(price_query)
            entity_data = {
                "id": product.id,
                "name": product.name,
                "description_short": product.description_short,
                "description_long": product.description_long,
                "code": product.code,
                "unit_name": None,  # TODO: add data
                "category_name": None,
                "manufacturer_name": None,
                "price": (
                    float(price_data.price) if price_data and price_data.price else 0.0
                ),
                "price_type": price_data.price_type if price_data else "chatting",
                "images": [],
                "barcodes": [],
                "production_time_min_from": product.production_time_min_from,
                "production_time_min_to": product.production_time_min_to,
            }
            return QRResolveResponse(
                type=QrEntityTypes.NOMENCLATURE,
                entity=entity_data,
                qr_hash=qr_hash,
                resolved_at=datetime.now(),
            )

        elif warehouse_db:
            warehouse_query = select(
                warehouses.c.id,
                warehouses.c.name,
                warehouses.c.address,
                warehouses.c.cashbox,
                warehouses.c.owner,
                warehouses.c.created_at,
                warehouses.c.updated_at,
            ).where(
                and_(
                    warehouses.c.id == warehouse_db.warehouses_id,
                    warehouses.c.is_deleted.is_not(True),
                )
            )

            warehouse = await database.fetch_one(warehouse_query)
            if not warehouse:
                raise HTTPException(
                    status_code=404, detail="Локация не найдена или не доступна"
                )

            entity_data = {
                "id": warehouse.id,
                "name": warehouse.name,
                "admin_id": warehouse.owner,
                "created_at": warehouse.created_at,
                "updated_at": warehouse.updated_at,
                "avg_rating": None,  # TODO: add reviews
                "reviews_count": 0,
            }
            return QRResolveResponse(
                type=QrEntityTypes.WAREHOUSE,
                entity=entity_data,
                qr_hash=qr_hash,
                resolved_at=datetime.now(),
            )
        else:
            raise HTTPException(
                status_code=404, detail="QR-код не найден или неактивен"
            )
