from typing import Optional

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.review_service.schemas import (
    CreateReviewRequest,
    MarketplaceReview,
    ReviewListRequest,
    ReviewListResponse,
    UpdateReviewRequest,
)
from database.db import (
    database,
    marketplace_rating_aggregates,
    marketplace_reviews,
)
from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import Table, and_, asc, desc, func, select


class MarketplaceReviewService(BaseMarketplaceService):
    @staticmethod
    async def __recalculate_rating_aggregate(entity_type: str, entity_id: int):
        # Получаем агрегированные данные
        agg_query = select(
            func.avg(marketplace_reviews.c.rating).label("avg_rating"),
            func.count(marketplace_reviews.c.id).label("reviews_count"),
        ).where(
            and_(
                marketplace_reviews.c.entity_type == entity_type,
                marketplace_reviews.c.entity_id == entity_id,
                marketplace_reviews.c.status == "visible",
            )
        )
        agg_result = await database.fetch_one(agg_query)

        avg_rating = (
            float(agg_result.avg_rating) if agg_result.avg_rating is not None else 0.0
        )
        reviews_count = agg_result.reviews_count or 0

        # Проверяем, существует ли запись в агрегатной таблице
        exists_query = select(marketplace_rating_aggregates.c.id).where(
            and_(
                marketplace_rating_aggregates.c.entity_type == entity_type,
                marketplace_rating_aggregates.c.entity_id == entity_id,
            )
        )
        existing = await database.fetch_one(exists_query)

        if existing:
            # Обновляем
            await database.execute(
                marketplace_rating_aggregates.update()
                .where(marketplace_rating_aggregates.c.id == existing.id)
                .values(
                    avg_rating=avg_rating,
                    reviews_count=reviews_count,
                    updated_at=func.now(),
                )
            )
        else:
            # Создаём новую запись
            await database.execute(
                marketplace_rating_aggregates.insert().values(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    avg_rating=avg_rating,
                    reviews_count=reviews_count,
                )
            )

    @staticmethod
    async def __is_entity_exist_by_id(table: Table, entity_id: int) -> bool:
        query = select(func.count(table.c.id)).where(table.c.id == entity_id)
        return True if await database.fetch_val(query) else False

    async def create_review(
        self, review_request: CreateReviewRequest
    ) -> MarketplaceReview:

        class CreateReviewDb(BaseModel):
            entity_id: int
            entity_type: str
            client_id: int
            rating: int
            text: str
            status: Optional[str] = "visible"

        # проверяем существование сущности
        if not (
            await self.__is_entity_exist_by_id(
                self._entity_types_to_tables[review_request.entity_type.value],
                review_request.entity_id,
            )
        ):
            raise HTTPException(status_code=404, detail="Entity type not found")

        client_id = await self._ensure_marketplace_client(
            phone=review_request.contragent_phone
        )

        review_id = await database.execute(
            marketplace_reviews.insert().values(
                entity_id=review_request.entity_id,
                entity_type=review_request.entity_type.value,
                client_id=client_id,
                rating=review_request.rating,
                text=review_request.text,
                status="visible",
                created_at=func.now(),
                updated_at=func.now(),
            )
        )

        created_review_query = select(marketplace_reviews).where(
            marketplace_reviews.c.id == review_id
        )
        created_review = await database.fetch_one(created_review_query)

        # Пересчитываем агрегат после создания
        await self.__recalculate_rating_aggregate(
            review_request.entity_type.value, review_request.entity_id
        )

        return MarketplaceReview.from_orm(created_review)

    @staticmethod
    async def get_reviews(request: ReviewListRequest) -> ReviewListResponse:
        offset = (request.page - 1) * request.size

        # Базовый запрос отзывов
        conditions = [
            marketplace_reviews.c.entity_type == request.entity_type.value,
            marketplace_reviews.c.entity_id == request.entity_id,
            marketplace_reviews.c.status == "visible",
        ]

        if request.view_only_rates:
            conditions.append(marketplace_reviews.c.rating == request.view_only_rates)

        query = select(marketplace_reviews).where(and_(*conditions))

        # Применяем сортировку
        if request.sort == ReviewListRequest.SortBy.newest:
            query = query.order_by(desc(marketplace_reviews.c.created_at))
        elif request.sort == ReviewListRequest.SortBy.oldest:
            query = query.order_by(asc(marketplace_reviews.c.created_at))
        elif request.sort == ReviewListRequest.SortBy.highest:
            query = query.order_by(
                desc(marketplace_reviews.c.rating),
                desc(marketplace_reviews.c.created_at),
            )
        elif request.sort == ReviewListRequest.SortBy.lowest:
            query = query.order_by(
                asc(marketplace_reviews.c.rating),
                desc(marketplace_reviews.c.created_at),
            )

        # Пагинация
        query = query.limit(request.size).offset(offset)
        reviews_rows = await database.fetch_all(query)

        # Получаем агрегатные данные из marketplace_rating_aggregates
        agg_query = select(
            marketplace_rating_aggregates.c.avg_rating,
            marketplace_rating_aggregates.c.reviews_count,
        ).where(
            and_(
                marketplace_rating_aggregates.c.entity_type
                == request.entity_type.value,
                marketplace_rating_aggregates.c.entity_id == request.entity_id,
            )
        )
        agg = await database.fetch_one(agg_query)

        total_count_query = select(func.count(marketplace_reviews.c.id)).where(
            *conditions
        )
        total_count = await database.fetch_val(total_count_query)
        avg_rating = (
            float(agg.avg_rating) if agg and agg.avg_rating is not None else None
        )

        # Преобразуем в схемы
        result = [MarketplaceReview.from_orm(review) for review in reviews_rows]

        return ReviewListResponse(
            result=result,
            count=total_count,
            page=request.page,
            size=request.size,
            avg_rating=avg_rating,
        )

    async def update_review(
        self, review_id: int, request: UpdateReviewRequest
    ) -> MarketplaceReview:

        # 1. Получаем ID контрагента по телефону
        client_id = await self._ensure_marketplace_client(
            phone=request.contragent_phone
        )

        # 2. Проверяем, существует ли отзыв и принадлежит ли он контрагенту
        existing_query = select(
            marketplace_reviews.c.id,
            marketplace_reviews.c.client_id,
            marketplace_reviews.c.entity_type,
            marketplace_reviews.c.entity_id,
        ).where(
            and_(
                marketplace_reviews.c.id == review_id,
                marketplace_reviews.c.client_id == client_id,
            )
        )
        existing_review = await database.fetch_one(existing_query)

        if not existing_review:
            raise HTTPException(
                status_code=404,
                detail="Отзыв не найден или не принадлежит указанному пользователю",
            )

        # 3. Обновляем отзыв
        update_query = (
            marketplace_reviews.update()
            .where(marketplace_reviews.c.id == review_id)
            .values(rating=request.rating, text=request.text, updated_at=func.now())
        )
        await database.execute(update_query)

        # 4. Пересчитываем агрегат
        await self.__recalculate_rating_aggregate(
            existing_review.entity_type, existing_review.entity_id
        )

        # 5. Возвращаем обновлённый отзыв
        updated_review_query = select(marketplace_reviews).where(
            marketplace_reviews.c.id == review_id
        )
        updated_review = await database.fetch_one(updated_review_query)

        return MarketplaceReview.from_orm(updated_review)
