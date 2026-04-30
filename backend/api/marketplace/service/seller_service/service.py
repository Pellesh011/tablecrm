import io
from os import environ
from typing import Optional
from uuid import uuid4

import aioboto3
from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.seller_service.schemas import (
    SellerResponse,
    SellerUpdateRequest,
)
from common.utils.url_helper import get_app_url_for_environment
from database.db import cboxes, database, users
from databases import Database
from fastapi import HTTPException, UploadFile
from functions.helpers import get_user_by_token
from sqlalchemy import func, select

s3_session = aioboto3.Session()
s3_data = {
    "service_name": "s3",
    "endpoint_url": environ.get("S3_URL"),
    "aws_access_key_id": environ.get("S3_ACCESS"),
    "aws_secret_access_key": environ.get("S3_SECRET"),
}

bucket_name = "5075293c-docs_generated"


class MarketplaceSellerService(BaseMarketplaceService):
    """
    Сервис для работы с профилем селлера
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def __transform_photo_route(photo_path: str) -> str:
        base_url = get_app_url_for_environment()
        if not base_url:
            raise ValueError("APP_URL не настроен для текущего окружения")
        return f"https://{base_url}/api/v1/{photo_path.lstrip('/')}"

    async def update_seller_profile(
        self,
        payload: SellerUpdateRequest,
        file: Optional[UploadFile],
        token: str,
        *,
        db: Database = database,
    ) -> SellerResponse:
        # 1. Авторизация по токену
        user = await get_user_by_token(token)
        cashbox_id = user.cashbox_id

        # 2. Проверяем, что селлер существует и вытаскиваем admin_id
        cashbox = await db.fetch_one(
            select(
                cboxes.c.id,
                cboxes.c.admin,
            ).where(cboxes.c.id == cashbox_id)
        )

        if cashbox is None:
            raise HTTPException(404, "Селлер не найден")

        # 3. Если нужно обновить фото — проверяем файл
        new_photo_path = None
        if file:
            if "image" not in file.content_type:
                raise HTTPException(422, "Вы загрузили не картинку")

            file_bytes = await file.read()
            file_ext = file.filename.split(".")[-1]

            new_photo_path = f"photos/seller_{cashbox_id}_{uuid4().hex[:8]}.{file_ext}"

            async with s3_session.client(**s3_data) as s3:
                await s3.upload_fileobj(
                    io.BytesIO(file_bytes), bucket_name, new_photo_path
                )

        # 4. Обновляем БД (имя/описание/фото)
        async with db.transaction():
            update_values = {}

            if payload.name is not None:
                update_values["seller_name"] = payload.name

            if payload.description is not None:
                update_values["seller_description"] = payload.description

            if new_photo_path:
                update_values["seller_photo"] = new_photo_path

            if update_values:
                await db.execute(
                    cboxes.update()
                    .where(cboxes.c.id == cashbox_id)
                    .values(update_values)
                )

        # 5. Возвращаем обновлённые данные
        row = await db.fetch_one(
            select(
                cboxes.c.id,
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("name"),
                cboxes.c.seller_description.label("description"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("photo"),
            )
            .select_from(cboxes.join(users, cboxes.c.admin == users.c.id))
            .where(cboxes.c.id == cashbox_id)
        )

        data = dict(row)

        if data.get("photo"):
            data["photo"] = self.__transform_photo_route(data["photo"])

        return SellerResponse(**data)
