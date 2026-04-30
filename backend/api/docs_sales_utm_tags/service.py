from api.docs_sales_utm_tags import schemas
from database.db import database, docs_sales, docs_sales_utm_tags
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from sqlalchemy import and_, select
from starlette import status
from starlette.responses import Response


class DocsSalesUTMTagsService:
    @staticmethod
    async def get_utm_tag(token: str, idx: int):
        user = await get_user_by_token(token)
        check_docs_sales_exist = select(docs_sales.c.id).where(
            and_(
                docs_sales.c.id == idx,
                docs_sales.c.cashbox == user.cashbox_id,
                docs_sales.c.is_deleted == False,
            )
        )

        if not await database.fetch_one(check_docs_sales_exist):
            raise HTTPException(
                status_code=404,
                detail="docs_sale не существует!",
            )
        query = docs_sales_utm_tags.select().where(
            and_(docs_sales_utm_tags.c.docs_sales_id == idx)
        )
        row = await database.fetch_one(query)
        if not row:
            raise HTTPException(status_code=404, detail="Utm tag not found")
        return schemas.UtmTag(
            id=row.id,
            docs_sales_id=row.docs_sales_id,
            utm_source=row.utm_source,
            utm_medium=row.utm_medium,
            utm_campaign=row.utm_campaign,
            utm_term=row.utm_term,
            utm_content=row.utm_content,
            utm_name=row.utm_name,
            utm_phone=row.utm_phone,
            utm_email=row.utm_email,
            utm_leadid=row.utm_leadid,
            utm_yclientid=row.utm_yclientid,
            utm_gaclientid=row.utm_gaclientid,
        )

    @staticmethod
    async def create_utm_tag(token: str, idx: int, utm_tags_data: schemas.CreateUTMTag):
        user = await get_user_by_token(token)

        check_docs_sales_exist = select(docs_sales.c.id).where(
            and_(
                docs_sales.c.id == idx,
                docs_sales.c.cashbox == user.cashbox_id,
                docs_sales.c.is_deleted == False,
            )
        )

        if not await database.fetch_one(check_docs_sales_exist):
            raise HTTPException(
                status_code=404,
                detail="docs_sale не существует!",
            )

        check_utm_exist_query = select(docs_sales_utm_tags.c.id).where(
            docs_sales_utm_tags.c.docs_sales_id == idx
        )
        if await database.fetch_one(check_utm_exist_query):
            raise HTTPException(400, detail="This document already exists")

        data = utm_tags_data.dict()

        query = docs_sales_utm_tags.insert().values(
            docs_sales_id=idx,
            utm_source=utm_tags_data.utm_source,
            utm_medium=utm_tags_data.utm_medium,
            utm_campaign=utm_tags_data.utm_campaign,
            utm_term=utm_tags_data.utm_term,
            utm_content=utm_tags_data.utm_content,
            utm_name=utm_tags_data.utm_name,
            utm_phone=utm_tags_data.utm_phone,
            utm_email=utm_tags_data.utm_email,
            utm_leadid=utm_tags_data.utm_leadid,
            utm_yclientid=utm_tags_data.utm_yclientid,
            utm_gaclientid=utm_tags_data.utm_gaclientid,
        )
        new_utm_id = await database.execute(query)

        return schemas.UtmTag(id=new_utm_id, docs_sales_id=idx, **data)

    @staticmethod
    async def update_utm_tag(token: str, idx: int, data: schemas.CreateUTMTag):
        user = await get_user_by_token(token)
        check_docs_sales_exist = select(docs_sales.c.id).where(
            and_(
                docs_sales.c.id == idx,
                docs_sales.c.cashbox == user.cashbox_id,
                docs_sales.c.is_deleted == False,
            )
        )

        if not await database.fetch_one(check_docs_sales_exist):
            raise HTTPException(
                status_code=404,
                detail="docs_sale не существует!",
            )

        try:
            query = (
                docs_sales_utm_tags.update()
                .where(
                    and_(
                        docs_sales_utm_tags.c.docs_sales_id == idx,
                    )
                )
                .values(**data.dict())
            )
            await database.execute(query)
        except Exception as e:
            raise HTTPException(status_code=400, detail="Ошибка при обновлении тэга!")
        query = docs_sales_utm_tags.select().where(
            docs_sales_utm_tags.c.docs_sales_id == idx
        )
        row = await database.fetch_one(query)
        return schemas.UtmTag(
            id=row.id,
            docs_sales_id=row.docs_sales_id,
            utm_source=row.utm_source,
            utm_medium=row.utm_medium,
            utm_campaign=row.utm_campaign,
            utm_term=row.utm_term,
            utm_content=row.utm_content,
            utm_name=row.utm_name,
            utm_phone=row.utm_phone,
            utm_email=row.utm_email,
            utm_leadid=row.utm_leadid,
            utm_yclientid=row.utm_yclientid,
            utm_gaclientid=row.utm_gaclientid,
        )

    @staticmethod
    async def delete_utm_tag(token: str, idx: int):
        user = await get_user_by_token(token)
        check_docs_sales_exist = select(docs_sales.c.id).where(
            and_(
                docs_sales.c.id == idx,
                docs_sales.c.cashbox == user.cashbox_id,
                docs_sales.c.is_deleted == False,
            )
        )

        if not await database.fetch_one(check_docs_sales_exist):
            raise HTTPException(
                status_code=404,
                detail="docs_sale не существует!",
            )
        query = (
            docs_sales_utm_tags.delete()
            .where(and_(docs_sales_utm_tags.c.docs_sales_id == idx))
            .returning(docs_sales_utm_tags.c.id)
        )
        deleted_id = await database.execute(query)
        if not deleted_id:
            raise HTTPException(status_code=404, detail="Utm tag не найден")
        return Response(status_code=status.HTTP_204_NO_CONTENT, content=None)


async def get_docs_sales_utm_service():
    return DocsSalesUTMTagsService()
