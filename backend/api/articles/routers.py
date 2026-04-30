from datetime import datetime

import aiofiles
import api.articles.schemas as article_schemas
import functions.filter_schemas as filter_schemas
from database.db import articles, database, users_cboxes_relation
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from functions.helpers import check_article_exists, get_filters_articles
from sqlalchemy import asc, desc, func, select
from ws_manager import manager

router = APIRouter(tags=["articles"])


@router.get("/articles/{id}/", response_model=article_schemas.Article)
async def get_article_by_id(token: str, id: int):
    """Получение статьи по ID"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = articles.select().where(
                articles.c.id == id, articles.c.cashbox == user.cashbox_id
            )
            article_db = await database.fetch_one(query)

            if article_db:
                if article_db.cashbox == user.cashbox_id:
                    return article_db

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/articles/", response_model=article_schemas.GetArticles)
async def get_articles(
    token: str,
    limit: int = 100,
    offset: int = 0,
    sort: str = "created_at:desc",
    filters: filter_schemas.ArticlesFiltersQuery = Depends(),
):
    """Получение статей кассы"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            filters = get_filters_articles(articles, filters)
            sort_list = sort.split(":")
            if sort_list[0] not in ["created_at", "updated_at"]:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )
            if sort_list[1] == "desc":
                query = (
                    articles.select()
                    .where(articles.c.cashbox == user.cashbox_id)
                    .filter(*filters)
                    .order_by(desc(getattr(articles.c, sort_list[0])))
                    .limit(limit)
                    .offset(offset)
                )

            elif sort_list[1] == "asc":
                query = (
                    articles.select()
                    .where(articles.c.cashbox == user.cashbox_id)
                    .filter(*filters)
                    .order_by(asc(getattr(articles.c, sort_list[0])))
                    .limit(limit)
                    .offset(offset)
                )
            else:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )

            articles_db = await database.fetch_all(query)

            q = (
                select(func.count(articles.c.id))
                .where(articles.c.cashbox == user.cashbox_id)
                .filter(*filters)
            )
            count = await database.fetch_one(q)

            return {"result": articles_db, "count": count.count_1}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.post("/articles/")
async def new_article(token: str, article: article_schemas.ArticleCreate):
    """Создание статьи"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            created = int(datetime.utcnow().timestamp())
            article_values: dict = article.dict()
            article_values.update(
                {
                    "created_at": created,
                    "updated_at": created,
                    "cashbox": user.cashbox_id,
                }
            )
            if await check_article_exists(
                article_values.get("name"), user.cashbox_id, article_values.get("dc")
            ):
                raise HTTPException(
                    status_code=400, detail="Эта статья уже существует!"
                )

            query = articles.insert().values(article_values)
            article_id = await database.execute(query)

            query = articles.select(articles.c.id == article_id)
            article_db = await database.fetch_one(query)

            await manager.send_message(
                token,
                {"action": "create", "target": "articles", "result": dict(article_db)},
            )

            return article_db

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.patch("/articles/")
async def edit_article(token: str, article: article_schemas.ArticleEdit):
    """Редактирование статьи"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = articles.select().where(
                articles.c.id == article.id, articles.c.cashbox == user.cashbox_id
            )
            article_db = await database.fetch_one(query)
            if article_db.cashbox != user.cashbox_id:
                raise HTTPException(403, "Not your article")

            article_dict = article.dict(exclude_unset=True)
            del article_dict["id"]
            if article_dict:
                if (
                    article_dict.get("name")
                    and article_dict.get("name") != article_db.name
                ) or article_dict.get("dc") != article_db.dc:
                    exists = await check_article_exists(
                        (
                            article_dict.get("name")
                            if article_dict.get("name")
                            else article_db.name
                        ),
                        user.cashbox_id,
                        (
                            article_dict.get("dc")
                            if article_dict.get("dc")
                            else article_db.dc
                        ),
                    )
                    if exists:
                        raise HTTPException(
                            status_code=400, detail="Эта статья уже существует!"
                        )
                article_dict["updated_at"] = int(datetime.now().timestamp())
                query = (
                    articles.update()
                    .where(
                        articles.c.id == article.id,
                        articles.c.cashbox == user.cashbox_id,
                    )
                    .values(article_dict)
                )
                await database.execute(query)

                query = articles.select().where(
                    articles.c.id == article.id,
                    articles.c.cashbox == user.cashbox_id,
                )
                article_db = await database.fetch_one(query)

                await manager.send_message(
                    token,
                    {
                        "action": "edit",
                        "target": "articles",
                        "result": dict(article_db),
                    },
                )

                return article_db
            return {"status": "пустое поле"}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.put("/articles/add_icon/")
async def add_icon_to_article(
    token: str, article_id: int, icon_file: UploadFile = File(...)
):
    """Изменить иконку статьи"""
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if user:
        if user.status:
            query = articles.select().where(
                articles.c.id == article_id, articles.c.cashbox == user.cashbox_id
            )
            article_db = await database.fetch_one(query)
            if article_db.cashbox == user.cashbox_id:
                new_values = {}
                file_link = f"photos/icon_article_{article_id}.{icon_file.filename.split('.')[-1]}"
                file_bytes = await icon_file.read()

                try:
                    async with aiofiles.open(file_link, "+wb") as file:
                        await file.write(file_bytes)
                finally:
                    await icon_file.close()

                new_values.update(
                    {
                        "icon_file": file_link,
                        "updated_at": int(datetime.now().timestamp()),
                    }
                )

                query = (
                    articles.update()
                    .where(
                        articles.c.id == article_id,
                        articles.c.cashbox == user.cashbox_id,
                    )
                    .values(new_values)
                )
                await database.execute(query)

                query = articles.select().where(
                    articles.c.id == article_id, articles.c.cashbox == user.cashbox_id
                )
                article_db = await database.fetch_one(query)

                await manager.send_message(
                    token,
                    {
                        "action": "edit",
                        "target": "articles",
                        "result": dict(article_db),
                    },
                )
                return article_db
