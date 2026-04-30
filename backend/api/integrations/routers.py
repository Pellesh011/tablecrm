from database.db import database, integrations, pictures, users_cboxes_relation
from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

from ..oauth.utils import generate_rand_hex, ouath_response
from .shemas import CreateApp, ShowIntegrationGet, UpdateIntegration

router = APIRouter(prefix="/integrations", tags=["integrations"])


@router.post("/")
async def create_integration(token: str, integration: CreateApp):
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    print(integration)
    print(user)
    if user:
        if user.status:
            client_secret = generate_rand_hex()
            dct = integration.dict()
            dct["owner"] = user.id
            dct["client_secret"] = client_secret
            dct["code"] = generate_rand_hex()[:8]
            query = integrations.insert(values=dct)
            int_id = await database.execute(query)
            return {
                "id": int_id,
                **integration.dict(),
                # "client_secret": client_secret,
                # "code": dct["code"] ##TODO Для тестов
            }
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/", response_model=ShowIntegrationGet)
async def get_ints_by_token(token: str):
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if user:
        if user.status:
            query = integrations.select().where(integrations.c.owner == user.id)
            ints = await database.fetch_all(query)

            query = select(func.count(integrations.c.id)).where(
                integrations.c.owner == user.id
            )
            count = await database.fetch_one(query)

            res_ints = []
            for integr in ints:
                integr_dict = dict(integr)
                query = pictures.select().where(
                    pictures.c.owner == user.id,
                    pictures.c.entity == "integrations",
                    pictures.c.entity_id == integr["id"],
                    pictures.c.is_main == True,
                )
                img = await database.fetch_one(query)
                url = img.url if img else ""
                integr_dict["image"] = url

                query = pictures.select().where(
                    pictures.c.owner == user.id,
                    pictures.c.entity == "integrations",
                    pictures.c.entity_id == integr["id"],
                    pictures.c.is_main == False,
                )
                imgs = await database.fetch_all(query)
                res_imgs = []
                for i in imgs:
                    url = "" if not i else i.url
                    res_imgs.append(url)
                integr_dict["images"] = res_imgs
                res_ints.append(integr_dict)

            return {"result": res_ints, "count": count.count_1}

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.patch("/{intg_id}/")
async def update_integration(intg_id: int, body: UpdateIntegration, token: str):
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    query = integrations.select(integrations.c.id == intg_id)
    integration = await database.fetch_one(query)

    if user:
        if user.status:
            if integration.owner == user.id:
                body = body.dict(exclude_unset=True)
                print(body)
                query = (
                    integrations.update()
                    .values(body)
                    .where(integrations.c.id == intg_id)
                )
                await database.execute(query)
                return body
            raise HTTPException(status_code=403, detail="Недостаточно прав")
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.post("/install/{intg_id}/")
async def install_app(intg_id: int, token: str):
    query = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    query = integrations.select(integrations.c.id == intg_id)
    integration = await database.fetch_one(query)
    if user:
        if user.status:
            url = (
                integration.url
                + f"?client_id={user.id}&client_secret={integration.client_secret}&code={integration.code}"
            )
            await ouath_response(url)
            return {"installed": True}
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")
