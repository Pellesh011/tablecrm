from database.db import database, integrations
from fastapi import APIRouter, Depends, HTTPException, Request
from jose import JWTError

from .shemas import OAuthCustomRequestForm
from .utils import (
    create_access_token,
    create_refresh_token,
    decode_jwt,
    get_jwt_groups,
    parse_openapi_json,
)

router = APIRouter(prefix="/oauth", tags=["oauth"])


@router.post(path="/token")
async def login(form_data: OAuthCustomRequestForm = Depends()):
    query = integrations.select().where(
        integrations.c.client_secret == form_data.client_secret,
        integrations.c.code == form_data.custom_token,
        integrations.c.owner == form_data.client_id,
    )
    integration = await database.fetch_one(query)
    if integration:
        scopes = integration.scopes.split()
        jwt_data = {"scopes": scopes, "client_id": form_data.client_id}
        return {
            "access_token": create_access_token(jwt_data),
            "refresh_token": create_refresh_token(jwt_data),
        }
    raise HTTPException(status_code=403, detail="Неккоректные данные")


@router.post("/token/refresh/")
async def refresh_response(refresh_token: str, client_id: int):
    try:
        payload = decode_jwt(refresh_token)
        if payload["client_id"] == client_id:
            return {
                "access_token": create_access_token({}),
                "refresh_token": create_refresh_token({}),
                "scopes": payload["scopes"],
            }
    except JWTError:
        raise HTTPException(status_code=403, detail="Неккоректные данные")


@router.get("/jwt_scopes/")
async def get_jwt_scopes(request: Request):
    res = parse_openapi_json(request.app.routes)
    return res


@router.get("/jwt_groups/")
async def get_jwt_scopes(request: Request):
    res = get_jwt_groups(request.app.routes)
    return res
