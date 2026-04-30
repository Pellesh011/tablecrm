from typing import Optional

from database.db import database, users_cboxes_relation
from fastapi import Header, HTTPException, Query, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select

security_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    token: Optional[str] = Query(
        None,
        description="User authentication token (alternative to Authorization header)",
        include_in_schema=False,
    ),
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_scheme),
):
    if not token:
        if credentials:
            token = credentials.credentials

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Token required. Provide token as query parameter (?token=...) or Authorization header (Bearer ...)",
        )

    query = select(
        [
            users_cboxes_relation.c.id,
            users_cboxes_relation.c.user,
            users_cboxes_relation.c.cashbox_id,
            users_cboxes_relation.c.token,
            users_cboxes_relation.c.status,
            users_cboxes_relation.c.is_owner,
        ]
    ).where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")

    if not user.status:
        raise HTTPException(status_code=403, detail="User inactive")

    return user


async def get_current_user_owner(
    token: Optional[str] = Query(
        None,
        description="User authentication token (alternative to Authorization header)",
        include_in_schema=False,
    ),
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_scheme),
):
    user = await get_current_user(token=token, credentials=credentials)

    if not user.is_owner:
        raise HTTPException(status_code=403, detail="Owner permissions required")

    return user


async def get_current_user_for_avito(
    token: Optional[str] = Query(
        None, description="User authentication token", include_in_schema=True
    ),
    authorization: Optional[str] = Header(
        None, description="Authorization header (Bearer token)", include_in_schema=False
    ),
):

    if not token:
        if authorization:
            token = authorization.replace("Bearer ", "").strip()

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Token required. Provide token as query parameter (?token=...) or Authorization header (Bearer ...)",
        )

    query = select(
        [
            users_cboxes_relation.c.id,
            users_cboxes_relation.c.user,
            users_cboxes_relation.c.cashbox_id,
            users_cboxes_relation.c.token,
            users_cboxes_relation.c.status,
            users_cboxes_relation.c.is_owner,
        ]
    ).where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid token")

    if not user.status:
        raise HTTPException(status_code=403, detail="User inactive")

    return user
