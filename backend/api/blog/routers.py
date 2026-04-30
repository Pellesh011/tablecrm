from typing import Optional

from database.db import (
    blog_folders,
    blog_posts,
    database,
    users,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import and_, select

from . import crud, schemas

# ВАЖНО: main.py уже имеет root_path="/api/v1"
# поэтому тут НЕ пишем /api/v1 в prefix.
router = APIRouter(prefix="/blog", tags=["blog"])
public_router = APIRouter(prefix="/public/blog", tags=["public_blog"])


@router.get("/health")
async def blog_health():
    return {"ok": True}


async def get_cashbox_id_by_token(token: str) -> int:
    """Resolve admin token -> cashbox_id.

    In this project:
      - users == tg_accounts
      - users_cboxes_relation == relation_tg_cashboxes (columns: token, status, user, cashbox_id)
    """
    q = (
        select(users_cboxes_relation.c.cashbox_id)
        .select_from(
            users_cboxes_relation.join(
                users, users.c.id == users_cboxes_relation.c.user
            )
        )
        .where(
            users_cboxes_relation.c.token == token,
            users_cboxes_relation.c.status.is_(True),
            # don't allow blocked accounts
            users.c.is_blocked.is_(False),
        )
        .limit(1)
    )
    row = await database.fetch_one(q)
    if not row:
        raise HTTPException(status_code=403, detail="Invalid token")
    return int(row["cashbox_id"])


@router.post("/sites", response_model=schemas.BlogSiteOut)
async def create_site(payload: schemas.BlogSiteCreate, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    return await crud.create_site(cashbox_id, payload.name)


@router.patch("/sites/{site_id}", response_model=schemas.BlogSiteOut)
async def update_site(
    site_id: int, payload: schemas.BlogSiteUpdate, token: str = Query(...)
):
    cashbox_id = await get_cashbox_id_by_token(token)
    data = payload.dict(exclude_unset=True)
    try:
        return await crud.update_site(cashbox_id, site_id, data)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/sites/{site_id}")
async def delete_site(site_id: int, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    try:
        await crud.delete_site(cashbox_id, site_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"ok": True}


@router.get("/sites", response_model=list[schemas.BlogSiteOut])
async def get_sites(token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    return await crud.list_sites(cashbox_id)


@router.put("/sites/{site_id}/folders")
async def set_site_folders(
    site_id: int, payload: schemas.BlogSiteFoldersSet, token: str = Query(...)
):
    cashbox_id = await get_cashbox_id_by_token(token)
    try:
        await crud.set_site_folders(cashbox_id, site_id, payload.folder_ids)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"ok": True}


@router.post("/folders", response_model=schemas.BlogFolderOut)
async def create_folder(payload: schemas.BlogFolderCreate, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)

    # Swagger often sends 0 for “no parent”; DB expects NULL, not 0.
    parent_id = payload.parent_id
    if parent_id == 0:
        parent_id = None

    try:
        return await crud.create_folder(
            cashbox_id=cashbox_id,
            name=payload.name,
            slug=payload.slug,
            parent_id=parent_id,
            settings=payload.settings,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/folders", response_model=list[schemas.BlogFolderOut])
async def list_folders(token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    return await crud.list_folders(cashbox_id)


@router.patch("/folders/{folder_id}", response_model=schemas.BlogFolderOut)
async def patch_folder(
    folder_id: int, payload: schemas.BlogFolderUpdate, token: str = Query(...)
):
    cashbox_id = await get_cashbox_id_by_token(token)
    data = payload.dict(exclude_unset=True)

    # Normalize 0 -> None to avoid FK violations.
    if data.get("parent_id") == 0:
        data["parent_id"] = None

    try:
        return await crud.update_folder(cashbox_id, folder_id, data)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/folders/{folder_id}")
async def delete_folder(folder_id: int, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    await crud.delete_folder(cashbox_id, folder_id)
    return {"ok": True}


@router.post("/posts", response_model=schemas.BlogPostOut)
async def create_post(payload: schemas.BlogPostCreate, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)

    # Guardrails to prevent DB integrity errors (which surface as 500 otherwise).
    if payload.folder_id <= 0:
        raise HTTPException(
            status_code=400, detail="folder_id must be a positive integer"
        )

    # folder must exist in this cashbox
    folder = await database.fetch_one(
        select(blog_folders.c.id).where(
            and_(
                blog_folders.c.id == payload.folder_id,
                blog_folders.c.cashbox_id == cashbox_id,
            )
        )
    )
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")

    # slug must be unique per cashbox
    existing = await database.fetch_one(
        select(blog_posts.c.id).where(
            and_(
                blog_posts.c.cashbox_id == cashbox_id, blog_posts.c.slug == payload.slug
            )
        )
    )
    if existing:
        raise HTTPException(status_code=400, detail="Post slug already exists")

    try:
        return await crud.create_post(cashbox_id, payload.dict())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        # Do not leak DB internals; most common reasons here are FK/unique violations.
        raise HTTPException(status_code=400, detail="Cannot create post")


@router.get("/posts", response_model=list[schemas.BlogPostOut])
async def list_posts(
    token: str = Query(...),
    folder_id: Optional[int] = None,
    status: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
):
    cashbox_id = await get_cashbox_id_by_token(token)
    return await crud.list_posts_admin(cashbox_id, folder_id, status, skip, limit)


@router.get("/posts/{post_id}", response_model=schemas.BlogPostOut)
async def get_post(post_id: int, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    post = await crud.get_post_admin(cashbox_id, post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.patch("/posts/{post_id}", response_model=schemas.BlogPostOut)
async def patch_post(
    post_id: int, payload: schemas.BlogPostUpdate, token: str = Query(...)
):
    cashbox_id = await get_cashbox_id_by_token(token)
    data = payload.dict(exclude_unset=True)

    # Normalize potential "folder_id": 0 misuse
    if data.get("folder_id") == 0:
        raise HTTPException(
            status_code=400, detail="folder_id must be a positive integer"
        )

    # If moving post to another folder, ensure folder belongs to cashbox.
    if "folder_id" in data and data["folder_id"] is not None:
        folder = await database.fetch_one(
            select(blog_folders.c.id).where(
                and_(
                    blog_folders.c.id == data["folder_id"],
                    blog_folders.c.cashbox_id == cashbox_id,
                )
            )
        )
        if not folder:
            raise HTTPException(status_code=404, detail="Folder not found")

    try:
        post = await crud.update_post(cashbox_id, post_id, data)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@router.delete("/posts/{post_id}")
async def delete_post(post_id: int, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    await crud.delete_post(cashbox_id, post_id)
    return {"ok": True}


# ---- Public API ----


@public_router.get("/sites/{site_token}")
async def get_site_public(site_token: str):
    site = await crud.get_site_by_token(site_token)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    return site


@router.get("/sites/{site_id}/folders", response_model=schemas.BlogSiteFoldersSet)
async def get_site_folders(site_id: int, token: str = Query(...)):
    cashbox_id = await get_cashbox_id_by_token(token)
    site = await crud.get_site_by_id_and_cashbox(site_id, cashbox_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    folder_ids = await crud.get_site_folder_ids(site_id)
    return schemas.BlogSiteFoldersSet(folder_ids=folder_ids)


@public_router.get(
    "/sites/{site_token}/folders", response_model=list[schemas.BlogFolderOut]
)
async def get_folders_public(site_token: str):
    site = await crud.get_site_by_token(site_token)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    return await crud.list_folders(site["cashbox_id"])


# api/blog/routers.py (фрагмент)
@public_router.get(
    "/sites/{site_token}/posts", response_model=list[schemas.BlogPostOut]
)
async def list_posts_public(
    site_token: str,
    folder_id: Optional[int] = None,
    status: Optional[str] = "PUBLISHED",
    skip: int = 0,
    limit: int = 50,
):
    site = await crud.get_site_by_token(site_token)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    return await crud.list_posts_public(
        site_id=site["id"],
        cashbox_id=site["cashbox_id"],
        folder_id=folder_id,
        status=status,
        skip=skip,
        limit=limit,
    )


@public_router.get(
    "/sites/{site_token}/posts/{slug}", response_model=schemas.BlogPostOut
)
async def get_post_public(site_token: str, slug: str):
    site = await crud.get_site_by_token(site_token)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    post = await crud.get_post_public(site["id"], site["cashbox_id"], slug)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post
