import secrets
from typing import Any, Dict, List, Optional

from database.db import (
    blog_folders,
    blog_post_tags,
    blog_posts,
    blog_site_folders,
    blog_sites,
    blog_tags,
    database,
    pictures,
)
from sqlalchemy import and_, delete, func, insert, or_, select, update


async def _get_post_tags(cashbox_id: int, post_id: int) -> List[str]:
    q = (
        select(blog_tags.c.slug)
        .select_from(
            blog_post_tags.join(blog_tags, blog_tags.c.id == blog_post_tags.c.tag_id)
        )
        .where(
            blog_post_tags.c.post_id == post_id, blog_tags.c.cashbox_id == cashbox_id
        )
        .order_by(blog_tags.c.slug.asc())
    )
    rows = await database.fetch_all(q)
    return [r["slug"] for r in rows]


async def _get_post_pictures(post_id: int) -> List[str]:
    q = (
        select(pictures.c.id, pictures.c.url)
        .where(
            pictures.c.entity == "blog_posts",
            pictures.c.entity_id == post_id,
            or_(pictures.c.is_deleted.is_(None), pictures.c.is_deleted.is_(False)),
        )
        .order_by(pictures.c.id.asc())
    )
    rows = await database.fetch_all(q)
    out = []
    for r in rows:
        url = r["url"]
        if not url:
            continue
        if url.startswith("http://") or url.startswith("https://"):
            out.append(url)
        else:
            # Добавляем /api/v1/ перед путём (предполагаем, что путь начинается с "photos/...")
            out.append(f"/api/v1/{url.lstrip('/')}")
    return out


async def _ensure_tags(cashbox_id: int, tag_slugs: List[str]) -> List[int]:
    """
    Ensure tags exist (by unique cashbox_id+slug), return tag_ids in same order.
    """
    tag_ids: List[int] = []
    for slug in tag_slugs:
        slug = (slug or "").strip()
        if not slug:
            continue

        existing = await database.fetch_one(
            select(blog_tags.c.id).where(
                and_(blog_tags.c.cashbox_id == cashbox_id, blog_tags.c.slug == slug)
            )
        )
        if existing:
            tag_ids.append(existing["id"])
            continue

        name = slug.replace("-", " ").strip()[:255]
        ins = (
            insert(blog_tags)
            .values(cashbox_id=cashbox_id, name=name, slug=slug)
            .returning(blog_tags.c.id)
        )
        row = await database.fetch_one(ins)
        tag_ids.append(row["id"])
    return tag_ids


async def create_site(cashbox_id: int, name: str) -> dict:
    token = secrets.token_urlsafe(32)
    q = (
        insert(blog_sites)
        .values(
            cashbox_id=cashbox_id,
            name=name,
            token=token,
            is_active=True,
        )
        .returning(
            blog_sites.c.id,
            blog_sites.c.cashbox_id,
            blog_sites.c.name,
            blog_sites.c.token,
            blog_sites.c.is_active,
        )
    )
    row = await database.fetch_one(q)
    return dict(row)


async def list_sites(cashbox_id: int) -> List[dict]:
    q = (
        select(
            blog_sites.c.id,
            blog_sites.c.cashbox_id,
            blog_sites.c.name,
            blog_sites.c.token,
            blog_sites.c.is_active,
        )
        .where(blog_sites.c.cashbox_id == cashbox_id)
        .order_by(blog_sites.c.id.desc())
    )
    rows = await database.fetch_all(q)
    return [dict(r) for r in rows]


async def get_site_by_token(site_token: str) -> Optional[dict]:
    q = select(blog_sites).where(
        blog_sites.c.token == site_token,
        blog_sites.c.is_active.is_(True),
    )
    row = await database.fetch_one(q)
    return dict(row) if row else None


async def get_site_by_id_and_cashbox(site_id: int, cashbox_id: int) -> Optional[dict]:
    q = select(blog_sites).where(
        blog_sites.c.id == site_id,
        blog_sites.c.cashbox_id == cashbox_id,
    )
    row = await database.fetch_one(q)
    return dict(row) if row else None


async def update_site(cashbox_id: int, site_id: int, payload: Dict[str, Any]) -> dict:
    q = (
        update(blog_sites)
        .where(blog_sites.c.id == site_id, blog_sites.c.cashbox_id == cashbox_id)
        .values(**payload)
        .returning(blog_sites)
    )
    row = await database.fetch_one(q)
    if not row:
        raise ValueError("Site not found")
    return dict(row)


async def delete_site(cashbox_id: int, site_id: int) -> None:
    site = await get_site_by_id_and_cashbox(site_id, cashbox_id)
    if not site:
        raise ValueError("Site not found")
    await database.execute(
        update(blog_sites)
        .where(blog_sites.c.id == site_id, blog_sites.c.cashbox_id == cashbox_id)
        .values(is_active=False)
    )


async def get_site_folder_ids(site_id: int) -> List[int]:
    q = select(blog_site_folders.c.folder_id).where(
        blog_site_folders.c.site_id == site_id
    )
    rows = await database.fetch_all(q)
    return [r["folder_id"] for r in rows]


async def set_site_folders(
    cashbox_id: int, site_id: int, folder_ids: List[int]
) -> None:
    # ensure site belongs to cashbox
    site = await database.fetch_one(
        select(blog_sites.c.id).where(
            blog_sites.c.id == site_id,
            blog_sites.c.cashbox_id == cashbox_id,
        )
    )
    if not site:
        raise ValueError("Site not found")

    # replace-all
    await database.execute(
        delete(blog_site_folders).where(blog_site_folders.c.site_id == site_id)
    )

    folder_ids = list(dict.fromkeys(folder_ids))  # unique preserve order
    if not folder_ids:
        return

    values = [{"site_id": site_id, "folder_id": fid} for fid in folder_ids]
    await database.execute_many(insert(blog_site_folders), values)


async def get_site_allowed_paths(site_id: int) -> List[str]:
    q = (
        select(blog_folders.c.path)
        .select_from(
            blog_site_folders.join(
                blog_folders, blog_folders.c.id == blog_site_folders.c.folder_id
            )
        )
        .where(
            blog_site_folders.c.site_id == site_id, blog_folders.c.is_active.is_(True)
        )
    )
    rows = await database.fetch_all(q)
    return [r["path"] for r in rows if r["path"]]


async def create_folder(
    cashbox_id: int,
    name: str,
    slug: str,
    parent_id: Optional[int],
    settings: Optional[Dict[str, Any]],
) -> dict:
    # insert with temporary path
    q = (
        insert(blog_folders)
        .values(
            cashbox_id=cashbox_id,
            name=name,
            slug=slug,
            parent_id=parent_id,
            path="TMP",
            settings=settings,
            is_active=True,
        )
        .returning(blog_folders.c.id)
    )
    row = await database.fetch_one(q)
    folder_id = row["id"]

    if parent_id:
        parent = await database.fetch_one(
            select(blog_folders.c.path).where(
                and_(
                    blog_folders.c.id == parent_id,
                    blog_folders.c.cashbox_id == cashbox_id,
                )
            )
        )
        if not parent:
            raise ValueError("Parent folder not found")
        path = f"{parent['path']}{folder_id}/"
    else:
        path = f"/{folder_id}/"

    await database.execute(
        update(blog_folders).where(blog_folders.c.id == folder_id).values(path=path)
    )

    folder = await database.fetch_one(
        select(blog_folders).where(blog_folders.c.id == folder_id)
    )
    return dict(folder)


async def list_folders(cashbox_id: int) -> List[dict]:
    q = (
        select(blog_folders)
        .where(blog_folders.c.cashbox_id == cashbox_id)
        .order_by(blog_folders.c.path.asc())
    )
    rows = await database.fetch_all(q)
    return [dict(r) for r in rows]


async def update_folder(
    cashbox_id: int,
    folder_id: int,
    payload: Dict[str, Any],
) -> dict:
    q = (
        update(blog_folders)
        .where(blog_folders.c.id == folder_id, blog_folders.c.cashbox_id == cashbox_id)
        .values(**payload)
        .returning(blog_folders)
    )
    row = await database.fetch_one(q)
    if not row:
        raise ValueError("Folder not found")
    return dict(row)


async def delete_folder(cashbox_id: int, folder_id: int) -> None:
    await database.execute(
        update(blog_folders)
        .where(blog_folders.c.id == folder_id, blog_folders.c.cashbox_id == cashbox_id)
        .values(is_active=False)
    )


async def create_post(cashbox_id: int, payload: Dict[str, Any]) -> dict:
    # Если статус PUBLISHED и не указана дата публикации, ставим текущее время
    if payload.get("status") == "PUBLISHED" and not payload.get("published_at"):
        payload["published_at"] = func.now()

    q = (
        insert(blog_posts)
        .values(
            cashbox_id=cashbox_id,
            folder_id=payload["folder_id"],
            title=payload["title"],
            slug=payload["slug"],
            excerpt=payload.get("excerpt"),
            content=payload["content"],
            status=payload.get("status") or "DRAFT",
            published_at=payload.get("published_at"),
            meta=payload.get("meta"),
            is_deleted=False,
        )
        .returning(blog_posts)
    )
    post_row = await database.fetch_one(q)
    if not post_row:
        raise ValueError("Cannot create post")

    post = dict(post_row)

    # tags
    tag_slugs = payload.get("tag_slugs") or []
    if tag_slugs:
        tag_ids = await _ensure_tags(cashbox_id, tag_slugs)
        if tag_ids:
            values = [{"post_id": post["id"], "tag_id": tid} for tid in tag_ids]
            await database.execute(
                delete(blog_post_tags).where(blog_post_tags.c.post_id == post["id"])
            )
            await database.execute_many(insert(blog_post_tags), values)

    post["tags"] = await _get_post_tags(cashbox_id, post["id"])
    post["pictures"] = await _get_post_pictures(post["id"])
    return post


async def get_post_admin(cashbox_id: int, post_id: int) -> Optional[dict]:
    q = select(blog_posts).where(
        blog_posts.c.id == post_id,
        blog_posts.c.cashbox_id == cashbox_id,
    )
    row = await database.fetch_one(q)
    if not row:
        return None
    post = dict(row)
    post["tags"] = await _get_post_tags(cashbox_id, post_id)
    post["pictures"] = await _get_post_pictures(post_id)
    return post


async def list_posts_admin(
    cashbox_id: int,
    folder_id: Optional[int] = None,
    status: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
) -> List[dict]:
    conds = [blog_posts.c.cashbox_id == cashbox_id]
    if folder_id is not None:
        conds.append(blog_posts.c.folder_id == folder_id)
    if status:
        conds.append(blog_posts.c.status == status)

    q = (
        select(blog_posts)
        .where(and_(*conds))
        .order_by(blog_posts.c.id.desc())
        .offset(skip)
        .limit(limit)
    )
    rows = await database.fetch_all(q)
    out: List[dict] = []
    for r in rows:
        p = dict(r)
        p["tags"] = await _get_post_tags(cashbox_id, p["id"])
        p["pictures"] = await _get_post_pictures(p["id"])
        out.append(p)
    return out


async def update_post(cashbox_id: int, post_id: int, payload: Dict[str, Any]) -> dict:
    tag_slugs = payload.pop("tag_slugs", None)

    q = (
        update(blog_posts)
        .where(
            blog_posts.c.id == post_id,
            blog_posts.c.cashbox_id == cashbox_id,
        )
        .values(**payload)
        .returning(blog_posts)
    )
    row = await database.fetch_one(q)
    if not row:
        raise ValueError("Post not found")

    if tag_slugs is not None:
        await database.execute(
            delete(blog_post_tags).where(blog_post_tags.c.post_id == post_id)
        )
        tag_ids = await _ensure_tags(cashbox_id, tag_slugs)
        if tag_ids:
            values = [{"post_id": post_id, "tag_id": tid} for tid in tag_ids]
            await database.execute_many(insert(blog_post_tags), values)

    post = dict(row)
    post["tags"] = await _get_post_tags(cashbox_id, post_id)
    post["pictures"] = await _get_post_pictures(post_id)
    return post


async def delete_post(cashbox_id: int, post_id: int) -> None:
    await database.execute(
        update(blog_posts)
        .where(blog_posts.c.id == post_id, blog_posts.c.cashbox_id == cashbox_id)
        .values(is_deleted=True)
    )


async def list_posts_public(
    site_id: int,
    cashbox_id: int,
    folder_id: Optional[int] = None,
    status: Optional[str] = "PUBLISHED",
    skip: int = 0,
    limit: int = 50,
) -> List[dict]:
    allowed_paths = await get_site_allowed_paths(site_id)
    if not allowed_paths:
        return []

    access_conds = [blog_folders.c.path.like(f"{p}%") for p in allowed_paths]

    base = (
        select(blog_posts)
        .select_from(
            blog_posts.join(blog_folders, blog_folders.c.id == blog_posts.c.folder_id)
        )
        .where(
            blog_posts.c.cashbox_id == cashbox_id,
            blog_posts.c.is_deleted.is_(False),
            blog_posts.c.status == status,
            blog_posts.c.published_at.isnot(None),
            blog_posts.c.published_at <= func.now(),
            or_(*access_conds),
        )
    )

    if folder_id is not None:
        frow = await database.fetch_one(
            select(blog_folders.c.path).where(blog_folders.c.id == folder_id)
        )
        if not frow:
            return []
        req_path = frow["path"]
        base = base.where(blog_folders.c.path.like(f"{req_path}%"))

    q = base.order_by(blog_posts.c.published_at.desc()).offset(skip).limit(limit)
    rows = await database.fetch_all(q)

    out: List[dict] = []
    for r in rows:
        p = dict(r)
        p["tags"] = await _get_post_tags(cashbox_id, p["id"])
        p["pictures"] = await _get_post_pictures(p["id"])
        out.append(p)
    return out


async def get_post_public(site_id: int, cashbox_id: int, slug: str) -> Optional[dict]:
    allowed_paths = await get_site_allowed_paths(site_id)
    if not allowed_paths:
        return None

    access_conds = [blog_folders.c.path.like(f"{p}%") for p in allowed_paths]

    q = (
        select(blog_posts)
        .select_from(
            blog_posts.join(blog_folders, blog_folders.c.id == blog_posts.c.folder_id)
        )
        .where(
            blog_posts.c.cashbox_id == cashbox_id,
            blog_posts.c.slug == slug,
            blog_posts.c.is_deleted.is_(False),
            blog_posts.c.status == "PUBLISHED",
            blog_posts.c.published_at.isnot(None),
            blog_posts.c.published_at <= func.now(),
            or_(*access_conds),
        )
        .limit(1)
    )
    row = await database.fetch_one(q)
    if not row:
        return None

    post = dict(row)
    post["tags"] = await _get_post_tags(cashbox_id, post["id"])
    post["pictures"] = await _get_post_pictures(post["id"])
    return post
