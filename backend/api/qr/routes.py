"""
api/qr/qr_routes.py

QR-Loyalty: генератор страниц, трекинг переходов/кликов, аналитика.
Подключить в main.py:
    from api.qr.qr_routes import router as qr_router
    app.include_router(qr_router)
"""

import secrets
import string
from datetime import datetime, timedelta

import sqlalchemy
from api.chats.auth import get_current_user_for_avito as get_current_user
from database.db import database, metadata
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    and_,
    desc,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY

router = APIRouter(prefix="/qr", tags=["qr-loyalty"])

# ─── Table definitions ────────────────────────────────────────────────────────

qr_pages = sqlalchemy.Table(
    "qr_pages",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    # «Назовите данный источник»
    sqlalchemy.Column("source_name", String(255), nullable=True),
    sqlalchemy.Column("name", String(255), nullable=False),
    sqlalchemy.Column("description", Text, nullable=True),
    # URL или inline SVG-строка
    sqlalchemy.Column("image_url", Text, nullable=True),
    sqlalchemy.Column("logo", Text, nullable=True),
    # [{type, label, short_code}]
    sqlalchemy.Column("buttons", JSON, nullable=True),
    # главный short_code страницы
    sqlalchemy.Column("short_code", String(50), unique=True, nullable=False),
    # UTM
    sqlalchemy.Column("utm_source", String(100), nullable=True),
    sqlalchemy.Column("utm_medium", String(100), nullable=True),
    sqlalchemy.Column("utm_campaign", String(100), nullable=True),
    # собирать ли UTM из адресной строки формы?
    sqlalchemy.Column(
        "collect_form_utm", Boolean, nullable=False, server_default=sqlalchemy.true()
    ),
    # автоматические теги
    sqlalchemy.Column("auto_tags", PG_ARRAY(String()), nullable=True),
    sqlalchemy.Column("status", String(20), nullable=False, server_default="active"),
    sqlalchemy.Column(
        "is_deleted", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now()),
    extend_existing=True,
)

qr_visits = sqlalchemy.Table(
    "qr_visits",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "page_id", Integer, ForeignKey("qr_pages.id"), nullable=False, index=True
    ),
    # через какой short_code зашли (кнопки имеют свои)
    sqlalchemy.Column("button_code", String(50), nullable=True),
    sqlalchemy.Column("ip", String(60), nullable=True),
    sqlalchemy.Column("user_agent", Text, nullable=True),
    sqlalchemy.Column("referer", String(500), nullable=True),
    # реальные utm из URL при переходе
    sqlalchemy.Column("utm_params", JSON, nullable=True),
    # Yandex Client ID (_ym_uid / yclid)
    sqlalchemy.Column("yandex_cid", String(100), nullable=True),
    # Google Client ID (_ga / gclid)
    sqlalchemy.Column("google_cid", String(100), nullable=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    extend_existing=True,
)

qr_targets = sqlalchemy.Table(
    "qr_targets",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("visit_id", Integer, ForeignKey("qr_visits.id"), nullable=False),
    sqlalchemy.Column(
        "page_id", Integer, ForeignKey("qr_pages.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("button_type", String(50), nullable=False),
    sqlalchemy.Column(
        "registered", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    extend_existing=True,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

_CHARS = string.ascii_lowercase + string.digits
_BTN_SUFFIX = {"Max": "m", "Telegram": "t", "SMS": "s"}
_DEFAULT_BUTTONS = [
    {"type": "Max", "label": "Через Max"},
    {"type": "Telegram", "label": "Telegram"},
    {"type": "SMS", "label": "Через SMS"},
]


def _rand(n: int = 8) -> str:
    return "".join(secrets.choice(_CHARS) for _ in range(n))


async def _unique_code(prefix: str = "") -> str:
    for _ in range(30):
        code = prefix + _rand(6 if prefix else 8)
        exists = await database.fetch_one(
            qr_pages.select().where(qr_pages.c.short_code == code)
        )
        if not exists:
            return code
    raise RuntimeError("Cannot generate unique short code")


async def _build_buttons(raw: list, page_code: str) -> list:
    result = []
    for btn in raw:
        sfx = _BTN_SUFFIX.get(btn.get("type", ""), "x")
        code = await _unique_code(f"{page_code}{sfx}")
        result.append({**btn, "short_code": code})
    return result


async def _stats(page_id: int) -> dict:
    v = (
        await database.fetch_val(
            select(func.count(qr_visits.c.id)).where(qr_visits.c.page_id == page_id)
        )
        or 0
    )
    r = (
        await database.fetch_val(
            select(func.count(qr_targets.c.id)).where(
                and_(qr_targets.c.page_id == page_id, qr_targets.c.registered.is_(True))
            )
        )
        or 0
    )
    c = (
        await database.fetch_val(
            select(func.count(qr_targets.c.id)).where(qr_targets.c.page_id == page_id)
        )
        or 0
    )
    return {
        "visits": v,
        "registrations": r,
        "clicks": c,
        "conversion": round((r / v * 100) if v else 0, 1),
    }


def _ip(request: Request) -> str:
    fwd = request.headers.get("X-Forwarded-For")
    return (
        fwd.split(",")[0].strip()
        if fwd
        else (request.client.host if request.client else "unknown")
    )


# ─── Endpoints ────────────────────────────────────────────────────────────────


@router.post("/pages")
async def create_page(request: Request, user=Depends(get_current_user)):
    body = await request.json()
    page_code = await _unique_code()
    raw_btns = body.get("buttons") or _DEFAULT_BUTTONS
    built_btns = await _build_buttons(raw_btns, page_code)

    page_id = await database.execute(
        qr_pages.insert().values(
            cashbox_id=user.cashbox_id,
            source_name=body.get("source_name"),
            name=body.get("name", "Без названия"),
            description=body.get("description"),
            image_url=body.get("image_url"),
            logo=body.get("logo"),
            buttons=built_btns,
            short_code=page_code,
            utm_source=body.get("utm_source", "qr_cashier"),
            utm_medium=body.get("utm_medium", "qr_physical"),
            utm_campaign=body.get("utm_campaign", "loyalty_card"),
            collect_form_utm=body.get("collect_form_utm", True),
            auto_tags=body.get("auto_tags") or [],
            status=body.get("status", "active"),
        )
    )
    page = await database.fetch_one(qr_pages.select().where(qr_pages.c.id == page_id))
    d = dict(page)
    d["stats"] = await _stats(page_id)
    return d


@router.get("/pages")
async def list_pages(user=Depends(get_current_user)):
    pages = await database.fetch_all(
        qr_pages.select()
        .where(
            and_(
                qr_pages.c.cashbox_id == user.cashbox_id,
                qr_pages.c.is_deleted.is_(False),
            )
        )
        .order_by(desc(qr_pages.c.created_at))
    )
    result = []
    for p in pages:
        d = dict(p)
        d["stats"] = await _stats(d["id"])
        result.append(d)
    return result


@router.get("/pages/{page_id}/stats")
async def page_stats(page_id: int, days: int = 7, user=Depends(get_current_user)):
    page = await database.fetch_one(
        qr_pages.select().where(
            and_(qr_pages.c.id == page_id, qr_pages.c.cashbox_id == user.cashbox_id)
        )
    )
    if not page:
        raise HTTPException(404, "Page not found")

    daily = []
    for i in range(days - 1, -1, -1):
        day = (datetime.utcnow() - timedelta(days=i)).date()
        d0 = datetime(day.year, day.month, day.day)
        d1 = d0 + timedelta(days=1)
        v = (
            await database.fetch_val(
                select(func.count(qr_visits.c.id)).where(
                    and_(
                        qr_visits.c.page_id == page_id,
                        qr_visits.c.created_at >= d0,
                        qr_visits.c.created_at < d1,
                    )
                )
            )
            or 0
        )
        r = (
            await database.fetch_val(
                select(func.count(qr_targets.c.id)).where(
                    and_(
                        qr_targets.c.page_id == page_id,
                        qr_targets.c.registered.is_(True),
                        qr_targets.c.created_at >= d0,
                        qr_targets.c.created_at < d1,
                    )
                )
            )
            or 0
        )
        c = (
            await database.fetch_val(
                select(func.count(qr_targets.c.id)).where(
                    and_(
                        qr_targets.c.page_id == page_id,
                        qr_targets.c.created_at >= d0,
                        qr_targets.c.created_at < d1,
                    )
                )
            )
            or 0
        )
        daily.append(
            {
                "date": day.strftime("%d.%m"),
                "visits": v,
                "registrations": r,
                "clicks": c,
                "conversion": round((r / v * 100) if v else 0, 1),
            }
        )

    buttons = await database.fetch_all(
        select(qr_targets.c.button_type, func.count(qr_targets.c.id).label("count"))
        .where(qr_targets.c.page_id == page_id)
        .group_by(qr_targets.c.button_type)
    )
    return {
        "page": dict(page),
        "daily": daily,
        "buttons": [dict(b) for b in buttons],
        "summary": await _stats(page_id),
    }


@router.get("/p/{short_code}")
async def page_public(short_code: str, request: Request):
    # --- 1. Поиск страницы (основной код или код кнопки) ---
    page = await database.fetch_one(
        qr_pages.select().where(
            and_(qr_pages.c.short_code == short_code, qr_pages.c.is_deleted.is_(False))
        )
    )
    button_type = None
    if not page:
        all_pages = await database.fetch_all(
            qr_pages.select().where(qr_pages.c.is_deleted.is_(False))
        )
        for p in all_pages:
            for btn in p["buttons"] or []:
                if btn.get("short_code") == short_code:
                    page = p
                    button_type = btn["type"]
                    break
            if page:
                break
    if not page:
        raise HTTPException(404, "Not found")

    # --- 2. Сбор UTM и создание визита ---
    qp = dict(request.query_params)
    utm_params = {k: v for k, v in qp.items() if k.startswith("utm_")}
    yandex_cid = qp.get("_ym_uid") or qp.get("yclid")
    google_cid = qp.get("_ga") or qp.get("gclid")
    try:
        body = await request.json()
    except Exception:
        body = {}
    yandex_cid = yandex_cid or body.get("yandex_cid")
    google_cid = google_cid or body.get("google_cid")

    visit_id = await database.execute(
        qr_visits.insert().values(
            page_id=page["id"],
            button_code=short_code if button_type else None,
            ip=_ip(request),
            user_agent=request.headers.get("User-Agent", ""),
            referer=request.headers.get("Referer", ""),
            utm_params=utm_params or None,
            yandex_cid=yandex_cid,
            google_cid=google_cid,
        )
    )

    # --- 3. Обогащение кнопок: добавляем start=visit_{visit_id} ---
    enriched_buttons = []
    for btn in page["buttons"]:
        btn_copy = dict(btn)
        if btn.get("bot_url") and btn["type"] in ("Telegram", "Max"):
            url = btn["bot_url"].rstrip("?&")
            sep = "&" if "?" in url else "?"
            btn_copy["bot_url"] = f"{url}{sep}start=visit_{visit_id}"
        enriched_buttons.append(btn_copy)

    # --- 4. Ответ ---
    return {
        "page": dict(page),
        "visit_id": visit_id,
        "buttons": enriched_buttons,
    }


@router.post("/visit/{short_code}")
async def track_visit(short_code: str, request: Request):
    """Public: фиксирует переход по QR."""
    result = await page_public(short_code)
    page = result["page"]
    button_type = result["button_type"]

    qp = dict(request.query_params)
    utm_params = {k: v for k, v in qp.items() if k.startswith("utm_")}
    yandex_cid = qp.get("_ym_uid") or qp.get("yclid")
    google_cid = qp.get("_ga") or qp.get("gclid")

    try:
        body = await request.json()
    except Exception:
        body = {}
    yandex_cid = yandex_cid or body.get("yandex_cid")
    google_cid = google_cid or body.get("google_cid")

    visit_id = await database.execute(
        qr_visits.insert().values(
            page_id=page["id"],
            button_code=short_code if button_type else None,
            ip=_ip(request),
            user_agent=request.headers.get("User-Agent", ""),
            referer=request.headers.get("Referer", ""),
            utm_params=utm_params or None,
            yandex_cid=yandex_cid,
            google_cid=google_cid,
        )
    )
    return {"visit_id": visit_id, "page_id": page["id"], "button_type": button_type}


@router.post("/target/{visit_id}")
async def track_target(visit_id: int, request: Request):
    """Public: фиксирует клик на кнопку (Max / Telegram / SMS)."""
    visit = await database.fetch_one(
        qr_visits.select().where(qr_visits.c.id == visit_id)
    )
    if not visit:
        raise HTTPException(404, "Visit not found")
    body = await request.json()
    tid = await database.execute(
        qr_targets.insert().values(
            visit_id=visit_id,
            page_id=visit["page_id"],
            button_type=body.get("button_type", "unknown"),
            registered=body.get("registered", True),
        )
    )
    return {"target_id": tid}


@router.get("/p/{short_code}")
async def page_public(short_code: str):
    """Public: отдаёт страницу по main-коду или коду кнопки."""
    page = await database.fetch_one(
        qr_pages.select().where(
            and_(qr_pages.c.short_code == short_code, qr_pages.c.is_deleted.is_(False))
        )
    )
    if page:
        return {"page": dict(page), "button_type": None}

    # ищем среди кодов кнопок
    all_pages = await database.fetch_all(
        qr_pages.select().where(qr_pages.c.is_deleted.is_(False))
    )
    for p in all_pages:
        for btn in p["buttons"] or []:
            if btn.get("short_code") == short_code:
                return {"page": dict(p), "button_type": btn["type"]}

    raise HTTPException(404, "Not found")


@router.post("/visit/{short_code}")
async def track_visit(short_code: str, request: Request):
    """Public: фиксирует переход по QR."""
    result = await page_public(short_code)
    page = result["page"]
    button_type = result["button_type"]

    qp = dict(request.query_params)
    utm_params = {k: v for k, v in qp.items() if k.startswith("utm_")}
    yandex_cid = qp.get("_ym_uid") or qp.get("yclid")
    google_cid = qp.get("_ga") or qp.get("gclid")

    try:
        body = await request.json()
    except Exception:
        body = {}
    yandex_cid = yandex_cid or body.get("yandex_cid")
    google_cid = google_cid or body.get("google_cid")

    visit_id = await database.execute(
        qr_visits.insert().values(
            page_id=page["id"],
            button_code=short_code if button_type else None,
            ip=_ip(request),
            user_agent=request.headers.get("User-Agent", ""),
            referer=request.headers.get("Referer", ""),
            utm_params=utm_params or None,
            yandex_cid=yandex_cid,
            google_cid=google_cid,
        )
    )
    return {"visit_id": visit_id, "page_id": page["id"], "button_type": button_type}


@router.post("/target/{visit_id}")
async def track_target(visit_id: int, request: Request):
    """Public: фиксирует клик на кнопку (Max / Telegram / SMS)."""
    visit = await database.fetch_one(
        qr_visits.select().where(qr_visits.c.id == visit_id)
    )
    if not visit:
        raise HTTPException(404, "Visit not found")
    body = await request.json()
    tid = await database.execute(
        qr_targets.insert().values(
            visit_id=visit_id,
            page_id=visit["page_id"],
            button_type=body.get("button_type", "unknown"),
            registered=body.get("registered", True),
        )
    )
    return {"target_id": tid}


@router.get("/visits/{page_id}")
async def get_visits(page_id: int, limit: int = 200, user=Depends(get_current_user)):
    page = await database.fetch_one(
        qr_pages.select().where(
            and_(qr_pages.c.id == page_id, qr_pages.c.cashbox_id == user.cashbox_id)
        )
    )
    if not page:
        raise HTTPException(404, "Access denied")

    visits = await database.fetch_all(
        qr_visits.select()
        .where(qr_visits.c.page_id == page_id)
        .order_by(desc(qr_visits.c.created_at))
        .limit(limit)
    )
    result = []
    for v in visits:
        vd = dict(v)
        targets = await database.fetch_all(
            qr_targets.select().where(qr_targets.c.visit_id == vd["id"])
        )
        vd["targets"] = [dict(t) for t in targets]
        vd["target_reached"] = any(t["registered"] for t in vd["targets"])
        vd["button_type"] = vd["targets"][0]["button_type"] if vd["targets"] else None
        result.append(vd)
    return result


@router.patch("/pages/{page_id}")
async def update_page(page_id: int, request: Request, user=Depends(get_current_user)):
    page = await database.fetch_one(
        qr_pages.select().where(
            and_(qr_pages.c.id == page_id, qr_pages.c.cashbox_id == user.cashbox_id)
        )
    )
    if not page:
        raise HTTPException(404, "Not found")

    body = await request.json()
    allowed = [
        "source_name",
        "name",
        "description",
        "image_url",
        "logo",
        "buttons",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "collect_form_utm",
        "auto_tags",
        "status",
    ]
    update_data = {k: v for k, v in body.items() if k in allowed}
    update_data["updated_at"] = datetime.utcnow()
    await database.execute(
        qr_pages.update().where(qr_pages.c.id == page_id).values(**update_data)
    )
    updated = await database.fetch_one(
        qr_pages.select().where(qr_pages.c.id == page_id)
    )
    d = dict(updated)
    d["stats"] = await _stats(page_id)
    return d


@router.delete("/pages/{page_id}")
async def delete_page(page_id: int, user=Depends(get_current_user)):
    page = await database.fetch_one(
        qr_pages.select().where(
            and_(qr_pages.c.id == page_id, qr_pages.c.cashbox_id == user.cashbox_id)
        )
    )
    if not page:
        raise HTTPException(404, "Not found")
    await database.execute(
        qr_pages.update().where(qr_pages.c.id == page_id).values(is_deleted=True)
    )
    return {"success": True}
