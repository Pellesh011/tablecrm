import json
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

import psutil
import redis.asyncio as aioredis
from database.db import database, events
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from sqlalchemy import desc, select

# ---------- Настройки авторизации ----------
router = APIRouter(prefix="/loggerinfo", tags=["observability"])
security = HTTPBasic()

OBSERVABILITY_USER = os.getenv("OBSERVABILITY_USER", "admin")
OBSERVABILITY_PASS = os.getenv(
    "OBSERVABILITY_PASS", "123321555999000222pass123321555999000222"
)


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_user = secrets.compare_digest(
        credentials.username.encode(), OBSERVABILITY_USER.encode()
    )
    correct_pass = secrets.compare_digest(
        credentials.password.encode(), OBSERVABILITY_PASS.encode()
    )
    if not (correct_user and correct_pass):
        raise HTTPException(
            status_code=401,
            detail="Неверные учётные данные",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


# ---------- Prometheus метрики ----------
REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "handler", "status"]
)
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["method", "handler"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)
REQUESTS_IN_PROGRESS = Gauge(
    "http_requests_inprogress",
    "HTTP requests currently in progress",
    ["method", "handler"],
)
DB_QUERY_DURATION = Histogram(
    "db_query_duration_seconds", "Database query duration", ["query_type"]
)
RABBITMQ_CONNECTION_COUNT = Gauge(
    "rabbitmq_connections_active", "Active RabbitMQ connections"
)


# ---------- Вспомогательные функции ----------
async def get_redis_client():
    """Возвращает асинхронного клиента Redis"""
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    return aioredis.from_url(redis_url, decode_responses=True)


async def check_redis():
    """Проверка доступности Redis"""
    try:
        r = await get_redis_client()
        await r.ping()
        await r.aclose()
        return True, "OK"
    except Exception as e:
        return False, str(e)


async def check_rabbitmq():
    """Проверка RabbitMQ через Management API"""
    try:
        import aiohttp

        url = f"http://{os.getenv('RABBITMQ_HOST', 'rabbitmq')}:15672/api/connections"
        auth = aiohttp.BasicAuth(
            login=os.getenv("RABBITMQ_USER", "guest"),
            password=os.getenv("RABBITMQ_PASS", "guest"),
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url, auth=auth, timeout=3) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    count = len(data)
                    RABBITMQ_CONNECTION_COUNT.set(count)
                    return True, f"{count} connections"
                return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


@router.get("/resource-history")
async def get_resource_history(
    minutes: int = Query(default=10, ge=1, le=60), _: str = Depends(verify_credentials)
):
    """История использования CPU/RAM/Disk за последние N минут (точки каждые 10 сек)"""
    # Здесь можно брать данные из Redis (сохраняемые отдельным сборщиком) или вычислять на лету
    # Для простоты возвращаем последние N минут с текущими значениями (заглушка)
    points = []
    now = datetime.utcnow()
    for i in range(minutes):
        ts = now - timedelta(minutes=i)
        points.append(
            {
                "time": ts.strftime("%H:%M"),
                "cpu": psutil.cpu_percent(interval=0.1),
                "memory": psutil.virtual_memory().percent,
                "disk": psutil.disk_usage("/").percent,
            }
        )
    points.reverse()
    return {"history": points}


@router.get("/top-processes")
async def get_top_processes(
    limit: int = Query(default=10, le=50), _: str = Depends(verify_credentials)
):
    """Топ процессов по CPU и памяти"""
    procs = []
    for p in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent"]):
        try:
            procs.append(p.info)
        except:
            pass
    procs.sort(key=lambda x: x["cpu_percent"] + x["memory_percent"], reverse=True)
    return {"processes": procs[:limit]}


@router.get("/status")
async def system_status(_: str = Depends(verify_credentials)):
    """Общий статус системы (JSON)"""
    # Системные метрики
    mem = psutil.virtual_memory()
    cpu_percent = psutil.cpu_percent(interval=0.2)
    disk = psutil.disk_usage("/")
    boot_time = datetime.fromtimestamp(psutil.boot_time()).isoformat()

    # Статусы БД и сервисов
    db_stats = await database.fetch_all(
        """
        SELECT state, count(*) as cnt
        FROM pg_stat_activity
        GROUP BY state
        """
    )
    slow_queries_count = await database.fetch_val(
        """
        SELECT count(*)
        FROM pg_stat_activity
        WHERE state = 'active'
          AND now() - query_start > interval '2 seconds'
          AND query NOT LIKE '%pg_stat_activity%'
        """
    )

    redis_ok, redis_status = await check_redis()
    rabbit_ok, rabbit_status = await check_rabbitmq()

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "system": {
            "cpu_percent": cpu_percent,
            "memory": {
                "total_gb": round(mem.total / 1024**3, 2),
                "used_gb": round(mem.used / 1024**3, 2),
                "percent": mem.percent,
            },
            "disk": {
                "total_gb": round(disk.total / 1024**3, 2),
                "used_gb": round(disk.used / 1024**3, 2),
                "percent": disk.percent,
            },
            "boot_time": boot_time,
        },
        "postgres": {
            "connections": {row["state"]: row["cnt"] for row in db_stats},
            "slow_queries_active": slow_queries_count,
        },
        "integrations": {
            "redis": {"ok": redis_ok, "status": redis_status},
            "rabbitmq": {"ok": rabbit_ok, "status": rabbit_status},
            "prometheus": True,
        },
    }


@router.get("/metrics", include_in_schema=False)
async def metrics_endpoint(_: str = Depends(verify_credentials)):
    """Prometheus metrics endpoint (текст)"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/errors")
async def get_errors(
    hours: int = Query(
        default=1, ge=1, le=168, description="За сколько часов показать ошибки"
    ),
    limit: int = Query(default=500, le=2000),
    _: str = Depends(verify_credentials),
):
    """Список последних ошибок (5xx) в формате JSON"""
    since = datetime.now() - timedelta(hours=hours)
    query = (
        select(events)
        .where(
            events.c.status_code >= 500,
            events.c.created_at >= since,
        )
        .order_by(desc(events.c.created_at))
        .limit(limit)
    )
    rows = await database.fetch_all(query)
    result = []
    for r in rows:
        err = dict(r)
        # Приводим datetime к строке
        if err.get("created_at"):
            err["created_at"] = err["created_at"].isoformat()
        # Пытаемся извлечь traceback
        traceback_text = ""
        if err.get("payload") and isinstance(err["payload"], dict):
            traceback_text = (
                err["payload"].get("traceback", "")
                or err["payload"].get("error", "")
                or ""
            )
        err["traceback"] = traceback_text[:1000] if traceback_text else None
        result.append(err)

    return {"errors": result, "total": len(result), "hours": hours}


@router.get("/error/{error_id}")
async def get_error_detail(error_id: int, _: str = Depends(verify_credentials)):
    """Детальная информация об одной ошибке (JSON)"""
    row = await database.fetch_one(select(events).where(events.c.id == error_id))
    if not row:
        raise HTTPException(status_code=404, detail="Ошибка не найдена")
    err = dict(row)
    if err.get("created_at"):
        err["created_at"] = err["created_at"].isoformat()
    if err.get("payload") and isinstance(err["payload"], dict):
        err["traceback"] = err["payload"].get("traceback", "") or err["payload"].get(
            "error", ""
        )
    else:
        err["traceback"] = None
    return err


@router.get("/slow-queries")
async def get_slow_queries(
    threshold_sec: float = Query(default=2.0, ge=0.1, description="Порог в секундах"),
    _: str = Depends(verify_credentials),
):
    """Список активных медленных запросов (JSON)"""
    rows = await database.fetch_all(
        f"""
        SELECT pid,
               round(extract(epoch from (now() - query_start))::numeric, 2) as duration_sec,
               query,
               state,
               wait_event_type,
               wait_event,
               usename,
               application_name,
               backend_start,
               xact_start
        FROM pg_stat_activity
        WHERE state = 'active'
          AND now() - query_start > interval '{threshold_sec} seconds'
          AND query NOT LIKE '%pg_stat_activity%'
        ORDER BY duration_sec DESC
        LIMIT 100
        """
    )
    result = []
    for row in rows:
        r = dict(row)
        if r.get("backend_start"):
            r["backend_start"] = r["backend_start"].isoformat()
        if r.get("xact_start"):
            r["xact_start"] = r["xact_start"].isoformat()
        result.append(r)
    return {"slow_queries": result, "threshold_sec": threshold_sec}


@router.get("/tables")
async def tables_stats(
    limit: int = Query(default=20, le=50), _: str = Depends(verify_credentials)
):
    """Статистика по таблицам: размер, сканирования, индексы (JSON)"""
    rows = await database.fetch_all(
        """
        SELECT relname,
               n_live_tup as rows,
               seq_scan,
               idx_scan,
               pg_size_pretty(pg_total_relation_size(relid)) as total_size,
               pg_size_pretty(pg_indexes_size(relid)) as index_size
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(relid) DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )
    return {"tables": [dict(r) for r in rows]}


# ---------- Новые эндпоинты для статистики из Redis ----------


@router.get("/hourly-stats")
async def get_hourly_stats(
    hours: int = Query(
        default=24, ge=1, le=168, description="Количество последних часов"
    ),
    _: str = Depends(verify_credentials),
):
    """
    Почасовая статистика запросов, собранная HourlyStatsMiddleware.
    Возвращает массив данных по каждому часу.
    """
    r = await get_redis_client()
    result = []
    now = datetime.utcnow()
    for i in range(hours):
        dt = now - timedelta(hours=i)
        key = dt.strftime("hourly:%Y-%m-%d:%H")
        data = await r.hgetall(key)
        if data:
            result.append(
                {
                    "hour": dt.strftime("%Y-%m-%d %H:00"),
                    "requests": int(data.get("requests", 0)),
                    "total_ms": float(data.get("total_ms", 0)),
                    "avg_ms": round(
                        float(data.get("total_ms", 0))
                        / max(1, int(data.get("requests", 1))),
                        1,
                    ),
                    "errors_5xx": int(data.get("errors_5xx", 0)),
                    "errors_4xx": int(data.get("errors_4xx", 0)),
                    "slow_requests": int(data.get("slow_requests", 0)),
                }
            )
    await r.aclose()
    # Сортируем по возрастанию времени
    result.sort(key=lambda x: x["hour"])
    return {"hourly_stats": result}


@router.get("/slow-requests-log")
async def get_slow_requests_log(
    limit: int = Query(
        default=100, le=1000, description="Количество последних записей"
    ),
    _: str = Depends(verify_credentials),
):
    """Лог медленных запросов (>2 сек), сохранённых в Redis"""
    r = await get_redis_client()
    items = await r.lrange("slow_requests_log", 0, limit - 1)
    await r.aclose()
    log = [json.loads(item) for item in items]
    return {"slow_requests": log, "count": len(log)}


@router.get("/daily-report")
async def get_daily_report(
    date: Optional[str] = Query(
        default=None,
        description="Дата в формате YYYY-MM-DD. По умолчанию — последний доступный отчёт.",
    ),
    _: str = Depends(verify_credentials),
):
    """
    Ежедневный отчёт, собранный daily_report.py.
    Если дата не указана, возвращается последний отчёт из списка ключей.
    """
    r = await get_redis_client()
    if not date:
        # Получить последний ключ из списка daily_report_keys
        keys = await r.lrange("daily_report_keys", 0, 0)
        if not keys:
            await r.aclose()
            raise HTTPException(status_code=404, detail="Нет ни одного отчёта")
        key = keys[0]
    else:
        key = f"daily_report:{date}"

    report_json = await r.get(key)
    await r.aclose()
    if not report_json:
        raise HTTPException(
            status_code=404, detail=f"Отчёт за {date or 'последний день'} не найден"
        )

    return json.loads(report_json)


@router.post("/refresh-metrics", include_in_schema=False)
async def refresh_metrics(_: str = Depends(verify_credentials)):
    """Принудительно обновляет метрики (например, RabbitMQ)"""
    await check_rabbitmq()
    return {"status": "ok"}
