"""
Запускать: python daily_report.py
Или через cron: 0 9 * * * python backend/daily_report.py
"""

import json
import os
from datetime import datetime, timedelta

import psycopg2
import redis


def get_db_dsn():
    """Собрать DSN для PostgreSQL из переменных окружения."""
    if "DATABASE_URL" in os.environ:
        return os.environ["DATABASE_URL"]
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASS", "")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", user)
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def get_redis_url():
    """Собрать URL для Redis из переменных окружения."""
    if "REDIS_URL" in os.environ:
        return os.environ["REDIS_URL"]
    host = os.environ.get("REDIS_HOST", "redis")
    port = os.environ.get("REDIS_PORT", "6379")
    password = os.environ.get("REDIS_PASS", "")
    db = os.environ.get("REDIS_DB", "0")
    if password:
        return f"redis://:{password}@{host}:{port}/{db}"
    return f"redis://{host}:{port}/{db}"


def collect_daily_stats(date=None):
    if date is None:
        date = (datetime.utcnow() - timedelta(days=1)).date()

    DB_DSN = get_db_dsn()
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    # 1. Топ-100 медленных запросов к БД
    cur.execute(
        """
        SELECT LEFT(query,200), calls,
               ROUND(mean_exec_time::numeric,1) avg_ms,
               ROUND(total_exec_time::numeric/1000,1) total_sec
        FROM pg_stat_statements
        ORDER BY mean_exec_time DESC LIMIT 100
    """
    )
    slow_db = [
        {"q": r[0], "calls": r[1], "avg_ms": r[2], "total_s": r[3]}
        for r in cur.fetchall()
    ]

    # 2. Таблицы с seq scans (top 20)
    cur.execute(
        """
        SELECT relname, seq_scan, idx_scan, n_live_tup,
               pg_size_pretty(pg_total_relation_size(relid))
        FROM pg_stat_user_tables
        ORDER BY seq_scan DESC LIMIT 20
    """
    )
    seq_tables = cur.fetchall()

    # 3. Состояние соединений
    cur.execute(
        """
        SELECT state, count(*) FROM pg_stat_activity
        GROUP BY state
    """
    )
    conn_stats = dict(cur.fetchall())

    # 4. Bloat
    cur.execute(
        """
        SELECT relname, n_dead_tup, last_autovacuum
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 5000
        ORDER BY n_dead_tup DESC LIMIT 10
    """
    )
    bloat = cur.fetchall()

    cur.close()
    conn.close()
    return {
        "date": str(date),
        "slow_db_queries": slow_db,
        "seq_scan_tables": seq_tables,
        "connections": conn_stats,
        "table_bloat": bloat,
    }


def save_to_redis(stats):
    REDIS_URL = get_redis_url()
    r = redis.from_url(REDIS_URL)
    key = f"daily_report:{stats['date']}"
    r.set(key, json.dumps(stats, default=str), ex=86400 * 30)
    r.lpush("daily_report_keys", key)
    r.ltrim("daily_report_keys", 0, 29)


if __name__ == "__main__":
    stats = collect_daily_stats()
    save_to_redis(stats)
    print(f"Report saved for {stats['date']}")
