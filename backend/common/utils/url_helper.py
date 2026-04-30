"""
Утилита для получения правильного APP_URL в зависимости от окружения (dev/master)
"""

import os
from typing import Optional


def get_app_url_for_environment() -> Optional[str]:
    """
    Получает правильный APP_URL в зависимости от окружения.

    Приоритет:
    1. Если задана переменная ENV или ENVIRONMENT:
       - Если "dev" или "development" -> использует APP_URL_DEV (или APP_URL если не задан)
       - Если "master" или "production" -> использует APP_URL_MASTER (или APP_URL если не задан)
    2. Если переменные APP_URL_DEV или APP_URL_MASTER заданы напрямую, использует их
    3. По умолчанию использует APP_URL

    Returns:
        str: URL приложения для текущего окружения
    """
    env = (os.getenv("ENV") or os.getenv("ENVIRONMENT") or "").lower()

    # Определяем окружение
    is_dev = env in ("dev", "development", "staging")
    is_prod = env in ("master", "production", "prod")

    # Если окружение определено явно
    if is_dev:
        return os.getenv("APP_URL_DEV") or os.getenv("APP_URL")
    elif is_prod:
        return os.getenv("APP_URL_MASTER") or os.getenv("APP_URL")

    # Если переменные окружения заданы напрямую (для обратной совместимости)
    app_url_dev = os.getenv("APP_URL_DEV")
    app_url_master = os.getenv("APP_URL_MASTER")

    # Если заданы обе переменные, но окружение не определено - используем APP_URL
    # Если задана только одна - используем её
    if app_url_dev and not app_url_master:
        return app_url_dev
    if app_url_master and not app_url_dev:
        return app_url_master

    # По умолчанию используем APP_URL
    return os.getenv("APP_URL")


def get_chat_image_api_prefix() -> str:
    return os.getenv("CHAT_IMAGE_API_PREFIX", "/api/v1").strip()
