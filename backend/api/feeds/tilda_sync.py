"""
Модуль для синхронизации фидов с Tilda через CommerceML
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time

import aiohttp
import requests
from aiohttp import BasicAuth
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


def _normalize_tilda_base_url(tilda_url: str) -> str:
    """Приводит базовый URL коннектора Tilda CommerceML к виду без лишних редиректов.

    На практике важно, чтобы все запросы ходили на один и тот же URL (с завершающим `/`),
    иначе возможны 301/302 редиректы, которые иногда ломают cookies/авторизацию.
    """

    if not tilda_url:
        return ""
    return tilda_url.rstrip("/") + "/"


async def send_both_feeds_to_tilda(
    catalog_xml: str,
    offers_xml: str,
    tilda_url: str,
    username: str,
    password: str,
) -> dict:
    """
    Отправляет каталог и предложения в Tilda через CommerceML протокол.

    Args:
        catalog_xml: XML содержимое каталога товаров
        offers_xml: XML содержимое предложений (цены и остатки)
        tilda_url: Базовый URL для отправки
        username: Имя пользователя для Basic Auth
        password: Пароль для Basic Auth

    Returns:
        dict с результатами отправки обоих файлов
    """
    # Проверяем, что файлы не пустые
    if not catalog_xml or len(catalog_xml.strip()) == 0:
        return {
            "success": False,
            "catalog_result": {
                "success": False,
                "error": "Catalog XML is empty",
            },
            "offers_result": {
                "success": False,
                "error": "Catalog XML is empty, offers not sent",
            },
            "method": "separate",
        }

    if not offers_xml or len(offers_xml.strip()) == 0:
        return {
            "success": False,
            "catalog_result": {
                "success": False,
                "error": "Offers XML is empty, catalog not sent",
            },
            "offers_result": {
                "success": False,
                "error": "Offers XML is empty",
            },
            "method": "separate",
        }

    import urllib.parse

    logger.info(
        f"Starting Tilda sync: catalog_size={len(catalog_xml)}, offers_size={len(offers_xml)}"
    )

    # Проверяем, что XML не пустой и правильно сформирован
    if not catalog_xml or len(catalog_xml.strip()) == 0:
        logger.error("Catalog XML is empty!")
        return {
            "success": False,
            "catalog_result": {"success": False, "error": "Catalog XML is empty"},
            "offers_result": {
                "success": False,
                "error": "Catalog XML is empty, offers not sent",
            },
        }

    if not offers_xml or len(offers_xml.strip()) == 0:
        logger.error("Offers XML is empty!")
        return {
            "success": False,
            "catalog_result": {
                "success": False,
                "error": "Offers XML is empty, catalog not sent",
            },
            "offers_result": {"success": False, "error": "Offers XML is empty"},
        }

    # Кодируем XML до передачи в замыкание и сохраняем как локальные переменные
    # Это гарантирует, что данные правильно захватываются в замыкании
    catalog_xml_bytes_local = catalog_xml.encode("utf-8")
    offers_xml_bytes_local = offers_xml.encode("utf-8")

    logger.info(
        f"Encoded catalog: {len(catalog_xml_bytes_local)} bytes, offers: {len(offers_xml_bytes_local)} bytes"
    )
    logger.info(
        f"Catalog XML type: {type(catalog_xml)}, bytes type: {type(catalog_xml_bytes_local)}"
    )
    logger.debug(f"Catalog XML first 200 bytes: {catalog_xml_bytes_local[:200]}")

    # Проверяем, что байты не пустые
    if len(catalog_xml_bytes_local) == 0:
        logger.error("Catalog XML bytes are empty after encoding!")
        return {
            "success": False,
            "catalog_result": {
                "success": False,
                "error": "Catalog XML bytes are empty after encoding",
            },
            "offers_result": {
                "success": False,
                "error": "Catalog XML bytes are empty, offers not sent",
            },
        }

    # Используем requests (синхронный) в async обертке
    # Это работает надежнее с Tilda API, чем aiohttp
    # Передаем все параметры явно, чтобы избежать проблем с замыканием
    def _sync_send(
        catalog_bytes, offers_bytes, tilda_url_param, username_param, password_param
    ):
        session = requests.Session()
        session.auth = HTTPBasicAuth(username_param, password_param)

        tilda_base_url_local = _normalize_tilda_base_url(tilda_url_param)

        logger.info(
            f"_sync_send called with catalog_bytes: {len(catalog_bytes)} bytes, offers_bytes: {len(offers_bytes)} bytes"
        )

        try:

            def _build_url(params: dict) -> str:
                # filename может содержать '/', поэтому строим query вручную.
                parts = []
                for k, v in params.items():
                    if v is None:
                        continue
                    parts.append(
                        f"{urllib.parse.quote(str(k))}={urllib.parse.quote(str(v), safe='/._-')}"
                    )
                return f"{tilda_base_url_local}?{'&'.join(parts)}"

            def _optimize_image_for_tilda(body: bytes, ext: str) -> bytes:
                """Приводит картинку под ограничения Tilda: <=1680px и <3MB.

                Делается синхронно, т.к. обмен с Tilda идёт в sync потоке.
                """

                ext = (ext or "").lower().lstrip(".")
                if ext not in ("jpg", "jpeg", "png"):
                    return body

                # Лёгкий fast-path
                if len(body) <= 3 * 1024 * 1024:
                    try:
                        import io

                        from PIL import Image

                        img = Image.open(io.BytesIO(body))
                        width, height = img.size
                        if max(width, height) <= 1680:
                            return body
                    except Exception:
                        return body

                try:
                    import io

                    from PIL import Image

                    img = Image.open(io.BytesIO(body))
                    width, height = img.size
                    max_side = max(width, height)

                    if max_side > 1680:
                        ratio = 1680 / max_side
                        new_size = (int(width * ratio), int(height * ratio))
                        img = img.resize(
                            new_size, getattr(Image, "Resampling", Image).LANCZOS
                        )

                    # Сохраняем
                    out = io.BytesIO()

                    if ext in ("jpg", "jpeg"):
                        if img.mode in ("RGBA", "P"):
                            img = img.convert("RGB")
                        img.save(out, format="JPEG", optimize=True, quality=85)
                        processed = out.getvalue()

                        # Если всё ещё >3MB — ужимаем сильнее
                        if len(processed) > 3 * 1024 * 1024:
                            out = io.BytesIO()
                            img.save(out, format="JPEG", optimize=True, quality=70)
                            processed = out.getvalue()
                        return processed

                    # PNG
                    img.save(out, format="PNG", optimize=True)
                    processed = out.getvalue()

                    # Если PNG всё ещё >3MB — fallback в JPEG
                    if len(processed) > 3 * 1024 * 1024:
                        if img.mode in ("RGBA", "P"):
                            img = img.convert("RGB")
                        out = io.BytesIO()
                        img.save(out, format="JPEG", optimize=True, quality=80)
                        return out.getvalue()

                    return processed
                except Exception:
                    return body

            def _extract_picture_urls(catalog_xml_text: str) -> list[str]:
                # CommerceML внутри — без namespaces, поэтому regex достаточно стабилен.
                raw = re.findall(r"<Картинка>(.*?)</Картинка>", catalog_xml_text)
                urls: list[str] = []
                for value in raw:
                    value = (value or "").strip()
                    if not value:
                        continue
                    # Часто в XML могут быть экранированы символы
                    value = (
                        value.replace("&amp;", "&")
                        .replace("&lt;", "<")
                        .replace("&gt;", ">")
                        .replace("&quot;", '"')
                        .replace("&apos;", "'")
                    )
                    urls.append(value)
                # Дедуп
                return list(dict.fromkeys(urls))

            def _url_to_import_filename(url: str) -> tuple[str, str] | None:
                """Возвращает (import_filename, ext) для `<Картинка>`.

                Используем стабильное имя без директорий, чтобы query encoding
                и обработка на стороне Tilda были предсказуемыми.
                """

                try:
                    parsed = urllib.parse.urlparse(url)
                    path = parsed.path or ""
                except Exception:
                    return None

                base = os.path.basename(path)
                if "." in base:
                    _, ext = os.path.splitext(base)
                else:
                    ext = ".jpg"
                ext = ext.lower()

                # Хешируем URL чтобы избежать коллизий и проблем с символами
                import hashlib

                digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:20]
                safe_ext = ext if ext in (".jpg", ".jpeg", ".png", ".gif") else ".jpg"
                filename = f"import_files/{digest}{safe_ext}"
                return filename, safe_ext.lstrip(".")

            def _download_image_bytes(url: str) -> bytes | None:
                """Получает байты картинки для загрузки в Tilda.

                1) Если URL указывает на наши `/api/v1/photos*` — читаем напрямую из S3,
                   чтобы не зависеть от внешнего DNS/HTTPS внутри контейнера.
                2) Иначе пробуем HTTP GET как fallback.
                """

                try:
                    parsed = urllib.parse.urlparse(url)
                    path = parsed.path or ""

                    marker_variants = (
                        "/api/v1/photos-tilda/",
                        "/api/v1/photos/",
                        "/photos-tilda/",
                        "/photos/",
                    )
                    rest: str | None = None
                    for marker in marker_variants:
                        if marker in path:
                            rest = path.split(marker, 1)[1]
                            break

                    # Если это не URL нашего API (например, в XML лежит S3 key
                    # вроде `photos/2025/...jpg` или просто `2025/...jpg`),
                    # тоже попробуем прочитать напрямую из S3.
                    if rest is None:
                        candidate = path.lstrip("/")
                        if candidate:
                            rest = candidate

                    if rest is not None:
                        rest = rest.lstrip("/")
                        # Поддержка случаев, когда в XML лежит уже API-путь без домена
                        # (например `api/v1/photos/...`).
                        if rest.startswith("api/v1/photos/"):
                            rest = rest.split("api/v1/photos/", 1)[1]
                        elif rest.startswith("api/v1/"):
                            rest = rest.split("api/v1/", 1)[1]

                        if rest.startswith("photos/"):
                            s3_key = rest
                        else:
                            s3_key = f"photos/{rest}"

                        import boto3

                        s3 = boto3.client(
                            "s3",
                            endpoint_url=os.environ.get("S3_URL"),
                            aws_access_key_id=os.environ.get("S3_ACCESS"),
                            aws_secret_access_key=os.environ.get("S3_SECRET"),
                        )
                        obj = s3.get_object(
                            Bucket="5075293c-docs_generated", Key=s3_key
                        )
                        return obj["Body"].read()
                except Exception as e:
                    logger.warning(
                        f"S3 download failed for image upload: url={url}, err={e}"
                    )

                try:
                    resp = requests.get(url, timeout=60)
                    if 200 <= resp.status_code < 300:
                        return resp.content
                    logger.warning(
                        f"HTTP download failed for image upload: status={resp.status_code}, url={url}"
                    )
                except Exception as e:
                    logger.warning(
                        f"HTTP download failed for image upload: url={url}, err={e}"
                    )
                return None

            def _upload_file(filename: str, content: bytes, label: str) -> dict:
                file_url = _build_url(
                    {"type": "catalog", "mode": "file", "filename": filename}
                )
                resp = session.post(
                    file_url,
                    data=content,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=120,
                )
                text = (resp.text or "").strip()
                logger.info(
                    f"Tilda upload {label}: status={resp.status_code}, response={text[:200]}, filename={filename}"
                )
                return {"status": resp.status_code, "response": text}

            def _run_import_until_done(import_url: str, label: str) -> dict:
                """Повторяет CommerceML mode=import до завершения.

                Tilda может отвечать 'progress' — это означает, что импорт идёт
                и клиент должен повторять вызов import, пока не придёт 'success'
                или не истечёт таймаут.
                """

                deadline = time.monotonic() + 180  # общий таймаут на один import
                attempts = 0
                last_text = ""
                last_status = 0

                while True:
                    attempts += 1
                    resp = session.post(
                        import_url,
                        data="",
                        headers={"Content-Type": "text/xml; charset=utf-8"},
                        timeout=60,
                    )
                    last_status = resp.status_code
                    last_text = (resp.text or "").strip()

                    logger.info(
                        f"Tilda {label} import attempt={attempts}: status={last_status}, response={last_text[:200]}"
                    )

                    # HTTP-level fail
                    if not (200 <= last_status < 300):
                        return {
                            "success": False,
                            "status": last_status,
                            "response": last_text,
                            "attempts": attempts,
                            "error": f"Import HTTP status {last_status}: {last_text}",
                        }

                    # protocol-level status
                    if last_text.startswith("success"):
                        return {
                            "success": True,
                            "status": last_status,
                            "response": last_text,
                            "attempts": attempts,
                        }

                    if last_text.startswith("progress"):
                        if time.monotonic() >= deadline:
                            return {
                                "success": False,
                                "status": last_status,
                                "response": last_text,
                                "attempts": attempts,
                                "error": "Import did not finish in time (still progress)",
                            }
                        time.sleep(2)
                        continue

                    return {
                        "success": False,
                        "status": last_status,
                        "response": last_text,
                        "attempts": attempts,
                        "error": f"Unexpected import response: {last_text}",
                    }

            # Шаг 1: checkauth
            checkauth_params = {"type": "catalog", "mode": "checkauth"}
            checkauth_url = _build_url(checkauth_params)
            checkauth_response = session.get(checkauth_url, timeout=30)
            checkauth_text = checkauth_response.text
            logger.info(f"Tilda checkauth result: {checkauth_text[:200]}")
            logger.info(f"Session cookies after checkauth: {dict(session.cookies)}")

            if not checkauth_text.startswith("success"):
                return {
                    "success": False,
                    "catalog_result": {
                        "success": False,
                        "error": f"Checkauth failed: {checkauth_text}",
                    },
                    "offers_result": {
                        "success": False,
                        "error": "Checkauth failed, offers not sent",
                    },
                }

            # Шаг 2: init
            init_params = {"type": "catalog", "mode": "init"}
            init_url = _build_url(init_params)
            init_response = session.get(init_url, timeout=30)
            init_text = init_response.text
            logger.info(f"Tilda init result: {init_text[:100]}")
            logger.info(f"Session cookies after init: {dict(session.cookies)}")

            # Шаг 3: загрузка картинок в Tilda и переписывание ссылок в XML
            # (если Tilda не ходит по HTTP за картинками, этот путь обязателен)
            try:
                catalog_xml_text = catalog_bytes.decode("utf-8", errors="replace")
                picture_urls = _extract_picture_urls(catalog_xml_text)

                url_to_filename: dict[str, str] = {}
                url_to_ext: dict[str, str] = {}

                for url in picture_urls:
                    converted = _url_to_import_filename(url)
                    if not converted:
                        continue
                    filename, ext = converted
                    url_to_filename[url] = filename
                    url_to_ext[url] = ext

                if url_to_filename:
                    logger.info(
                        f"Tilda: found {len(url_to_filename)} images in catalog; uploading via CommerceML file"
                    )

                # Загружаем файлы. Переписываем XML только для реально загруженных картинок,
                # чтобы не оставлять в каталоге "битые" import_files пути.
                uploaded_url_to_filename: dict[str, str] = {}
                for url, import_filename in url_to_filename.items():
                    body = _download_image_bytes(url)
                    if not body:
                        logger.warning(
                            f"Tilda image skipped: download failed, url={url}"
                        )
                        continue
                    ext = url_to_ext.get(url, "jpg")
                    body = _optimize_image_for_tilda(body, ext)
                    upload_result = _upload_file(import_filename, body, label="image")
                    status = int(upload_result.get("status") or 0)
                    response_text = str(upload_result.get("response") or "")
                    if 200 <= status < 300 and response_text.startswith("success"):
                        uploaded_url_to_filename[url] = import_filename
                    else:
                        logger.warning(
                            "Tilda image upload failed, keeping original URL in XML: "
                            f"url={url}, filename={import_filename}, status={status}, response={response_text[:200]}"
                        )

                # Переписываем XML: URL -> import_files/... (с теми именами, которые загрузили)
                if uploaded_url_to_filename:
                    for url, import_filename in uploaded_url_to_filename.items():
                        catalog_xml_text = catalog_xml_text.replace(
                            url, import_filename
                        )
                    catalog_bytes = catalog_xml_text.encode("utf-8")
            except Exception as e:
                logger.warning(f"Tilda image upload step failed (continuing): {e}")

            # Шаг 4: отправка каталога товаров (XML)
            catalog_file_url = _build_url(
                {"type": "catalog", "mode": "file", "filename": "import0_1.xml"}
            )

            # Tilda CommerceML принимает XML напрямую в теле POST запроса с Content-Type: text/xml; charset=utf-8
            logger.info(f"Sending catalog file: {len(catalog_bytes)} bytes")

            # Убеждаемся, что это действительно байты
            if isinstance(catalog_bytes, str):
                catalog_bytes = catalog_bytes.encode("utf-8")
                logger.warning(
                    f"Catalog bytes were string, encoded to bytes: {len(catalog_bytes)} bytes"
                )

            catalog_file_response = session.post(
                catalog_file_url,
                data=catalog_bytes,
                headers={"Content-Type": "text/xml; charset=utf-8"},
                timeout=30,
            )

            catalog_file_text = catalog_file_response.text
            logger.info(
                f"Tilda catalog file: status={catalog_file_response.status_code}, response={catalog_file_text[:500]}"
            )
            catalog_file_success = catalog_file_text.startswith("success")

            if not catalog_file_success:
                logger.error(f"Catalog upload failed: {catalog_file_text}")
                logger.error(f"URL: {catalog_file_url}")
                logger.error(f"Size: {len(catalog_bytes)} bytes")
                logger.error(f"Response status: {catalog_file_response.status_code}")
                logger.error(f"Response headers: {dict(catalog_file_response.headers)}")

            if not catalog_file_success:
                return {
                    "success": False,
                    "catalog_result": {
                        "success": False,
                        "error": f"Catalog file upload failed: {catalog_file_text}",
                    },
                    "offers_result": {
                        "success": False,
                        "error": "Catalog upload failed, offers not sent",
                    },
                }

            # Шаг 4: отправка предложений (цены и остатки)
            # В Tilda для всех файлов используется type=catalog
            offers_file_params = {
                "type": "catalog",
                "mode": "file",
                "filename": "offers0_1.xml",
            }
            offers_file_url = (
                f"{tilda_base_url_local}?{urllib.parse.urlencode(offers_file_params)}"
            )

            # Tilda CommerceML принимает XML напрямую в теле POST запроса с Content-Type: text/xml; charset=utf-8
            logger.info(f"Sending offers file: {len(offers_bytes)} bytes")

            # Убеждаемся, что это действительно байты
            if isinstance(offers_bytes, str):
                offers_bytes = offers_bytes.encode("utf-8")
                logger.warning(
                    f"Offers bytes were string, encoded to bytes: {len(offers_bytes)} bytes"
                )

            offers_file_response = session.post(
                offers_file_url,
                data=offers_bytes,
                headers={"Content-Type": "text/xml; charset=utf-8"},
                timeout=30,
            )
            offers_file_text = offers_file_response.text
            logger.info(
                f"Tilda offers file: status={offers_file_response.status_code}, response={offers_file_text[:500]}"
            )
            offers_file_success = offers_file_text.startswith("success")

            if not offers_file_success:
                logger.error(f"Offers upload failed: {offers_file_text}")
                logger.error(f"URL: {offers_file_url}")
                logger.error(f"Size: {len(offers_bytes)} bytes")

            if not offers_file_success:
                return {
                    "success": False,
                    "catalog_result": {
                        "success": True,
                        "message": "Catalog uploaded successfully",
                    },
                    "offers_result": {
                        "success": False,
                        "error": f"Offers file upload failed: {offers_file_text}",
                    },
                }

            # Шаг 5: import - импорт catalog
            catalog_import_params = {
                "type": "catalog",
                "mode": "import",
                "filename": "import0_1.xml",
            }
            catalog_import_url = f"{tilda_base_url_local}?{urllib.parse.urlencode(catalog_import_params)}"

            catalog_import_result = _run_import_until_done(
                catalog_import_url, label="catalog"
            )
            if not catalog_import_result.get("success"):
                return {
                    "success": False,
                    "catalog_result": {
                        "success": False,
                        "file_upload": catalog_file_text,
                        "import": catalog_import_result.get("response", ""),
                        "error": catalog_import_result.get(
                            "error", "Catalog import failed"
                        ),
                    },
                    "offers_result": {
                        "success": False,
                        "error": "Catalog import failed, offers not imported",
                    },
                }

            # Шаг 6: импорт предложений
            offers_import_params = {
                "type": "catalog",
                "mode": "import",
                "filename": "offers0_1.xml",
            }
            offers_import_url = (
                f"{tilda_base_url_local}?{urllib.parse.urlencode(offers_import_params)}"
            )

            offers_import_result = _run_import_until_done(
                offers_import_url, label="offers"
            )
            if not offers_import_result.get("success"):
                return {
                    "success": False,
                    "catalog_result": {
                        "success": True,
                        "file_upload": catalog_file_text,
                        "import": catalog_import_result.get("response", ""),
                        "import_attempts": catalog_import_result.get("attempts", 1),
                    },
                    "offers_result": {
                        "success": False,
                        "file_upload": offers_file_text,
                        "import": offers_import_result.get("response", ""),
                        "error": offers_import_result.get(
                            "error", "Offers import failed"
                        ),
                    },
                }

            return {
                "success": True,
                "catalog_result": {
                    "success": True,
                    "file_upload": catalog_file_text,
                    "import": catalog_import_result.get("response", ""),
                    "import_attempts": catalog_import_result.get("attempts", 1),
                },
                "offers_result": {
                    "success": True,
                    "file_upload": offers_file_text,
                    "import": offers_import_result.get("response", ""),
                    "import_attempts": offers_import_result.get("attempts", 1),
                },
            }

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Tilda sync error: {error_msg}", exc_info=True)
            return {
                "success": False,
                "catalog_result": {
                    "success": False,
                    "error": f"Unexpected error during sync: {error_msg}",
                },
                "offers_result": {
                    "success": False,
                    "error": f"Unexpected error during sync: {error_msg}",
                },
            }

    # Запускаем синхронную функцию в executor с явной передачей параметров
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _sync_send,
        catalog_xml_bytes_local,
        offers_xml_bytes_local,
        tilda_url,
        username,
        password,
    )
    return result


async def sync_feed_to_tilda_by_id(feed_id: int) -> dict:
    """
    Функция для автоматической синхронизации фида с Tilda по ID.
    Используется джобом для периодической отправки.

    Args:
        feed_id: ID фида для синхронизации

    Returns:
        Результат синхронизации
    """
    from api.feeds.feed_generator.generator import FeedGenerator
    from database.db import database, feeds

    query = feeds.select().where(feeds.c.id == feed_id)
    feed = await database.fetch_one(query)

    if feed is None:
        return {"success": False, "error": "Feed not found"}

    # Проверяем, что синхронизация включена и есть все необходимые данные
    if not feed.get("tilda_sync_enabled"):
        return {"success": False, "error": "Tilda sync is disabled for this feed"}

    tilda_url = feed.get("tilda_url")
    tilda_username = feed.get("tilda_username")
    tilda_password = feed.get("tilda_password")

    if not tilda_url or not tilda_username or not tilda_password:
        return {"success": False, "error": "Tilda credentials not configured"}

    feed_cashbox_id = feed.get("cashbox_id")
    if not feed_cashbox_id:
        return {"success": False, "error": "Cashbox ID not found"}

    # Генерируем XML
    generator = FeedGenerator(feed["url_token"])

    catalog_xml = await generator.generate_catalog(cashbox_id=feed_cashbox_id)
    if catalog_xml is None:
        return {"success": False, "error": "Failed to generate catalog XML"}

    offers_xml = await generator.generate_offers(cashbox_id=feed_cashbox_id)
    if offers_xml is None:
        return {"success": False, "error": "Failed to generate offers XML"}

    # Отправляем в Tilda
    sync_result = await send_both_feeds_to_tilda(
        catalog_xml=catalog_xml,
        offers_xml=offers_xml,
        tilda_url=tilda_url,
        username=tilda_username,
        password=tilda_password,
    )

    # Обновляем updated_at после успешной синхронизации
    if sync_result.get("success"):
        from sqlalchemy import func

        update_query = (
            feeds.update().where(feeds.c.id == feed_id).values(updated_at=func.now())
        )
        await database.execute(update_query)
        logger.info(f"Updated feed {feed_id} updated_at after successful sync")

    return {
        "feed_id": feed_id,
        "feed_name": feed["name"],
        "success": sync_result.get("success", False),
        "catalog_sync_result": sync_result.get("catalog_result", {}),
        "offers_sync_result": sync_result.get("offers_result", {}),
    }


async def send_feed_to_tilda(
    xml_content: str,
    tilda_url: str,
    username: str,
    password: str,
    mode: str = "import",
    type: str = "catalog",
    filename: str = "import.xml",
) -> dict:
    """Отправляет один файл CommerceML в Tilda.

    Примечание: основной путь интеграции использует `send_both_feeds_to_tilda` (requests),
    но этот метод оставляем рабочим для одиночных вызовов/отладки.
    """

    import urllib.parse

    try:
        auth = BasicAuth(username, password)

        # Сформировать URL запроса
        if mode in ("checkauth", "init"):
            params = {"mode": mode, "type": type}
        elif mode in ("file", "import"):
            params = {"mode": mode, "type": type, "filename": filename}
        else:
            params = {"mode": mode}

        url_with_params = f"{tilda_url.rstrip('/')}?{urllib.parse.urlencode(params)}"
        post_headers = {"Content-Type": "text/xml; charset=utf-8"}

        async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
            # Полная последовательность для import
            if mode == "import":
                # checkauth
                checkauth_url = f"{tilda_url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'checkauth', 'type': type})}"
                async with session.get(
                    checkauth_url,
                    auth=auth,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as r:
                    text = (await r.text()).strip()
                    if not text.startswith("success"):
                        return {
                            "success": False,
                            "status_code": r.status,
                            "response_text": text,
                            "error": f"Checkauth failed: {text}",
                        }

                # init
                init_url = f"{tilda_url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'init', 'type': type})}"
                async with session.get(
                    init_url,
                    auth=auth,
                    timeout=aiohttp.ClientTimeout(total=30),
                ):
                    pass

                # file
                file_url = f"{tilda_url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'file', 'type': type, 'filename': filename})}"
                async with session.post(
                    file_url,
                    data=xml_content.encode("utf-8"),
                    headers=post_headers,
                    auth=auth,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as fr:
                    file_text = (await fr.text()).strip()
                    if not file_text.startswith("success"):
                        # multipart fallback
                        form_data = aiohttp.FormData()
                        form_data.add_field(
                            "file",
                            xml_content.encode("utf-8"),
                            filename=filename,
                            content_type="application/xml",
                        )
                        async with session.post(
                            file_url,
                            data=form_data,
                            auth=auth,
                            timeout=aiohttp.ClientTimeout(total=60),
                        ) as fr2:
                            file_text = (await fr2.text()).strip()
                            if not file_text.startswith("success"):
                                return {
                                    "success": False,
                                    "status_code": fr2.status,
                                    "response_text": file_text,
                                    "error": f"File upload failed: {file_text}",
                                }

                # import (с учётом progress)
                import_url = f"{tilda_url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'import', 'type': type, 'filename': filename})}"
                deadline = time.monotonic() + 180
                last_text = ""
                last_status = 0
                while True:
                    async with session.post(
                        import_url,
                        data="",
                        headers=post_headers,
                        auth=auth,
                        timeout=aiohttp.ClientTimeout(total=60),
                    ) as ir:
                        last_status = ir.status
                        last_text = (await ir.text()).strip()

                    ok_http = 200 <= last_status < 300
                    if ok_http and last_text.startswith("success"):
                        return {
                            "success": True,
                            "status_code": last_status,
                            "response_text": last_text,
                            "error": None,
                        }
                    if ok_http and last_text.startswith("progress"):
                        if time.monotonic() >= deadline:
                            return {
                                "success": False,
                                "status_code": last_status,
                                "response_text": last_text,
                                "error": "Import did not finish in time (still progress)",
                            }
                        await asyncio.sleep(2)
                        continue

                    return {
                        "success": False,
                        "status_code": last_status,
                        "response_text": last_text,
                        "error": last_text,
                    }

            # Остальные режимы
            if mode in ("checkauth", "init", "query"):
                async with session.get(
                    url_with_params,
                    auth=auth,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as r:
                    text = ""
                    try:
                        text = await r.text()
                    except Exception:
                        pass
                    return {
                        "success": 200 <= r.status < 300,
                        "status_code": r.status,
                        "response_text": text,
                        "error": None if 200 <= r.status < 300 else text,
                    }

            async with session.post(
                url_with_params,
                data=(
                    xml_content.encode("utf-8")
                    if mode == "file"
                    else xml_content.encode("utf-8")
                ),
                headers=post_headers,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                text = ""
                try:
                    text = await r.text()
                except Exception:
                    pass
                return {
                    "success": 200 <= r.status < 300,
                    "status_code": r.status,
                    "response_text": text,
                    "error": None if 200 <= r.status < 300 else text,
                }

    except aiohttp.ClientError as e:
        error_msg = f"HTTP client error: {str(e)}"
        logger.error(f"Tilda sync error: {error_msg}")
        return {
            "success": False,
            "status_code": 0,
            "response_text": "",
            "error": error_msg,
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Tilda sync error: {error_msg}")
        return {
            "success": False,
            "status_code": 0,
            "response_text": "",
            "error": error_msg,
        }
