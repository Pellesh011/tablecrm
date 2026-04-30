"""Приём CommerceML: checkauth → init → file → import. По имени файла: offers — цены/остатки, иначе каталог."""

import asyncio
import io
import logging
import os
import secrets
import shutil
import tempfile
import urllib.parse
from pathlib import Path
from typing import Optional

import aioboto3
import aiohttp
from aiohttp import BasicAuth
from database.db import commerceml_connections, database
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import and_, select, update
from starlette.responses import PlainTextResponse

from .generators.orders import generate_orders_xml
from .generators.products import generate_products_xml
from .parsers.offers import parse_offers_xml
from .parsers.orders import parse_order_xml
from .parsers.products import parse_catalog_xml

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter(tags=["commerceml_server"])

security = HTTPBasic()
# query допускает Basic Auth без session_id
security_optional = HTTPBasic(auto_error=False)

COMMERCEML_FILE_LIMIT = 10_000_000

# Сессии обмена (по умолчанию in-memory)
sessions: dict[str, dict] = {}
# Файловое хранилище нужно как общий слой между несколькими воркерами uvicorn.
SESSION_STORAGE_ROOT = Path(tempfile.gettempdir()) / "commerceml_sessions"

SAVE_CML_COPIES = True
CML_COPIES_DIR = Path("/var/log/tablecrm/commerceml_copies")

S3_CML_BUCKET = os.getenv("S3_BUCKET_NAME", "5075293c-docs_generated")
AUTO_IMPORT_DELAY = 100


async def verify_credentials(
    credentials: HTTPBasicCredentials, connection_id: int
) -> bool:
    """Проверка логина и пароля. Не бросает исключений — возвращает False при ошибке."""
    try:
        query = select(commerceml_connections).where(
            and_(
                commerceml_connections.c.id == connection_id,
                commerceml_connections.c.active == True,
                commerceml_connections.c.is_deleted == False,
            )
        )
        connection = await database.fetch_one(query)
        if not connection:
            return False
        u = getattr(connection, "username", None)
        p = getattr(connection, "password", None)
        return (
            u is not None
            and p is not None
            and str(u).strip() == str(credentials.username or "").strip()
            and str(p) == str(credentials.password or "")
        )
    except Exception as e:
        logger.warning("commerceml verify_credentials failed: %s", e)
        return False


# Ответы с CRLF
CML_LF = "\r\n"


def _session_dir(session_id: str) -> Path:
    """Путь к директории одной CommerceML-сессии."""
    return SESSION_STORAGE_ROOT / session_id


def _session_exists(session_id: str) -> bool:
    """Проверяет, существует ли сессия в памяти или во временном хранилище."""
    return session_id in sessions or _session_dir(session_id).is_dir()


def _create_session_storage(session_id: str, connection_id: int) -> None:
    """Создаёт файловую директорию сессии сразу после checkauth."""
    session_path = _session_dir(session_id)
    session_path.mkdir(parents=True, exist_ok=True)
    (session_path / ".connection_id").write_text(str(connection_id), encoding="utf-8")


def _append_session_file(session_id: str, filename: str, content: bytes) -> None:
    """Дозаписывает очередной chunk файла CommerceML в общую файловую сессию."""
    session_path = _session_dir(session_id)
    session_path.mkdir(parents=True, exist_ok=True)
    with (session_path / filename).open("ab") as file_obj:
        file_obj.write(content)


def _load_session_files(session_id: str) -> dict[str, bytes]:
    """Собирает все файлы сессии с диска, когда запрос попал в другой воркер."""
    result: dict[str, bytes] = {}
    session_path = _session_dir(session_id)
    if not session_path.is_dir():
        return result
    for file_path in session_path.iterdir():
        if file_path.name.startswith(".") or not file_path.is_file():
            continue
        result[file_path.name] = file_path.read_bytes()
    return result


def _drop_session_storage(session_id: str) -> None:
    """Полностью очищает сессию после завершения обмена."""
    sessions.pop(session_id, None)
    shutil.rmtree(_session_dir(session_id), ignore_errors=True)


async def _schedule_auto_import(
    connection_id: int, session_id: str, filename: str, type: str
):
    """Запланировать автоматический импорт для файла после задержки."""
    await asyncio.sleep(AUTO_IMPORT_DELAY)

    session = sessions.get(session_id)
    if session is None:
        loaded_files = _load_session_files(session_id)
        if not loaded_files:
            logger.warning(
                f"Session {session_id} not found on disk, cannot auto-import"
            )
            return
        session = {"files": loaded_files}
        sessions[session_id] = session

    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == connection_id,
            commerceml_connections.c.active == True,
            commerceml_connections.c.is_deleted == False,
        )
    )
    connection = await database.fetch_one(query)
    if not connection:
        return

    file_content = session["files"].get(filename)
    if not file_content:
        return

    xml_content = (
        file_content.decode("utf-8")
        if isinstance(file_content, bytes)
        else str(file_content)
    )

    try:
        await _perform_import(
            connection_id=connection_id,
            cashbox_id=connection.cashbox_id,
            session_id=session_id,
            filename=filename,
            type=type,
            xml_content=xml_content,
            session_files=session.get("files", {}),
        )
        session.setdefault("imported_files", {})[filename] = True
    except Exception as e:
        logger.error(f"Auto-import failed for {filename}: {e}")


def _request_meta(request: Optional[Request]) -> dict:
    if not request:
        return {}
    return {
        "client_ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("user-agent"),
    }


async def _perform_import(
    connection_id: int,
    cashbox_id: int,
    session_id: str,
    filename: str,
    type: str,
    xml_content: str,
    session_files: dict,
) -> int:
    """Общая логика импорта: парсинг, обновление счётчиков, сохранение в S3, возвращает количество обработанных записей."""
    count = 0
    is_offers = (filename and "offers" in filename.lower()) or (
        type and type.lower() == "offers"
    )

    if (type == "catalog" or type == "offers") and True:
        if is_offers:
            count = await parse_offers_xml(xml_content, cashbox_id)
        else:
            count = await parse_catalog_xml(
                xml_content, cashbox_id, session_files=session_files
            )

        if count and count > 0 and not is_offers:
            await database.execute(
                update(commerceml_connections)
                .where(commerceml_connections.c.id == connection_id)
                .values(
                    products_loaded_count=commerceml_connections.c.products_loaded_count
                    + count
                )
            )
    elif type == "sale" and True:
        await parse_order_xml(xml_content, cashbox_id)
    else:
        logger.warning(f"Import type {type} not enabled for connection {connection_id}")

    if (
        S3_CML_BUCKET
        and os.getenv("S3_ACCESS")
        and os.getenv("S3_SECRET")
        and os.getenv("S3_URL")
    ):
        try:
            s3_session = aioboto3.Session()
            s3_kwargs = {
                "aws_access_key_id": os.getenv("S3_ACCESS"),
                "aws_secret_access_key": os.getenv("S3_SECRET"),
                "endpoint_url": os.getenv("S3_URL"),
            }
            s3_key = f"commerceml/{connection_id}/{session_id}/{filename}"
            file_bytes = (
                xml_content.encode("utf-8")
                if isinstance(xml_content, str)
                else xml_content
            )
            async with s3_session.client("s3", **s3_kwargs) as s3:
                # Создаём бакет если его нет
                try:
                    await s3.head_bucket(Bucket=S3_CML_BUCKET)
                except Exception:
                    try:
                        await s3.create_bucket(Bucket=S3_CML_BUCKET)
                        logger.info(f"Created S3 bucket: {S3_CML_BUCKET}")
                    except Exception as bucket_err:
                        logger.warning(
                            f"Could not create S3 bucket {S3_CML_BUCKET}: {bucket_err}"
                        )
                await s3.upload_fileobj(io.BytesIO(file_bytes), S3_CML_BUCKET, s3_key)
            logger.info(
                f"CommerceML файл {filename} загружен в S3: {S3_CML_BUCKET}/{s3_key}"
            )
        except Exception as e:
            logger.error(f"Ошибка загрузки файла {filename} в S3: {e}", exc_info=True)

    return count


def _log_cml_event(event: str, **payload) -> None:
    details = {
        key: value
        for key, value in payload.items()
        if value is not None and value != ""
    }
    logger.info("CommerceML %s: %s", event, details)


def _response_for_log(body: str) -> str:
    return body.strip().replace(CML_LF, "|")


def _session_file_size(session_id: str, filename: str) -> int:
    file_path = _session_dir(session_id) / filename
    if not file_path.is_file():
        return 0
    return file_path.stat().st_size


async def _do_checkauth(
    connection_id: int,
    credentials: HTTPBasicCredentials,
    request: Optional[Request] = None,
):
    """Общая логика checkauth (path или query mode=checkauth)."""
    if not await verify_credentials(credentials, connection_id):
        _log_cml_event(
            "checkauth_failed",
            connection_id=connection_id,
            **_request_meta(request),
        )
        raise HTTPException(status_code=401, detail="Invalid credentials")

    session_id = secrets.token_hex(16)
    sessions[session_id] = {
        "connection_id": connection_id,
        "files": {},
    }
    _create_session_storage(session_id, connection_id)
    # success + имя cookie + значение (3 строки)
    body = f"success{CML_LF}session_id{CML_LF}{session_id}{CML_LF}"
    _log_cml_event(
        "checkauth_success",
        connection_id=connection_id,
        session_id=session_id,
        response=_response_for_log(body),
        **_request_meta(request),
    )
    return PlainTextResponse(body, media_type="text/plain; charset=utf-8")


def _get_session_id(request: Request, fallback: str = "") -> str:
    """Session_id из заголовка, cookie или query."""
    return (
        request.headers.get("X-Session-Id")
        or request.query_params.get("session_id")
        or (request.cookies.get("session_id") if request.cookies else "")
        or fallback
    )


@router.get("/commerceml/{connection_id}")
async def commerceml_base(
    request: Request,
    connection_id: int,
    mode: str = "",
    type: str = "catalog",
    session_id: str = "",
    filename: str = "import.xml",
    credentials: Optional[HTTPBasicCredentials] = Depends(security_optional),
):
    """Один URL: checkauth, init, query, import, success."""
    if mode == "init":
        session_id_val = _get_session_id(request, session_id)
        by_session = session_id_val and _session_exists(session_id_val)
        by_auth = credentials and await verify_credentials(credentials, connection_id)
        if not by_session and not by_auth:
            _log_cml_event(
                "init_failed",
                connection_id=connection_id,
                session_id=session_id_val,
                type=type,
                reason="invalid_session_or_credentials",
                **_request_meta(request),
            )
            raise HTTPException(
                status_code=401, detail="Invalid session or credentials"
            )
        body = f"zip=no{CML_LF}file_limit={COMMERCEML_FILE_LIMIT}{CML_LF}"
        _log_cml_event(
            "init",
            connection_id=connection_id,
            session_id=session_id_val,
            type=type,
            by_session=bool(by_session),
            by_auth=bool(by_auth),
            response=_response_for_log(body),
            **_request_meta(request),
        )
        return PlainTextResponse(
            body,
            media_type="text/plain; charset=utf-8",
        )
    if mode == "query":
        return await query(connection_id, request, type)
    if mode == "import":
        # GET import после загрузки файлов
        return await import_data(
            connection_id, request, mode="import", type=type, filename=filename
        )
    if mode == "success":
        # Завершение обмена
        session_id_val = _get_session_id(request, session_id)
        if session_id_val:
            _drop_session_storage(session_id_val)
        body = "success" + CML_LF
        _log_cml_event(
            "success",
            connection_id=connection_id,
            session_id=session_id_val,
            response=_response_for_log(body),
            **_request_meta(request),
        )
        return PlainTextResponse(
            body,
            media_type="text/plain; charset=utf-8",
        )
    if mode and mode != "checkauth":
        raise HTTPException(status_code=404, detail="Not found")
    # checkauth требует Basic Auth
    if not credentials:
        raise HTTPException(status_code=401, detail="Basic auth required")
    return await _do_checkauth(connection_id, credentials, request)


@router.post("/commerceml/{connection_id}")
async def commerceml_base_post(
    request: Request,
    connection_id: int,
    mode: str = "",
    type: str = "catalog",
    filename: str = "import.xml",
):
    """POST с mode=file или mode=import."""
    if mode == "file":
        return await file(connection_id, request, filename)
    if mode == "import":
        return await import_data(
            connection_id, request, mode="import", type=type, filename=filename
        )
    raise HTTPException(status_code=404, detail="Not found")


@router.get("/commerceml/{connection_id}/checkauth")
async def checkauth(
    request: Request,
    connection_id: int,
    credentials: HTTPBasicCredentials = Depends(security),
):
    """Checkauth по path."""
    return await _do_checkauth(connection_id, credentials, request)


@router.get("/commerceml/{connection_id}/init")
async def init(
    connection_id: int,
    request: Request,
    type: str = "catalog",
    mode: str = "checkauth",
):
    """Init обмена."""
    session_id = _get_session_id(request)
    if not session_id or not _session_exists(session_id):
        _log_cml_event(
            "init_failed",
            connection_id=connection_id,
            session_id=session_id,
            type=type,
            reason="invalid_session",
            **_request_meta(request),
        )
        raise HTTPException(status_code=401, detail="Invalid session")

    # zip=no, file_limit
    body = f"zip=no{CML_LF}file_limit={COMMERCEML_FILE_LIMIT}{CML_LF}"
    _log_cml_event(
        "init",
        connection_id=connection_id,
        session_id=session_id,
        type=type,
        response=_response_for_log(body),
        **_request_meta(request),
    )
    return PlainTextResponse(
        body,
        media_type="text/plain; charset=utf-8",
    )


@router.post("/commerceml/{connection_id}/file")
async def file(
    connection_id: int,
    request: Request,
    filename: str,
):
    """Приём файла, сохранение в сессию."""
    session_id = _get_session_id(request)
    if not session_id or not _session_exists(session_id):
        _log_cml_event(
            "file_failed",
            connection_id=connection_id,
            session_id=session_id,
            filename=filename,
            reason="invalid_session",
            **_request_meta(request),
        )
        raise HTTPException(status_code=401, detail="Invalid session")

    if session_id not in sessions:
        loaded_files = _load_session_files(session_id)
        sessions[session_id] = {
            "files": loaded_files,
        }

    content = await request.body()
    sessions[session_id]["files"][filename] = (
        sessions[session_id]["files"].get(filename, b"") + content
    )
    _append_session_file(session_id, filename, content)
    total_size = _session_file_size(session_id, filename)
    body = "success" + CML_LF
    _log_cml_event(
        "file_chunk_received",
        connection_id=connection_id,
        session_id=session_id,
        filename=filename,
        chunk_size=len(content),
        total_size=total_size,
        response=_response_for_log(body),
        **_request_meta(request),
    )

    if AUTO_IMPORT_DELAY > 0:
        session = sessions[session_id]
        if "pending_import_tasks" not in session:
            session["pending_import_tasks"] = {}
        if filename in session["pending_import_tasks"]:
            session["pending_import_tasks"][filename].cancel()
        auto_type = "offers" if "offers" in filename.lower() else "catalog"
        task = asyncio.create_task(
            _schedule_auto_import(connection_id, session_id, filename, auto_type)
        )
        session["pending_import_tasks"][filename] = task
        logger.info(
            f"Scheduled auto-import for {filename} in session {session_id} after {AUTO_IMPORT_DELAY}s"
        )

    return PlainTextResponse(body, media_type="text/plain; charset=utf-8")


@router.post("/commerceml/{connection_id}/import")
async def import_data(
    connection_id: int,
    request: Request,
    mode: str = "import",
    type: str = "catalog",
    filename: str = "import.xml",
):
    """Импорт: файл из сессии или из тела запроса."""
    session_id = _get_session_id(request)
    if not session_id or not _session_exists(session_id):
        _log_cml_event(
            "import_failed",
            connection_id=connection_id,
            session_id=session_id,
            type=type,
            filename=filename,
            reason="invalid_session",
            **_request_meta(request),
        )
        raise HTTPException(status_code=401, detail="Invalid session")

    session = sessions.get(session_id)
    if (
        session
        and "pending_import_tasks" in session
        and filename in session["pending_import_tasks"]
    ):
        session["pending_import_tasks"][filename].cancel()
        del session["pending_import_tasks"][filename]
        logger.info(
            f"Cancelled auto-import for {filename} in session {session_id} because real import arrived"
        )

    if session is None:
        session = {"files": _load_session_files(session_id)}
    elif "files" not in session:
        session["files"] = _load_session_files(session_id)

    session_files = sorted(session.get("files", {}).keys())
    requested_file_present = filename in session.get("files", {})

    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == connection_id,
            commerceml_connections.c.active == True,
            commerceml_connections.c.is_deleted == False,
        )
    )
    connection = await database.fetch_one(query)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    xml_content = None
    request_body_size = 0
    _log_cml_event(
        "import_requested",
        connection_id=connection_id,
        session_id=session_id,
        type=type,
        mode=mode,
        filename=filename,
        requested_file_present=requested_file_present,
        session_files=session_files,
        **_request_meta(request),
    )

    if filename in session["files"]:
        file_content = session["files"][filename]
        if isinstance(file_content, bytes):
            xml_content = file_content.decode("utf-8")
        else:
            xml_content = str(file_content)
    else:
        body = await request.body()
        if body:
            request_body_size = len(body)
            xml_content = body.decode("utf-8")
        logger.warning(
            "CommerceML import file is missing in session: connection_id=%s "
            "session_id=%s filename=%s available_files=%s request_body_size=%s",
            connection_id,
            session_id,
            filename,
            session_files,
            request_body_size,
        )

    if xml_content:
        lines_count = len(xml_content.splitlines())
        logger.info(
            f"commerceml Файл {filename} для сессии {session_id} содержит {lines_count} строк"
        )

    if not xml_content or not xml_content.strip():
        logger.error(
            "Empty XML content for filename %s in session %s; connection_id=%s "
            "available_files=%s request_body_size=%s",
            filename,
            session_id,
            connection_id,
            session_files,
            request_body_size,
        )
        return PlainTextResponse(
            "failure" + CML_LF + "Empty file content" + CML_LF,
            status_code=400,
            media_type="text/plain; charset=utf-8",
        )

    # Всегда разрешаем импорт при наличии валидной сессии — флаги import_products/import_orders
    # используются только для явной ручной синхронизации через /sync, но не блокируют 1С
    can_import = True
    if type not in ("catalog", "offers", "sale"):
        logger.warning(f"Import type {type} not enabled for connection {connection_id}")
        can_import = False

    if not can_import:
        body = "success" + CML_LF
        _log_cml_event(
            "import_skipped",
            connection_id=connection_id,
            session_id=session_id,
            type=type,
            filename=filename,
            reason="import_not_enabled",
            response=_response_for_log(body),
            **_request_meta(request),
        )
        return PlainTextResponse(body, media_type="text/plain; charset=utf-8")

    try:
        count = await _perform_import(
            connection_id=connection_id,
            cashbox_id=connection.cashbox_id,
            session_id=session_id,
            filename=filename,
            type=type,
            xml_content=xml_content,
            session_files=session.get("files", {}),
        )
        if session and "imported_files" not in session:
            session["imported_files"] = {}
        if session:
            session["imported_files"][filename] = True

        body = "success" + CML_LF
        _log_cml_event(
            "import_success",
            connection_id=connection_id,
            session_id=session_id,
            type=type,
            filename=filename,
            is_offers=(filename and "offers" in filename.lower())
            or (type and type.lower() == "offers"),
            imported_count=count,
            response=_response_for_log(body),
            **_request_meta(request),
        )
        return PlainTextResponse(body, media_type="text/plain; charset=utf-8")
    except Exception as e:
        logger.error(f"Error importing {type}: {str(e)}", exc_info=True)
        return PlainTextResponse(
            "failure" + CML_LF + str(e) + CML_LF,
            status_code=500,
            media_type="text/plain; charset=utf-8",
        )


@router.get("/commerceml/{connection_id}/query")
async def query(
    connection_id: int,
    request: Request,
    type: str = "catalog",
    credentials: Optional[HTTPBasicCredentials] = Depends(security_optional),
):
    """Выгрузка XML: каталог или заказы. Auth: session_id или Basic Auth."""
    logger.info(
        f"Commerceml - Query started: connection_id={connection_id}, type={type}"
    )
    session_id = _get_session_id(request)
    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == connection_id,
            commerceml_connections.c.active == True,
            commerceml_connections.c.is_deleted == False,
        )
    )
    connection = await database.fetch_one(query)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    by_session = session_id and session_id in sessions
    by_auth = credentials and await verify_credentials(credentials, connection_id)
    if not by_session and not by_auth:
        raise HTTPException(status_code=401, detail="Invalid session or credentials")

    try:
        if type == "catalog" and connection.export_products:
            xml_content = await generate_products_xml(connection.cashbox_id)
            logger.info(
                f"Commerceml - Generated catalog XML, length={len(xml_content)}"
            )
        elif type == "sale" and connection.export_orders:
            xml_content, orders_count = await generate_orders_xml(connection.cashbox_id)
            logger.info(
                f"Commerceml - Generated orders XML, {orders_count} orders, length={len(xml_content)}"
            )
            if orders_count > 0:
                await database.execute(
                    update(commerceml_connections)
                    .where(commerceml_connections.c.id == connection_id)
                    .values(
                        orders_exported_count=commerceml_connections.c.orders_exported_count
                        + orders_count
                    )
                )
        else:
            raise HTTPException(
                status_code=400, detail=f"Export type {type} not enabled"
            )

        from fastapi.responses import Response

        return Response(
            content=xml_content.encode("utf-8"),
            media_type="application/xml; charset=utf-8",
        )
    except Exception as e:
        logger.error(f"commerceml Error generating {type}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


async def send_commerceml_data_to_url(
    connection_id: int,
    type: str = "catalog",
) -> dict:
    """Отправка CommerceML на URL подключения."""
    logger.info(
        f"Commerceml - Starting outgoing sync: connection_id={connection_id}, type={type}"
    )
    query = select(commerceml_connections).where(
        and_(
            commerceml_connections.c.id == connection_id,
            commerceml_connections.c.active == True,
            commerceml_connections.c.is_deleted == False,
        )
    )
    connection = await database.fetch_one(query)
    if not connection:
        return {"success": False, "error": "Connection not found"}

    if not connection.url:
        return {"success": False, "error": "Connection URL not set"}

    if type == "import_catalog":
        type = "catalog"
    orders_count = 0
    try:
        if type == "catalog" and connection.export_products:
            xml_content = await generate_products_xml(connection.cashbox_id)
            filename = "import.xml"
        elif type == "sale" and connection.export_orders:
            xml_content, orders_count = await generate_orders_xml(connection.cashbox_id)
            filename = "orders.xml"
        else:
            return {"success": False, "error": f"Export type {type} not enabled"}

        auth = BasicAuth(connection.username, connection.password)

        async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
            checkauth_url = f"{connection.url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'checkauth', 'type': type})}"
            logger.debug(f"Commerceml - Sending checkauth to {checkauth_url}")
            async with session.get(
                checkauth_url,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                logger.info(f"commerceml - checkauth response: status={r.status}")
                text = (await r.text()).strip()
                if not text.startswith("success"):
                    return {
                        "success": False,
                        "status_code": r.status,
                        "response_text": text,
                        "error": f"Checkauth failed: {text}",
                    }
                lines = [s.strip() for s in text.splitlines()]
                remote_session_id = (
                    lines[2]
                    if len(lines) > 2
                    else (lines[1] if len(lines) > 1 else None)
                )

            init_url = f"{connection.url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'init', 'type': type, 'session_id': remote_session_id})}"
            logger.debug(f"commerceml Sending init to {init_url}")
            async with session.get(
                init_url,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as r:
                if r.status != 200:
                    return {
                        "success": False,
                        "status_code": r.status,
                        "error": "Init failed",
                    }

            file_url = f"{connection.url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'file', 'type': type, 'filename': filename, 'session_id': remote_session_id})}"
            logger.info(f"commerceml Sending file {filename}, size={len(xml_content)}")
            post_headers = {"Content-Type": "text/xml; charset=utf-8"}
            async with session.post(
                file_url,
                data=xml_content.encode("utf-8"),
                headers=post_headers,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                file_text = (await r.text()).strip()
                if not file_text.startswith("success"):
                    return {
                        "success": False,
                        "status_code": r.status,
                        "response_text": file_text,
                        "error": f"File upload failed: {file_text}",
                    }

            import_url = f"{connection.url.rstrip('/')}?{urllib.parse.urlencode({'mode': 'import', 'type': type, 'filename': filename, 'session_id': remote_session_id})}"
            logger.debug(f"commerceml Sending import to {import_url}")
            async with session.post(
                import_url,
                data="",
                headers=post_headers,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                import_text = (await r.text()).strip()
                if import_text.startswith("success"):
                    if type == "sale" and orders_count > 0:
                        await database.execute(
                            update(commerceml_connections)
                            .where(commerceml_connections.c.id == connection_id)
                            .values(
                                orders_exported_count=commerceml_connections.c.orders_exported_count
                                + orders_count
                            )
                        )
                    return {
                        "success": True,
                        "status_code": r.status,
                        "response_text": import_text,
                    }
                else:
                    return {
                        "success": False,
                        "status_code": r.status,
                        "response_text": import_text,
                        "error": f"Import failed: {import_text}",
                    }

    except Exception as e:
        logger.error(
            f"commerceml Error sending {type} to {connection.url}: {str(e)}",
            exc_info=True,
        )
        return {"success": False, "error": str(e)}
