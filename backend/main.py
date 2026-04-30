import asyncio
import json
import logging
import os
import time
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
from api.analytics.routers import router as analytics_router
from api.apple_wallet.routers import router as apple_wallet_router
from api.apple_wallet_card_settings.routers import (
    router as apple_wallet_card_settings_router,
)
from api.articles.routers import router as articles_router
from api.autosuggestion.routers import router as autosuggestion_router
from api.balances.routers import router as balances_router
from api.balances.transactions_routers import (
    router as transactions_router,
    tinkoff_callback,
    tinkoff_router,
)
from api.blog.routers import (
    public_router as public_blog_router,
    router as blog_router,
)
from api.cashboxes.routers import router as cboxes_router
from api.categories.routers import router as categories_router
from api.categories.web.InstallCategoriesWeb import InstallCategoriesWeb

# from api.health.rabbitmq_check import router as rabbitmq_health_router
from api.chats.avito.avito_consumer import avito_consumer
from api.chats.avito.avito_default_webhook import router as avito_default_webhook_router
from api.chats.avito.avito_routes import router as avito_router
from api.chats.max.max_routes import router as max_router
from api.chats.rabbitmq_consumer import chat_consumer
from api.chats.routers import router as chats_router
from api.chats.telegram.telegram_routes import router as telegram_router
from api.chats.websocket import router as chats_ws_router
from api.cheques.routers import router as cheques_router
from api.commerceml.routers import router as commerceml_router
from api.commerceml.server import router as commerceml_server_router
from api.contracts.routers import router as contracts_router
from api.contragents.routers import router as contragents_router
from api.contragents.web.InstallContragentsWeb import InstallContragentsWeb
from api.distribution_docs.routers import router as distribution_docs_router
from api.docs_generate.routers import router as doc_generate_router
from api.docs_purchases.routers import router as docs_purchases_router
from api.docs_reconciliation.routers import router as docs_reconciliation_router
from api.docs_sales.api.routers import router as docs_sales_router
from api.docs_sales.web.InstallDocsSalesWeb import InstallDocsSalesWeb
from api.docs_sales_utm_tags.routers import router as utm_router
from api.docs_warehouses.routers import router as docs_warehouses_router
from api.employee_shifts.routers import router as employee_shifts_router
from api.events.routers import router as events_router
from api.feeds.routers import router as feeds_router
from api.fifo_settings.routers import router as fifo_settings_router
from api.files.routers import router as files_router
from api.folders.routers import router as folders_router
from api.functions.routers import router as entity_functions_router
from api.gross_profit_docs.routers import router as gross_profit_docs_router
from api.installs.routers import router as installs_router
from api.integrations.routers import router as int_router
from api.loyality_cards.routers import router as loyality_cards
from api.loyality_settings.routers import router as loyality_settings
from api.loyality_transactions.routers import router as loyality_transactions
from api.loyality_transactions.web.InstallLoyalityTransactionsWeb import (
    InstallLoyalityTransactionsWeb,
)
from api.manufacturers.web.InstallManufacturersWeb import InstallManufacturersWeb
from api.marketplace.routers import router as marketplace_router
from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from api.nomenclature.infrastructure.readers.impl.NomenclatureReader import (
    NomenclatureReader,
)
from api.nomenclature.routers import router as nomenclature_router
from api.nomenclature.video.routers import router as nomenclature_videos_router
from api.nomenclature.web.InstallNomenclatureWeb import InstallNomenclatureWeb
from api.nomenclature_attributes.infrastructure.functions.core.IDeleteNomenclatureAttributesFunction import (
    IDeleteNomenclatureAttributesFunction,
)
from api.nomenclature_attributes.infrastructure.functions.core.IInsertNomenclatureAttributesFunction import (
    IInsertNomenclatureAttributesFunction,
)
from api.nomenclature_attributes.infrastructure.functions.impl.DeleteNomenclatureAttributesFunction import (
    DeleteNomenclatureAttributesFunction,
)
from api.nomenclature_attributes.infrastructure.functions.impl.InsertNomenclatureAttributesFunction import (
    InsertNomenclatureAttributesFunction,
)
from api.nomenclature_attributes.infrastructure.readers.core.INomenclatureAttributesReader import (
    INomenclatureAttributesReader,
)
from api.nomenclature_attributes.infrastructure.readers.impl.NomenclatureAttributesReader import (
    NomenclatureAttributesReader,
)
from api.nomenclature_attributes.web.InstallNomenclatureAttributesWeb import (
    InstallNomenclatureAttributesWeb,
)
from api.nomenclature_groups.infrastructure.functions.core.IAddNomenclatureToGroupFunction import (
    IAddNomenclatureToGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IChangeMainNomenclGroupFunction import (
    IChangeMainNomenclGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.ICreateNomenclatureGroupFunction import (
    ICreateNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IDeleteNomenclatureGroupFunction import (
    IDeleteNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IDelNomenclatureFromGroupFunction import (
    IDelNomenclatureFromGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.core.IPatchNomenclatureGroupFunction import (
    IPatchNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.impl.AddNomenclatureToGroupFunction import (
    AddNomenclatureToGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.impl.ChangeMainNomenclGroupFunction import (
    ChangeMainNomenclGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.impl.CreateNomenclatureGroupFunction import (
    CreateNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.impl.DeleteNomenclatureGroupFunction import (
    DeleteNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.impl.DelNomenclatureFromGroupFunction import (
    DelNomenclatureFromGroupFunction,
)
from api.nomenclature_groups.infrastructure.functions.impl.PatchNomenclatureGroupFunction import (
    PatchNomenclatureGroupFunction,
)
from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import (
    INomenclatureGroupsReader,
)
from api.nomenclature_groups.infrastructure.readers.impl.NomenclatureGroupsReader import (
    NomenclatureGroupsReader,
)
from api.nomenclature_groups.web.InstallNomenclatureGroupsWeb import (
    InstallNomenclatureGroupsWeb,
)
from api.oauth.routes import router as oauth_router
from api.observability.hourly_stats_middleware import HourlyStatsMiddleware
from api.observability.request_logging_middleware import RequestLoggingMiddleware
from api.observability.router import router as observability_router
from api.organizations.routers import router as organizations_router
from api.payments.routers import (
    router as payments_router,
)
from api.pboxes.routers import router as pboxes_router
from api.pictures.routers import router as pictures_router
from api.price_types.routers import router as price_types_router
from api.prices.routers import router as prices_router
from api.projects.routers import router as projects_router
from api.promocodes.routers import router as promocodes_router
from api.qr.routes import router as qr_router
from api.rate_limiter import limiter
from api.reports.routers import router as reports_router
from api.segments.routers import router as segments_router
from api.segments_tags.routers import router as segments_tags_router
from api.settings.amo_triggers.routers import router as triggers_router
from api.settings.cashbox.routers import router as cashbox_settings_router
from api.tags.routers import router as tags_router
from api.tech_cards.router import router as tech_cards_router
from api.tech_operations.router import router as tech_operations_router
from api.templates.routers import router as templates_router
from api.trigger_notification.routers import router as triggers_notification
from api.units.routers import router as units_router
from api.users.routers import router as users_router
from api.warehouse_balances.routers import router as warehouse_balances_router
from api.warehouses.routers import router as warehouses_router
from api.webapp.routers import router as webapp_router
from api.webhooks.routers import router as webhook_router
from api.websockets.routers import router as websockets_router
from apps.amocrm.api.pair.routes import router as amo_pair_router
from apps.amocrm.install.web.routes import router as amo_install_router
from apps.amocrm.installer.infrastructure.repositories.core.IWidgetInstallerRepository import (
    IWidgetInstallerRepository,
)
from apps.amocrm.installer.infrastructure.repositories.impl.WidgetInstallerRepository import (
    WidgetInstallerRepository,
)
from apps.amocrm.installer.web.InstallWidgetInstallerInfoWeb import (
    InstallWidgetInstallerInfoWeb,
)
from apps.booking.booking.infrastructure.repositories.core.IBookingRepository import (
    IBookingRepository,
)
from apps.booking.booking.infrastructure.repositories.impl.BookingRepository import (
    BookingRepository,
)
from apps.booking.events.infrastructure.repositories.core.IBookingEventsRepository import (
    IBookingEventsRepository,
)
from apps.booking.events.infrastructure.repositories.impl.BookingEventsRepository import (
    BookingEventsRepository,
)
from apps.booking.events.web.InstallBookingEventsWeb import InstallBookingEventsWeb
from apps.booking.nomenclature.infrastructure.repositories.core.IBookingNomenclatureRepository import (
    IBookingNomenclatureRepository,
)
from apps.booking.nomenclature.infrastructure.repositories.impl.BookingNomenclatureRepository import (
    BookingNomenclatureRepository,
)
from apps.booking.repeat.web.InstallBookingRepeatWeb import InstallBookingRepeatWeb
from apps.booking.routers import router as booking_router
from apps.cdek.routes import router as cdek_router
from apps.evotor.routes import (
    router as evotor_router,
    router_auth as evotor_router_auth,
)
from apps.module_bank.routes import router as module_bank_router
from apps.tochka_bank.routes import router as tochka_router
from apps.yookassa.repositories.core.IYookassaCrmPaymentsRepository import (
    IYookassaCrmPaymentsRepository,
)
from apps.yookassa.repositories.core.IYookassaOauthRepository import (
    IYookassaOauthRepository,
)
from apps.yookassa.repositories.core.IYookassaPaymentsRepository import (
    IYookassaPaymentsRepository,
)
from apps.yookassa.repositories.core.IYookassaRequestRepository import (
    IYookassaRequestRepository,
)
from apps.yookassa.repositories.core.IYookassaTableNomenclature import (
    IYookassaTableNomenclature,
)
from apps.yookassa.repositories.core.IYookasssaAmoTableCrmRepository import (
    IYookasssaAmoTableCrmRepository,
)
from apps.yookassa.repositories.impl.YookassaCrmPaymentsRepository import (
    YookassaCrmPaymentsRepository,
)
from apps.yookassa.repositories.impl.YookassaOauthRepository import (
    YookassaOauthRepository,
)
from apps.yookassa.repositories.impl.YookassaPaymentsRepository import (
    YookassaPaymentsRepository,
)
from apps.yookassa.repositories.impl.YookassaRequestRepository import (
    YookassaRequestRepository,
)
from apps.yookassa.repositories.impl.YookassaTableNomenclature import (
    YookassaTableNomenclature,
)
from apps.yookassa.repositories.impl.YookasssaAmoTableCrmRepository import (
    YookasssaAmoTableCrmRepository,
)
from apps.yookassa.web.InstallOauthWeb import InstallYookassaOauthWeb
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings
from common.redis_utils import get_redis_uri
from common.s3_service.core.IS3ServiceFactory import IS3ServiceFactory
from common.s3_service.impl.S3ServiceFactory import S3ServiceFactory
from common.s3_service.models.S3SettingsModel import S3SettingsModel
from common.utils.ioc.ioc import ioc
from database.db import SQLALCHEMY_DATABASE_URL, database
from database.fixtures import init_db
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from functions.events import write_event

# import sentry_sdk
from functions.users import get_user_id_cashbox_id_by_token
from jobs.jobs import scheduler
from scripts.upload_default_apple_wallet_images import DefaultImagesUploader
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.requests import ClientDisconnect

logger = logging.getLogger(__name__)
notification_consumer_task: Optional[asyncio.Task] = None
embedded_consumers_lock: Optional[asyncpg.Connection] = None
embedded_consumers_owner = False

EMBEDDED_CONSUMERS_LOCK_KEY = 90421017

# sentry_sdk.init(
#     dsn="https://92a9c03cbf3042ecbb382730706ceb1b@sentry.tablecrm.com/4",
#     enable_tracing=True,
#     # Set traces_sample_rate to 1.0 to capture 100%
#     # of transactions for performance monitoring.
#     # We recommend adjusting this value in production,
#     traces_sample_rate=1.0,
# )

app = FastAPI(
    root_path="/api/v1",
    title="TABLECRM API",
    description="Документация API TABLECRM",
    # version="1.0",
    # docs_url="/docs",
    # redoc_url="/redoc",
    # openapi_url="/openapi.json",
)

app.add_middleware(GZipMiddleware)
# Для CommerceML проверки с формы МойСклад (cross-origin + Basic Auth) нужны credentials.
# Без allow_credentials браузер не отправляет Authorization — «Не удалось установить соединение».
# CORS_ORIGINS через env: через запятую, например CORS_ORIGINS=https://a.com,https://b.com
_default_origins = [
    "https://online.moysklad.ru",
    "https://dev.tablecrm.com",
    "https://tablecrm.com",
    "http://localhost:5173",
    "http://localhost:3000",
    "https://bystroi.ru",
    "https://www.bystroi.ru",
    "https://kavkaz-market.com",
    "https://www.kavkaz-market.com",
]
CORS_ORIGINS = [
    o.strip() for o in (os.environ.get("CORS_ORIGINS") or "").split(",") if o.strip()
] or _default_origins
app.add_middleware(
    CORSMiddleware,
    # allow_origins=CORS_ORIGINS,
    allow_origin_regex=".*",  # пока что разрешаем всем
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Detected-City", "X-Detected-Lat", "X-Detected-Lon"],
)
app.state.limiter = limiter
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(HourlyStatsMiddleware)
app.add_middleware(SlowAPIMiddleware)


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": f"Превышен лимит запросов: {exc.detail}"},
        headers={"Retry-After": "60"},
    )


app.include_router(cdek_router)
app.include_router(tags_router)
app.include_router(triggers_notification)
app.include_router(triggers_router)
app.include_router(booking_router)
app.include_router(evotor_router)
app.include_router(evotor_router_auth)
app.include_router(analytics_router)
app.include_router(cboxes_router)
app.include_router(contragents_router)
app.include_router(payments_router)
app.include_router(transactions_router)
app.include_router(tinkoff_router)
app.include_router(pboxes_router)
app.include_router(projects_router)
app.include_router(articles_router)
app.include_router(users_router)
app.include_router(websockets_router)
app.include_router(installs_router)
app.include_router(balances_router)
app.include_router(cheques_router)
app.include_router(events_router)
app.include_router(amo_pair_router)
app.include_router(amo_install_router)
app.include_router(organizations_router)
app.include_router(contracts_router)
app.include_router(categories_router)
app.include_router(warehouses_router)
app.include_router(price_types_router)
app.include_router(prices_router)
app.include_router(nomenclature_router)
app.include_router(nomenclature_videos_router)
app.include_router(pictures_router)
app.include_router(folders_router)
app.include_router(files_router)
app.include_router(entity_functions_router)
app.include_router(units_router)
app.include_router(docs_sales_router)
app.include_router(docs_purchases_router)
app.include_router(docs_warehouses_router)
app.include_router(docs_reconciliation_router)
app.include_router(distribution_docs_router)
app.include_router(fifo_settings_router)
app.include_router(warehouse_balances_router)
app.include_router(webhook_router)
app.include_router(observability_router)

app.include_router(gross_profit_docs_router)
app.include_router(loyality_cards)
app.include_router(loyality_transactions)
app.include_router(loyality_settings)
app.include_router(cashbox_settings_router)
app.include_router(segments_tags_router)
app.include_router(promocodes_router)

app.include_router(int_router)
app.include_router(oauth_router)

app.include_router(templates_router)
app.include_router(doc_generate_router)
app.include_router(webapp_router)

app.include_router(tochka_router)
app.include_router(reports_router)

app.include_router(module_bank_router)
app.include_router(utm_router)
app.include_router(segments_router)
app.include_router(marketplace_router)
app.include_router(tech_cards_router)
app.include_router(tech_operations_router)
app.include_router(autosuggestion_router)

app.include_router(employee_shifts_router)
app.include_router(apple_wallet_router)
app.include_router(apple_wallet_card_settings_router)

app.include_router(feeds_router)
app.include_router(commerceml_router)
app.include_router(commerceml_server_router)
app.include_router(chats_router)
app.include_router(chats_ws_router)
# app.include_router(rabbitmq_health_router)
app.include_router(avito_router)
app.include_router(avito_default_webhook_router)
app.include_router(telegram_router)
app.include_router(max_router)
app.include_router(blog_router)
app.include_router(public_blog_router)
app.include_router(qr_router)


# @app.get("/api/v1/openapi.json", include_in_schema=False)
# async def get_openapi():
#     """Проксировать openapi.json для Swagger UI"""
#     return app.openapi()


@app.get("/health")
async def check_health_app():
    return {"status": "ok"}


@app.post("/api/v1/payments/tinkoff/callback")
@app.get("/api/v1/payments/tinkoff/callback")
async def tinkoff_callback_direct(request: Request):
    return await tinkoff_callback(request)


@app.get("/hook/chat/123456", include_in_schema=False)
@app.post("/hook/chat/123456", include_in_schema=False)
async def avito_oauth_callback_legacy(
    request: Request,
    code: str = Query(None, description="Authorization code from Avito"),
    state: str = Query(None, description="State parameter for CSRF protection"),
    error: str = Query(None, description="Error from Avito OAuth"),
    error_description: str = Query(
        None, description="Error description from Avito OAuth"
    ),
    token: str = Query(None, description="Optional user authentication token"),
):
    if not code and not state and not error:
        return {"status": "ok", "message": "OAuth callback endpoint is available"}

    if error:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=400, detail=f"OAuth error: {error}. {error_description or ''}"
        )

    if not code or not state:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=400,
            detail="Missing required OAuth parameters: code and state are required",
        )

    try:
        from api.chats.avito.avito_routes import avito_oauth_callback

        result = await avito_oauth_callback(code=code, state=state, token=token)
        return result
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise


@app.post("/api/v1/hook/chat/{cashbox_id}", include_in_schema=False)
async def receive_avito_webhook_legacy(cashbox_id: int, request: Request):
    logger = logging.getLogger("main")
    logger.info(
        f"Webhook received: cashbox_id={cashbox_id}, method={request.method}, path={request.url.path}"
    )

    try:
        from api.chats.avito.avito_handler import AvitoHandler
        from api.chats.avito.avito_types import AvitoWebhook
        from api.chats.avito.avito_webhook import verify_webhook_signature

        body = await request.body()

        # Логируем сырой body от Avito API
        try:
            body_str = body.decode("utf-8")
            print("=" * 80)
            print(f"AVITO WEBHOOK RECEIVED (LEGACY) - cashbox_id={cashbox_id}")
            print("Raw body:")
            print(json.dumps(json.loads(body_str), indent=2, ensure_ascii=False))
            print("=" * 80)
        except Exception as e:
            print(f"Failed to log webhook body as JSON: {e}")
            print(f"Raw body (bytes): {body!r}")

        signature_header = request.headers.get("X-Avito-Signature")
        print(f"X-Avito-Signature header: {signature_header}")
        print(f"Request headers: {dict(request.headers)}")

        if signature_header:
            if not verify_webhook_signature(body, signature_header):
                logger.error("Webhook signature verification failed")
                return {"success": False, "message": "Invalid webhook signature"}

        try:
            webhook_data = json.loads(body.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to parse webhook JSON: {e}")
            return {"success": False, "message": f"Invalid webhook JSON: {str(e)}"}

        print(
            f"Parsed webhook data: {json.dumps(webhook_data, indent=2, ensure_ascii=False, default=str)}"
        )

        if not webhook_data:
            logger.error("Empty webhook data")
            return {"success": False, "message": "Empty webhook data"}

        has_id = "id" in webhook_data
        has_payload = "payload" in webhook_data
        has_timestamp = "timestamp" in webhook_data

        if not (has_id or has_payload):
            logger.warning(
                f"Webhook missing required fields. Has id: {has_id}, has payload: {has_payload}, has timestamp: {has_timestamp}"
            )
            logger.warning(f"Webhook keys: {list(webhook_data.keys())}")

        try:
            webhook = AvitoWebhook(**webhook_data)
        except Exception as e:
            logger.error(f"Invalid webhook structure: {e}", exc_info=True)
            return {"success": False, "message": f"Invalid webhook structure: {str(e)}"}

        result = await AvitoHandler.handle_webhook_event(webhook, cashbox_id)

        return {
            "success": result.get("success", False),
            "message": result.get("message", "Event processed"),
            "chat_id": result.get("chat_id"),
            "message_id": result.get("message_id"),
        }

    except Exception as e:
        logger.error(f"Error processing Avito webhook: {e}", exc_info=True)
        return {"success": False, "message": f"Error: {str(e)}"}


async def _write_event_safe(request: Request, status_code: int, duration: float):
    """Безопасная запись события в БД (асинхронно, с обработкой ошибок)."""
    try:
        if request.url.path in {"/health", "/docs", "/openapi.json", "/metrics"}:
            return

        token = request.query_params.get("token")
        if not token:
            token = request.path_params.get("token")
        if not token:
            return

        user_id, cashbox_id = await get_user_id_cashbox_id_by_token(token=token)
        if not user_id or not cashbox_id:
            return
        try:
            body = await request.body()
        except ClientDisconnect:
            logger.debug("Skipping event write for disconnected client")
            return
        payload = {}
        if body and request.headers.get("content-type") == "application/json":
            try:
                payload = json.loads(body)
            except:
                pass

        event_type = "cashevent"
        name = ""

        await write_event(
            type=event_type,
            name=name,
            method=request.method,
            url=str(request.url),
            payload=payload,
            cashbox_id=cashbox_id,
            user_id=user_id,
            token=token,
            ip=request.headers.get("X-Forwarded-For"),
            status_code=status_code,
            request_time=duration,
        )
    except Exception as e:
        logger.exception("Failed to write event (non-critical)")


async def _acquire_embedded_consumers_lock() -> bool:
    global embedded_consumers_lock

    connection = await asyncpg.connect(SQLALCHEMY_DATABASE_URL)
    acquired = await connection.fetchval(
        "SELECT pg_try_advisory_lock($1)",
        EMBEDDED_CONSUMERS_LOCK_KEY,
    )
    if acquired:
        embedded_consumers_lock = connection
        return True

    await connection.close()
    return False


async def _release_embedded_consumers_lock() -> None:
    global embedded_consumers_lock

    if embedded_consumers_lock is None:
        return

    try:
        await embedded_consumers_lock.execute(
            "SELECT pg_advisory_unlock($1)",
            EMBEDDED_CONSUMERS_LOCK_KEY,
        )
    finally:
        await embedded_consumers_lock.close()
        embedded_consumers_lock = None


@app.middleware("http")
async def metrics_and_events_middleware(request: Request, call_next):
    from api.observability.router import (
        REQUEST_COUNT,
        REQUEST_DURATION,
        REQUESTS_IN_PROGRESS,
    )

    # Пропускаем тяжёлые пути без записи в events
    skip_event = request.url.path in {
        "/health",
        "/docs",
        "/openapi.json",
        "/metrics",
        "/api/v1/loggerinfo",
        "/api/v1/loggerinfo/metrics",
    }

    handler = request.url.path.split("?")[0]
    method = request.method

    REQUESTS_IN_PROGRESS.labels(method=method, handler=handler).inc()
    start_time = time.time()

    try:
        response = await call_next(request)
        duration = time.time() - start_time

        REQUEST_COUNT.labels(
            method=method, handler=handler, status=response.status_code
        ).inc()
        REQUEST_DURATION.labels(method=method, handler=handler).observe(duration)

        if not skip_event:
            asyncio.create_task(
                _write_event_safe(request, response.status_code, duration)
            )

        return response
    finally:
        REQUESTS_IN_PROGRESS.labels(method=method, handler=handler).dec()


@app.on_event("startup")
async def startup():
    global embedded_consumers_owner, notification_consumer_task

    rabbit_factory = RabbitFactory(
        settings=RabbitMqSettings(
            rabbitmq_host=os.getenv("RABBITMQ_HOST"),
            rabbitmq_user=os.getenv("RABBITMQ_USER"),
            rabbitmq_pass=os.getenv("RABBITMQ_PASS"),
            rabbitmq_port=os.getenv("RABBITMQ_PORT"),
            rabbitmq_vhost=os.getenv("RABBITMQ_VHOST"),
        )
    )

    s3_factory = S3ServiceFactory(
        s3_settings=S3SettingsModel(
            aws_access_key_id=os.getenv("S3_ACCESS"),
            aws_secret_access_key=os.getenv("S3_SECRET"),
            endpoint_url=os.getenv("S3_URL"),
        )
    )
    redis_uri = get_redis_uri()
    try:
        r = aioredis.from_url(redis_uri, socket_connect_timeout=3)
        await r.ping()
        print(f"Redis connected successfully: {redis_uri}")
        await r.close()
    except Exception as e:
        print(f"Redis connection failed: {e}")
        import traceback

        traceback.print_exc()

    ioc.set(IRabbitFactory, await rabbit_factory())
    ioc.set(IS3ServiceFactory, s3_factory)

    ioc.set(IBookingEventsRepository, BookingEventsRepository())

    ioc.set(IBookingRepository, BookingRepository())

    ioc.set(IBookingNomenclatureRepository, BookingNomenclatureRepository())

    ioc.set(IWidgetInstallerRepository, WidgetInstallerRepository())
    ioc.set(IYookassaOauthRepository, YookassaOauthRepository())
    ioc.set(IYookassaRequestRepository, YookassaRequestRepository())
    ioc.set(IYookassaPaymentsRepository, YookassaPaymentsRepository())
    ioc.set(IYookassaCrmPaymentsRepository, YookassaCrmPaymentsRepository())

    ioc.set(INomenclatureReader, NomenclatureReader())
    ioc.set(INomenclatureGroupsReader, NomenclatureGroupsReader())
    ioc.set(IAddNomenclatureToGroupFunction, AddNomenclatureToGroupFunction())
    ioc.set(ICreateNomenclatureGroupFunction, CreateNomenclatureGroupFunction())
    ioc.set(IDeleteNomenclatureGroupFunction, DeleteNomenclatureGroupFunction())
    ioc.set(IPatchNomenclatureGroupFunction, PatchNomenclatureGroupFunction())
    ioc.set(IDelNomenclatureFromGroupFunction, DelNomenclatureFromGroupFunction())

    ioc.set(
        IInsertNomenclatureAttributesFunction, InsertNomenclatureAttributesFunction()
    )
    ioc.set(INomenclatureAttributesReader, NomenclatureAttributesReader())
    ioc.set(
        IDeleteNomenclatureAttributesFunction, DeleteNomenclatureAttributesFunction()
    )
    ioc.set(IChangeMainNomenclGroupFunction, ChangeMainNomenclGroupFunction())

    InstallCategoriesWeb()(app=app)
    InstallNomenclatureWeb()(app=app)
    ioc.set(IYookasssaAmoTableCrmRepository, YookasssaAmoTableCrmRepository())
    ioc.set(IYookassaTableNomenclature, YookassaTableNomenclature())

    InstallBookingRepeatWeb()(app=app)
    InstallBookingEventsWeb()(app=app)
    InstallWidgetInstallerInfoWeb()(app=app)
    InstallYookassaOauthWeb()(app=app)
    InstallNomenclatureGroupsWeb()(app=app)
    InstallNomenclatureAttributesWeb()(app=app)
    InstallManufacturersWeb()(app=app)
    InstallDocsSalesWeb()(app=app)
    InstallContragentsWeb()(app=app)
    InstallLoyalityTransactionsWeb()(app=app)

    init_db()
    await database.connect()

    embedded_consumers_owner = await _acquire_embedded_consumers_lock()
    if embedded_consumers_owner:
        logger.info("Acquired embedded consumers lock; starting AMQP consumers once")

        if os.getenv("ENABLE_AVITO_ENV_INIT", "false").lower() == "true":
            try:
                from api.chats.avito.avito_init import init_avito_credentials

                await init_avito_credentials()
            except Exception:
                pass

        try:
            await chat_consumer.start()
        except Exception:
            import traceback

            traceback.print_exc()

        try:
            await avito_consumer.start()
        except Exception:
            import traceback

            traceback.print_exc()

        try:
            # Запускаем notification_consumer в фоновой задаче
            from notification_consumer import consume

            notification_consumer_task = asyncio.create_task(consume())
        except Exception:
            import traceback

            traceback.print_exc()

        try:
            from segments.amqp_messaging.consumers import segment_consumer

            await segment_consumer.start()
        except Exception:
            import traceback

            traceback.print_exc()
    else:
        logger.info(
            "Embedded consumers lock is already held; skipping duplicate AMQP consumers"
        )

    try:
        await DefaultImagesUploader().upload_all()
    except Exception as e:
        pass


@app.on_event("shutdown")
async def shutdown():
    global embedded_consumers_owner, notification_consumer_task

    if embedded_consumers_owner:
        await chat_consumer.stop()
        await avito_consumer.stop()

        try:
            from segments.amqp_messaging.consumers import segment_consumer

            await segment_consumer.stop()
        except Exception:
            pass

        if notification_consumer_task:
            notification_consumer_task.cancel()
            try:
                await notification_consumer_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            finally:
                notification_consumer_task = None

        await _release_embedded_consumers_lock()
        embedded_consumers_owner = False

    try:
        rabbit_factory = ioc.get(IRabbitFactory)
        if hasattr(rabbit_factory, "close"):
            await rabbit_factory.close()
    except Exception:
        pass

    try:
        if scheduler.running:
            scheduler.shutdown()
    except Exception as e:
        pass

    try:
        await database.disconnect()
    except Exception:
        pass
