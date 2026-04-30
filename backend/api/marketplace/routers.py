import logging
import time
from datetime import datetime
from typing import Literal, Optional, Union
from urllib.parse import quote

logger = logging.getLogger(__name__)

from api.marketplace.service.favorites_service.schemas import (
    CreateFavoritesUtm,
    FavoriteGroupedListResponse,
    FavoriteListResponse,
    FavoriteRequest,
    FavoriteResponse,
    FavoritesFilters,
    FavoritesGroupBy,
    FavoritesSortBy,
)
from api.marketplace.service.locations_service.schemas import (
    LocationsListRequest,
    LocationsListResponse,
)
from api.marketplace.service.orders_service.schemas import (
    CreateOrderUtm,
    LastDeliveryAddressResponse,
    MarketplaceOrderRequest,
    MarketplaceOrderResponse,
    OrderItemResponse,
    OrderListResponse,
    OrderSortBy,
    OrderStatusLabel,
)
from api.marketplace.service.product_cart_service.schemas import (
    MarketplaceAddToCartRequest,
    MarketplaceCartResponse,
    MarketplaceGetCartRequest,
    MarketplaceRemoveFromCartRequest,
)
from api.marketplace.service.products_list_service.schemas import (
    MarketplaceProductDetail,
    MarketplaceProductList,
    MarketplaceProductsRequest,
)
from api.marketplace.service.public_categories.schema import (
    GlobalCategoryCreate,
    GlobalCategoryList,
    GlobalCategoryTree,
    GlobalCategoryTreeList,
    GlobalCategoryUpdate,
    TreeSelectNodeList,
)
from api.marketplace.service.qr_service.schemas import QRResolveResponse
from api.marketplace.service.review_service.schemas import (
    CreateReviewRequest,
    MarketplaceReview,
    ReviewListRequest,
    ReviewListResponse,
    UpdateReviewRequest,
)
from api.marketplace.service.seller_service.schemas import (
    SellerResponse,
    SellerUpdateRequest,
)
from api.marketplace.service.seller_statistics_service.schemas import (
    SellersListResponse,
    SellerStatisticsResponse,
)
from api.marketplace.service.service import MarketplaceService
from api.marketplace.service.view_event_service.schemas import (
    CreateViewEventRequest,
    CreateViewEventResponse,
    GetViewEventsRequest,
    ViewEventsUtm,
)
from api.marketplace.utils import get_marketplace_service
from common.geocoders.instance import geocoder
from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    Query,
    Request,
    Response,
    UploadFile,
    status,
)

router = APIRouter(prefix="/mp", tags=["marketplace"])


def get_client_ip(request: Request) -> Optional[str]:
    """Получение IP адреса клиента из заголовков"""
    # Проверяем X-Vercel-Forwarded-For (для Vercel)
    vercel_forwarded = request.headers.get("X-Vercel-Forwarded-For")
    if vercel_forwarded:
        # X-Vercel-Forwarded-For может содержать несколько IP через запятую
        # Берем первый (оригинальный IP клиента)
        return vercel_forwarded.split(",")[0].strip()

    # Проверяем X-Forwarded-For (для прокси/nginx/Vercel)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # X-Forwarded-For может содержать несколько IP через запятую
        # Берем первый (оригинальный IP клиента)
        return forwarded_for.split(",")[0].strip()

    # Проверяем X-Real-IP (для nginx)
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()

    # Проверяем CF-Connecting-IP (для Cloudflare, если используется)
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip.strip()

    # Если заголовков нет, берем из request.client
    if request.client:
        return request.client.host

    return None


def get_create_order_utm(
    entity_type: str = Query("docs_sales"),
    utm_source: Optional[str] = Query(None),
    utm_medium: Optional[str] = Query(None),
    utm_campaign: Optional[str] = Query(None),
    utm_term: Optional[str] = Query(None),
    utm_content: Optional[str] = Query(None),
    utm_name: Optional[str] = Query(None),
    utm_phone: Optional[str] = Query(None),
    utm_email: Optional[str] = Query(None),
    utm_leadid: Optional[str] = Query(None),
    utm_yclientid: Optional[str] = Query(None),
    utm_gaclientid: Optional[str] = Query(None),
    ref_user: Optional[str] = Query(None),
) -> CreateOrderUtm:
    """Зависимость для парсинга UTM параметров из query string

    Все параметры опциональны. Передаются через query string, например:
    /api/v1/mp/orders?entity_type=docs_sales&utm_source=google&utm_medium=cpc
    """
    from api.marketplace.schemas import UtmEntityType

    utm_term_list = None
    if utm_term:
        # Парсим utm_term как список через запятую: "term1,term2,term3"
        utm_term_list = [t.strip() for t in utm_term.split(",") if t.strip()]

    try:
        entity_type_enum = UtmEntityType(entity_type)
    except ValueError:
        # Если передан неверный entity_type, используем значение по умолчанию
        entity_type_enum = UtmEntityType.docs_sales

    return CreateOrderUtm(
        entity_type=entity_type_enum,
        utm_source=utm_source,
        utm_medium=utm_medium,
        utm_campaign=utm_campaign,
        utm_term=utm_term_list,
        utm_content=utm_content,
        utm_name=utm_name,
        utm_phone=utm_phone,
        utm_email=utm_email,
        utm_leadid=utm_leadid,
        utm_yclientid=utm_yclientid,
        utm_gaclientid=utm_gaclientid,
        ref_user=ref_user,
    )


def get_create_favorites_utm(
    utm_source: Optional[str] = Query(None),
    utm_medium: Optional[str] = Query(None),
    utm_campaign: Optional[str] = Query(None),
    utm_term: Optional[str] = Query(None),
    utm_content: Optional[str] = Query(None),
    utm_name: Optional[str] = Query(None),
    utm_phone: Optional[str] = Query(None),
    utm_email: Optional[str] = Query(None),
    utm_leadid: Optional[str] = Query(None),
    utm_yclientid: Optional[str] = Query(None),
    utm_gaclientid: Optional[str] = Query(None),
    ref_user: Optional[str] = Query(None),
) -> CreateFavoritesUtm:
    """Зависимость для парсинга UTM параметров из query string для избранного

    Все параметры опциональны. Передаются через query string, например:
    /api/v1/mp/favorites?utm_source=google&utm_medium=cpc&utm_term=keyword1,keyword2
    """
    utm_term_list = None
    if utm_term:
        # Парсим utm_term как список через запятую: "term1,term2,term3"
        utm_term_list = [t.strip() for t in utm_term.split(",") if t.strip()]

    return CreateFavoritesUtm(
        utm_source=utm_source,
        utm_medium=utm_medium,
        utm_campaign=utm_campaign,
        utm_term=utm_term_list,
        utm_content=utm_content,
        utm_name=utm_name,
        utm_phone=utm_phone,
        utm_email=utm_email,
        utm_leadid=utm_leadid,
        utm_yclientid=utm_yclientid,
        utm_gaclientid=utm_gaclientid,
        ref_user=ref_user,
    )


def get_view_events_utm(
    utm_source: Optional[str] = Query(None),
    utm_medium: Optional[str] = Query(None),
    utm_campaign: Optional[str] = Query(None),
    utm_term: Optional[str] = Query(None),
    utm_content: Optional[str] = Query(None),
    utm_name: Optional[str] = Query(None),
    utm_phone: Optional[str] = Query(None),
    utm_email: Optional[str] = Query(None),
    utm_leadid: Optional[str] = Query(None),
    utm_yclientid: Optional[str] = Query(None),
    utm_gaclientid: Optional[str] = Query(None),
    ref_user: Optional[str] = Query(None),
) -> ViewEventsUtm:
    """Зависимость для парсинга UTM параметров для view events из query string

    Все параметры опциональны. Передаются через query string, например:
    /api/v1/mp/events/view?utm_source=google&utm_medium=cpc
    """
    from api.marketplace.schemas import UtmEntityType

    utm_term_list = None
    if utm_term:
        # Парсим utm_term как список через запятую: "term1,term2,term3"
        utm_term_list = [t.strip() for t in utm_term.split(",") if t.strip()]

    return ViewEventsUtm(
        entity_type=UtmEntityType.view_events,
        utm_source=utm_source,
        utm_medium=utm_medium,
        utm_campaign=utm_campaign,
        utm_term=utm_term_list,
        utm_content=utm_content,
        utm_name=utm_name,
        utm_phone=utm_phone,
        utm_email=utm_email,
        utm_leadid=utm_leadid,
        utm_yclientid=utm_yclientid,
        utm_gaclientid=utm_gaclientid,
        ref_user=ref_user,
    )


@router.get("/products/{product_id}", response_model=MarketplaceProductDetail)
async def get_marketplace_product(
    product_id: int,
    http_request: Request,
    response: Response,
    lat: Optional[float] = Query(None, description="Широта клиента"),
    lon: Optional[float] = Query(None, description="Долгота клиента"),
    address: Optional[str] = Query(
        None, description="Адрес клиента для геокодирования"
    ),
    city: Optional[str] = Query(
        None, description="Город клиента (для обратной совместимости)"
    ),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Получить один товар маркетплейса с SEO, атрибутами и остатками по складам

    Если город не задан, автоматически определяется по IP адресу клиента.
    """
    # Если город не задан, пытаемся определить по IP
    resolved_city = city
    resolved_lat = lat
    resolved_lon = lon
    detected_city = None
    detected_lat = None
    detected_lon = None

    if not city and not address:
        client_ip = get_client_ip(http_request)
        if client_ip:
            try:
                location = await geocoder.get_location_by_ip(client_ip)
                if location and location.city:
                    resolved_city = location.city
                    detected_city = location.city
                    # Если координат нет, но есть из IP - используем их
                    if not resolved_lat and location.latitude:
                        resolved_lat = location.latitude
                        detected_lat = location.latitude
                    if not resolved_lon and location.longitude:
                        resolved_lon = location.longitude
                        detected_lon = location.longitude
            except Exception:
                # Если не удалось определить по IP - игнорируем ошибку
                pass

    start = time.perf_counter()
    product = await service.get_product(
        product_id,
        lat=resolved_lat,
        lon=resolved_lon,
        address=address,
        city=resolved_city,
    )
    end_ms = int((time.perf_counter() - start) * 1000)

    result = product.copy(update={"processing_time_ms": end_ms})

    # Добавляем заголовки с информацией об автоматически определенном городе
    # URL-encode для поддержки кириллицы в заголовках
    if detected_city:
        response.headers["X-Detected-City"] = quote(detected_city, safe="")
    if detected_lat is not None:
        response.headers["X-Detected-Lat"] = str(detected_lat)
    if detected_lon is not None:
        response.headers["X-Detected-Lon"] = str(detected_lon)

    return result


@router.get("/products", response_model=MarketplaceProductList)
async def get_marketplace_products(
    http_request: Request,
    response: Response,
    request: MarketplaceProductsRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Получение всех публичных товаров маркетплейса

    Фильтрует только товары с:
    - price_type = 'chatting'

    Если город не задан, автоматически определяется по IP адресу клиента.
    """
    # Если город не задан, пытаемся определить по IP
    detected_city = None
    detected_lat = None
    detected_lon = None

    # Проверяем, что city и address действительно отсутствуют (None или пустая строка)
    has_city = request.city and request.city.strip()
    has_address = request.address and request.address.strip()

    # Используем print для гарантированного вывода в логи
    print(
        f"[AUTO-CITY] Checking auto-detection. city={request.city}, address={request.address}, has_city={has_city}, has_address={has_address}"
    )
    logger.info(
        f"[AUTO-CITY] Checking auto-detection. city={request.city}, address={request.address}, has_city={has_city}, has_address={has_address}"
    )

    if not has_city and not has_address:
        # Логируем все заголовки для отладки
        headers_debug = {
            "X-Vercel-Forwarded-For": http_request.headers.get(
                "X-Vercel-Forwarded-For"
            ),
            "X-Forwarded-For": http_request.headers.get("X-Forwarded-For"),
            "X-Real-IP": http_request.headers.get("X-Real-IP"),
            "CF-Connecting-IP": http_request.headers.get("CF-Connecting-IP"),
            "request.client.host": (
                http_request.client.host if http_request.client else None
            ),
        }
        print(f"[AUTO-CITY] Headers debug: {headers_debug}")
        logger.info(f"[AUTO-CITY] Headers debug: {headers_debug}")

        client_ip = get_client_ip(http_request)
        print(f"[AUTO-CITY] Request without city/address. Client IP: {client_ip}")
        logger.info(f"[AUTO-CITY] Request without city/address. Client IP: {client_ip}")
        if client_ip:
            try:
                print(f"[AUTO-CITY] Trying to get location for IP: {client_ip}")
                logger.info(f"[AUTO-CITY] Trying to get location for IP: {client_ip}")
                location = await geocoder.get_location_by_ip(client_ip)
                print(f"[AUTO-CITY] Location result: {location}")
                logger.info(f"[AUTO-CITY] Location result: {location}")
                if location and location.city:
                    # Используем город из IP, если он определен
                    request.city = location.city
                    detected_city = location.city
                    # Если координат нет, но есть из IP - используем их
                    if not request.lat and location.latitude:
                        request.lat = location.latitude
                        detected_lat = location.latitude
                    if not request.lon and location.longitude:
                        request.lon = location.longitude
                        detected_lon = location.longitude
                    print(
                        f"[AUTO-CITY] Detected city: {detected_city}, lat: {detected_lat}, lon: {detected_lon}"
                    )
                    logger.info(
                        f"[AUTO-CITY] Detected city: {detected_city}, lat: {detected_lat}, lon: {detected_lon}"
                    )
                else:
                    print(f"[AUTO-CITY] Location or city is None for IP: {client_ip}")
                    logger.warning(
                        f"[AUTO-CITY] Location or city is None for IP: {client_ip}"
                    )
            except Exception as e:
                # Если не удалось определить по IP - логируем ошибку
                logger.error(
                    f"[AUTO-CITY] Error getting location by IP {client_ip}: {e}",
                    exc_info=True,
                )
        else:
            print("[AUTO-CITY] Client IP is None, cannot determine city")
            logger.warning("[AUTO-CITY] Client IP is None, cannot determine city")

    start = time.perf_counter()
    products = await service.get_products(request)
    end_ms = int((time.perf_counter() - start) * 1000)

    # Логируем поисковые запросы в бд
    await service.log_search(
        request=request,
        result_count=products.count,
    )

    # Добавляем информацию об автоматически определенном городе в тело ответа
    # Это работает без изменения конфигурации nginx
    result = products.copy(
        update={
            "processing_time_ms": end_ms,
            "detected_city": detected_city,
            "detected_lat": detected_lat,
            "detected_lon": detected_lon,
        }
    )

    # Также добавляем заголовки для совместимости (если nginx настроен)
    if detected_city:
        response.headers["X-Detected-City"] = quote(detected_city, safe="")
    if detected_lat is not None:
        response.headers["X-Detected-Lat"] = str(detected_lat)
    if detected_lon is not None:
        response.headers["X-Detected-Lon"] = str(detected_lon)

    return result


@router.get("/locations", response_model=LocationsListResponse)
async def get_marketplace_locations(
    request: LocationsListRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Получить список публичных локаций
    """
    return await service.get_locations(request)


@router.get(
    "/orders/last-delivery-address",
    response_model=LastDeliveryAddressResponse,
    summary="Последний адрес доставки клиента",
)
async def get_last_delivery_address(
    phone: str = Query(..., description="Телефон клиента в любом формате"),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.get_last_delivery_address(phone)


@router.get(
    "/orders",
    response_model=OrderListResponse,
    summary="Список заказов клиента по телефону",
)
async def get_marketplace_orders(
    phone: str = Query(..., description="Телефон клиента"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    status_filter: Optional[OrderStatusLabel] = Query(
        None, alias="status", description="Фильтр по статусу"
    ),
    sort_by: OrderSortBy = Query(
        OrderSortBy.created_at,
        description="Поле сортировки: created_at, updated_at, delivery_date",
    ),
    sort_order: Literal["asc", "desc"] = Query(
        "desc", description="Порядок сортировки: asc или desc"
    ),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.get_orders_by_phone(
        phone, page, size, status_filter, sort_by, sort_order
    )


@router.get(
    "/orders/{order_id}",
    response_model=OrderItemResponse,
    summary="Получить заказ по ID",
)
async def get_marketplace_order_by_id(
    order_id: int,
    phone: str = Query(..., description="Телефон клиента для проверки владельца"),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.get_order_by_id(order_id, phone)


@router.post("/orders", response_model=MarketplaceOrderResponse)
async def create_marketplace_order(
    order_request: MarketplaceOrderRequest = Body(..., embed=False),
    utm: CreateOrderUtm = Depends(get_create_order_utm),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Создать заказ маркетплейса с автоматическим распределением по кешбоксам
    """
    # Логируем UTM параметры для отладки
    print(f"[UTM DEBUG] UTM parameters received: {utm.dict() if utm else 'None'}")
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"UTM parameters received: {utm.dict() if utm else 'None'}")

    start = time.perf_counter()
    order = await service.create_order(order_request, utm)
    end_ms = int((time.perf_counter() - start) * 1000)

    return order.copy(update={"processing_time_ms": end_ms})


@router.get("/qr/{qr_hash}", response_model=QRResolveResponse)
async def resolve_qr_code(
    qr_hash: str, service: MarketplaceService = Depends(get_marketplace_service)
):
    """
    Получить товар или локацию по QR-коду (MD5 хэш)
    """
    return await service.resolve_qr(qr_hash)


@router.post("/reviews", response_model=MarketplaceReview)
async def create_review(
    review_request: CreateReviewRequest,
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.create_review(review_request)


@router.get("/reviews", response_model=ReviewListResponse)
async def get_reviews(
    request: ReviewListRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.get_reviews(request)


@router.patch("/reviews/{review_id}", response_model=MarketplaceReview)
async def update_review(
    review_id: int,
    request: UpdateReviewRequest,
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.update_review(review_id, request)


@router.post("/favorites", response_model=FavoriteResponse)
async def add_to_favorites(
    favorite_request: FavoriteRequest,
    utm: CreateFavoritesUtm = Depends(get_create_favorites_utm),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Добавить товар или локацию в избранное

    UTM параметры могут быть переданы:
    - через query string: /favorites?utm_term=keyword1,keyword2&ref_user=123
    - через body: {"utm_term": "keyword1,keyword2", "ref_user": "123"}

    Если UTM параметры переданы в body, они имеют приоритет над query string.
    """
    # Если UTM параметры переданы в body, используем их
    if favorite_request.utm_term or favorite_request.ref_user:
        utm_term_list = None
        if favorite_request.utm_term:
            # Парсим utm_term как список через запятую: "term1,term2,term3"
            utm_term_list = [
                t.strip() for t in favorite_request.utm_term.split(",") if t.strip()
            ]

        # Создаем новый UTM объект с параметрами из body
        utm = CreateFavoritesUtm(
            utm_source=utm.utm_source,
            utm_medium=utm.utm_medium,
            utm_campaign=utm.utm_campaign,
            utm_term=utm_term_list if utm_term_list else utm.utm_term,
            utm_content=utm.utm_content,
            utm_name=utm.utm_name,
            utm_phone=utm.utm_phone,
            utm_email=utm.utm_email,
            utm_leadid=utm.utm_leadid,
            utm_yclientid=utm.utm_yclientid,
            utm_gaclientid=utm.utm_gaclientid,
            ref_user=(
                favorite_request.ref_user if favorite_request.ref_user else utm.ref_user
            ),
        )

    return await service.add_to_favorites(favorite_request, utm)


@router.delete("/favorites/{favorite_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_from_favorites(
    favorite_id: int,
    phone: str = Query(..., description="Номер телефона"),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Удалить элемент из избранного
    """
    await service.remove_from_favorites(favorite_id, phone)


@router.get("/favorites")
async def get_favorites(
    phone: str = Query(..., description="Номер телефона"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(20, ge=1, le=100, description="Размер страницы"),
    sort_by: Optional[FavoritesSortBy] = Query(
        None,
        description="Сортировка: name, description, favorite_created_at, product_created_at, seller, price",
    ),
    sort_order: Literal["asc", "desc"] = Query(
        "desc", description="Порядок: asc или desc"
    ),
    group_by: Optional[FavoritesGroupBy] = Query(
        None, description="Группировка: favorite_created_at, product_created_at, seller"
    ),
    name: Optional[str] = Query(None, description="Фильтр: поиск по имени товара"),
    description: Optional[str] = Query(None, description="Фильтр: поиск по описанию"),
    favorite_created_at_from: Optional[datetime] = Query(
        None, description="Фильтр: дата добавления в избранное (от)"
    ),
    favorite_created_at_to: Optional[datetime] = Query(
        None, description="Фильтр: дата добавления в избранное (до)"
    ),
    product_created_at_from: Optional[datetime] = Query(
        None, description="Фильтр: дата создания товара (от)"
    ),
    product_created_at_to: Optional[datetime] = Query(
        None, description="Фильтр: дата создания товара (до)"
    ),
    seller_id: Optional[int] = Query(None, description="Фильтр: ID селлера"),
    seller_name: Optional[str] = Query(
        None, description="Фильтр: поиск по имени селлера"
    ),
    min_price: Optional[float] = Query(None, description="Фильтр: минимальная цена"),
    max_price: Optional[float] = Query(None, description="Фильтр: максимальная цена"),
    service: MarketplaceService = Depends(get_marketplace_service),
) -> Union[FavoriteListResponse, FavoriteGroupedListResponse]:
    """
    Получить список избранного пользователя.

    - **sort_by**: сортировка (name, description, favorite_created_at, product_created_at, seller, price)
    - **group_by**: группировка (favorite_created_at, product_created_at, seller)
    - Фильтры: name, description, даты, seller_id, seller_name, min_price, max_price
    """
    filters = None
    if any(
        [
            name,
            description,
            favorite_created_at_from,
            favorite_created_at_to,
            product_created_at_from,
            product_created_at_to,
            seller_id,
            seller_name,
            min_price is not None,
            max_price is not None,
        ]
    ):
        filters = FavoritesFilters(
            name=name,
            description=description,
            favorite_created_at_from=favorite_created_at_from,
            favorite_created_at_to=favorite_created_at_to,
            product_created_at_from=product_created_at_from,
            product_created_at_to=product_created_at_to,
            seller_id=seller_id,
            seller_name=seller_name,
            min_price=min_price,
            max_price=max_price,
        )

    return await service.get_favorites(
        contragent_phone=phone,
        page=page,
        size=size,
        sort_by=sort_by,
        sort_order=sort_order,
        group_by=group_by,
        filters=filters,
    )


@router.get("/events/view")
async def get_view_events_info(
    request: GetViewEventsRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """Информация о событиях просмотра"""
    return await service.get_view_events(request)


@router.post("/events/view", response_model=CreateViewEventResponse)
async def create_view_event(
    request: CreateViewEventRequest = Body(..., embed=False),
    utm: ViewEventsUtm = Depends(get_view_events_utm),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """Создание события просмотра товара"""
    return await service.create_view_event(request, utm)


@router.post("/cart/add", response_model=MarketplaceCartResponse)
async def add_to_cart(
    request: MarketplaceAddToCartRequest,
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Добавить товар в корзину покупок

    Если корзина не существует, она будет создана автоматически.
    Если товар уже есть в корзине, количество будет увеличено.
    """
    return await service.add_to_cart(request)


@router.get("/cart", response_model=MarketplaceCartResponse)
async def get_cart(
    request: MarketplaceGetCartRequest = Depends(),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Получить содержимое корзины покупок для указанного номера телефона
    """
    return await service.get_cart(request)


@router.delete("/cart/remove", response_model=MarketplaceCartResponse)
async def remove_from_cart(
    request: MarketplaceRemoveFromCartRequest,
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Удалить товар из корзины покупок

    Если warehouse_id не указан, будет удален товар без привязки к складу.
    Если указан - будет удален товар конкретного склада.
    """

    return await service.remove_from_cart(request)


@router.get("/categories/", response_model=GlobalCategoryList)
async def get_global_categories(
    limit: int = 100,
    offset: int = 0,
    only_with_products: bool = Query(
        False, description="Показывать только категории с актуальными товарами"
    ),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    start = time.perf_counter()
    data = await service.get_global_categories(
        limit=limit, offset=offset, only_with_products=only_with_products
    )
    end_ms = int((time.perf_counter() - start) * 1000)

    return GlobalCategoryList(**data, processing_time_ms=end_ms)


@router.get("/categories/tree/", response_model=GlobalCategoryTreeList)
async def get_global_categories_tree(
    only_with_products: bool = Query(
        False, description="Показывать только категории с актуальными товарами"
    ),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    start = time.perf_counter()
    data = await service.get_global_categories_tree(
        only_with_products=only_with_products
    )
    end_ms = int((time.perf_counter() - start) * 1000)

    return GlobalCategoryTreeList(**data, processing_time_ms=end_ms)


@router.get("/categories/tree-select/")
async def get_global_categories_tree_select(
    only_with_products: bool = Query(
        False, description="Показывать только категории с актуальными товарами"
    ),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Возвращает дерево категорий в формате для Ant Design TreeSelect.
    Формат: [{ title: string, value: number, children: [...] }]
    """
    start = time.perf_counter()
    data = await service.get_global_categories_tree_for_select(
        only_with_products=only_with_products
    )
    end_ms = int((time.perf_counter() - start) * 1000)

    return TreeSelectNodeList(**data, processing_time_ms=end_ms)


@router.get("/categories/{category_id}/", response_model=GlobalCategoryTree)
async def get_global_category(
    category_id: int, service: MarketplaceService = Depends(get_marketplace_service)
):
    start = time.perf_counter()
    data = await service.get_global_category(category_id)
    end_ms = int((time.perf_counter() - start) * 1000)

    return GlobalCategoryTree(**data, processing_time_ms=end_ms)


@router.post("/categories/", response_model=GlobalCategoryTree, status_code=201)
async def create_global_category(
    category: GlobalCategoryCreate,
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.create_global_category(category)


@router.patch("/categories/{category_id}/", response_model=GlobalCategoryTree)
async def update_global_category(
    category_id: int,
    category_update: GlobalCategoryUpdate,
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.update_global_category(category_id, category_update)


@router.delete("/categories/{category_id}/")
async def delete_global_category(
    category_id: int, service: MarketplaceService = Depends(get_marketplace_service)
):
    return await service.delete_global_category(category_id)


@router.post("/categories/{category_id}/upload_image/")
async def upload_category_image(
    category_id: int,
    file: UploadFile = File(...),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    return await service.upload_category_image(category_id, file)


@router.patch("/sellers/profile/", response_model=SellerResponse)
async def update_seller_profile(
    token: str = Query(...),
    name: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    file: UploadFile = File(None),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    request = SellerUpdateRequest(
        name=name,
        description=description,
    )

    return await service.update_seller_profile(payload=request, file=file, token=token)


@router.get("/sellers/statistics", response_model=SellerStatisticsResponse)
async def get_sellers_statistics(
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Получить статистику по актуальным селлерам
    """
    return await service.get_sellers_statistics()


@router.get("/sellers/", response_model=SellersListResponse)
async def get_sellers_list(
    search: Optional[str] = Query(
        None,
        description="Поиск по имени продавца",
    ),
    limit: int = Query(100, ge=1, le=500, description="Максимальное кол-во продавцов"),
    service: MarketplaceService = Depends(get_marketplace_service),
):
    """
    Облегчённый список активных продавцов маркетплейса.

    Используется для фильтра продавцов на фронтенде без загрузки полной статистики.
    """
    return await service.get_sellers_list(search=search, limit=limit)
