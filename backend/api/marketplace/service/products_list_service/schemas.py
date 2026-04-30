from datetime import datetime
from enum import Enum
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class AvailableWarehouse(BaseModel):
    warehouse_id: int
    organization_id: int
    warehouse_name: str
    warehouse_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_to_client: Optional[float] = None
    current_amount: Optional[float] = None

    class Config:
        orm_mode = True


class MarketplaceProductUserAdmin(BaseModel):
    recipient_id: str
    username: Optional[str] = None

    class Config:
        orm_mode = True


class MarketplaceProduct(BaseModel):
    """Модель товара для маркетплейса"""

    id: int
    name: str
    description_short: Optional[str] = None
    description_long: Optional[str] = None
    code: Optional[str] = None
    unit_name: Optional[str] = None
    cashbox_id: int
    category_name: Optional[str] = None
    manufacturer_name: Optional[str] = None
    price: float
    price_type: str
    price_address: Optional[str] = None
    price_latitude: Optional[float] = None
    price_longitude: Optional[float] = None
    created_at: datetime
    updated_at: datetime
    images: Optional[List[str]] = None
    videos: List[dict] = []
    barcodes: Optional[List[str]] = None
    type: Optional[str] = None

    distance: Optional[float] = None

    # Новые поля для расширенной функциональности
    listing_pos: Optional[int] = None  # Позиция в выдаче для аналитики
    listing_page: Optional[int] = None
    is_ad_pos: Optional[bool] = False  # Рекламное размещение

    tags: Optional[List[str]] = None  # Теги товара
    variations: Optional[List[dict]] = None  # Вариации товара
    current_amount: Optional[float] = None  # Остатки

    seller_name: Optional[str] = None  # Имя селлера
    seller_photo: Optional[str] = None  # Фото селлера
    seller_description: Optional[str] = None  # Описание селлера
    user_admin: Optional[List[MarketplaceProductUserAdmin]] = (
        None  # Главный админ кассы
    )

    total_sold: Optional[int] = None

    rating: Optional[float] = None  # Рейтинг отзывов 1-5
    global_rating: Optional[int] = (
        None  # Глобальный рейтинг товара (nomenclature.rating)
    )
    reviews_count: Optional[int] = None  # Количество отзывов

    button_text: Optional[str] = None
    button_logic: Optional[str] = None

    available_warehouses: Optional[List[AvailableWarehouse]] = None

    production_time_min_from: Optional[int] = None
    production_time_min_to: Optional[int] = None

    class Config:
        orm_mode = True


class MarketplaceProductAttribute(BaseModel):
    """Атрибуты товара"""

    name: str
    value: str


class MarketplaceProductDetail(MarketplaceProduct):
    """Дополненная модель товара для маркетплейса"""

    seo_title: Optional[str] = None
    seo_description: Optional[str] = None
    seo_keywords: Optional[List[str]] = None
    attributes: Optional[List[MarketplaceProductAttribute]] = None
    nomenclatures: Optional[List[dict]] = None
    processing_time_ms: Optional[int] = None

    class Config:
        orm_mode = True


class MarketplaceProductList(BaseModel):
    result: List[MarketplaceProduct]
    count: int
    page: int
    size: int
    processing_time_ms: Optional[int] = None
    detected_city: Optional[str] = None
    detected_lat: Optional[float] = None
    detected_lon: Optional[float] = None
    sellers: Optional[List[dict]] = None


class MarketplaceSort(Enum):
    distance = "distance"
    price = "price"
    name = "name"
    rating = "rating"
    global_rating = "global_rating"
    total_sold = "total_sold"
    created_at = "created_at"
    updated_at = "updated_at"
    seller = "seller"


class MarketplaceProductsRequest(BaseModel):
    phone: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    page: int = 1
    size: int = Field(default=10, le=100)
    sort_by: Optional[MarketplaceSort] = None
    sort_order: Optional[Literal["asc", "desc"]] = "desc"
    category: Optional[str] = None
    manufacturer: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    # tags: Optional[str] = None
    in_stock: Optional[bool] = None

    rating_from: Optional[int] = None
    rating_to: Optional[int] = None
    global_rating_from: Optional[int] = None
    global_rating_to: Optional[int] = None

    seller_name: Optional[str] = None
    seller_id: Optional[int] = None
    seller_phone: Optional[str] = None

    city: Optional[str] = None  # Город для приоритетной сортировки товаров
    address: Optional[str] = None  # Адрес для геокодирования и выбора ближайшей цены
    apply_radius_filter: bool = True

    name: Optional[str] = None
    description_long: Optional[str] = None
    id: Optional[str] = None
    seo_title: Optional[str] = None
    seo_description: Optional[str] = None
    seo_keywords: Optional[str] = None
    nomenclature_attributes: Optional[str] = None
    global_category_id: Optional[int] = None


product_buttons_text: dict[str, dict[str, str]] = {
    "product": {
        "name": "В корзину",
        "logic": "Добавить выбранный товар в корзину пользователя для последующей покупки.",
    },
    "rent": {
        "name": "Забронировать",
        "logic": "Открыть форму бронирования аренды с выбором даты и условий.",
    },
    "service": {
        "name": "В корзину",
        "logic": "Добавить выбранную услугу в корзину для оформления заказа.",
    },
    "offer": {
        "name": "Подробнее",
        "logic": "Показать детальную информацию о спецпредложении или акции.",
    },
    "resurs": {
        "name": "Арендовать",
        "logic": "Начать процесс аренды ресурса с выбором периода и условий.",
    },
    "property": {
        "name": "Оставить заявку",
        "logic": "Открыть форму подачи заявки на недвижимость или объект.",
    },
    "work": {
        "name": "Подробнее",
        "logic": "Показать расширенную информацию о работе или вакансии.",
    },
}
