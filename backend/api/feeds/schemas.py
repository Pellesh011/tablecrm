import re
from typing import Any, Dict, List, Optional, Union

from fastapi import HTTPException
from pydantic import BaseModel, Field, constr, validator

XML_TAG_RE = re.compile(r"^[A-Za-z_:][A-Za-z0-9._:\-]*$")

XML_TAG = constr(regex=r"^[A-Za-z_:][A-Za-z0-9._:\-]*$")

ALLOWED_DB_FIELDS = [
    "name",
    "description",
    "category",
    "price",
    "current_amount",
    "images",
    "params",
]


def is_valid_xml_tag(tag: str) -> bool:
    if not isinstance(tag, str) or not tag:
        return False
    if tag.lower().startswith("xml"):
        return False
    return bool(XML_TAG_RE.match(tag))


class PricesFeed(BaseModel):
    from_: float = Field(..., alias="from")
    to_: float = Field(..., alias="to")


class CriteriaFeed(BaseModel):
    warehouse_id: Optional[List[int]]
    only_on_stock: Optional[bool]
    prices: Optional[PricesFeed]
    category_id: Optional[List[int]]
    price_types_id: Optional[int]
    tags: Optional[List[str]]
    tech_cards: Optional[str]
    exclude_nomenclature_ids: Optional[List[int]]
    exclude_component_ids: Optional[List[int]]

    @validator("tags", pre=True)
    def normalize_tags(cls, v):
        """Нормализует список тегов: строка -> список, чистит пустые значения."""
        if v is None:
            return None
        if isinstance(v, str):
            v = [item.strip() for item in v.split(",")]
        if isinstance(v, list):
            cleaned = [
                item.strip() for item in v if isinstance(item, str) and item.strip()
            ]
            return cleaned
        raise HTTPException(status_code=400, detail="Неправильный формат тегов")

    @validator("exclude_nomenclature_ids", pre=True)
    def normalize_exclude_nomenclature_ids(cls, v):
        """Нормализуем список номенклатур, которые не попадут в фид"""
        if v is None:
            return None
        if isinstance(v, str):
            v = [item.strip() for item in v.split(",")]
        if isinstance(v, list):
            cleaned = []
            for item in v:
                if isinstance(item, int):
                    cleaned.append(item)
                elif isinstance(item, str) and item.strip().isdigit():
                    cleaned.append(int(item.strip()))
            return cleaned or None
        raise HTTPException(
            status_code=400, detail="Неправильный формат exclude_nomenclature_ids"
        )

    @validator("exclude_component_ids", pre=True)
    def normalize_exclude_component_ids(cls, v):
        """Нормализуем список компонентов, которые исключаются из пересчета техкарт"""
        if v is None:
            return None
        if isinstance(v, str):
            v = [item.strip() for item in v.split(",")]
        if isinstance(v, list):
            cleaned = []
            for item in v:
                if isinstance(item, int):
                    cleaned.append(item)
                elif isinstance(item, str) and item.strip().isdigit():
                    cleaned.append(int(item.strip()))
            return cleaned or None
        raise HTTPException(
            status_code=400, detail="Неправильный формат exclude_component_ids"
        )


class FeedCreate(BaseModel):
    name: str
    description: Optional[str]
    root_tag: XML_TAG
    item_tag: XML_TAG
    field_tags: Dict[str, str]
    criteria: Optional[CriteriaFeed]
    # Поля для синхронизации с Tilda
    tilda_url: Optional[str] = Field(
        None,
        description="URL для отправки в Tilda (например, https://store.tilda.ru/connectors/commerceml/)",
    )
    tilda_username: Optional[str] = Field(
        None, description="Имя пользователя для Basic Auth в Tilda"
    )
    tilda_password: Optional[str] = Field(
        None, description="Пароль для Basic Auth в Tilda"
    )
    tilda_sync_enabled: Optional[bool] = Field(
        False, description="Включить автоматическую синхронизацию с Tilda"
    )
    tilda_sync_interval: Optional[int] = Field(
        None,
        description="Интервал синхронизации в минутах (например, 60 для раз в час)",
    )
    tilda_price_id: Optional[str] = Field(
        None,
        description="ID цены в системе учета Tilda (например, cbcf493b-55bc-11d9-848a-00112f43529a)",
    )
    tilda_discount_price_id: Optional[str] = Field(
        None, description="ID цены со скидкой в системе учета Tilda"
    )
    tilda_catalog_id: Optional[str] = Field(
        None,
        description="ID каталога в системе учета Tilda (например, tablecrm-catalog)",
    )
    tilda_warehouse_id: Optional[Union[str, List[str]]] = Field(
        None,
        description="ID склада(ов) в системе учета Tilda. Может быть строкой (один склад) или массивом строк (несколько складов). Пусто = все склады",
    )

    @validator("tilda_warehouse_id", pre=True)
    def normalize_warehouse_id(cls, v):
        """Нормализует tilda_warehouse_id: пустые строки и пустые массивы -> None"""
        if v is None:
            return None
        if isinstance(v, str):
            return v if v.strip() else None
        if isinstance(v, list):
            # Фильтруем пустые строки
            filtered = [item for item in v if isinstance(item, str) and item.strip()]
            return filtered if filtered else None
        return v

    @validator("field_tags")
    def validate_field_tags(cls, values: Dict[str, str]) -> Dict[str, str]:
        for xml_tag, value in values.items():
            if not is_valid_xml_tag(xml_tag):
                raise HTTPException(
                    status_code=400, detail=f"Invalid xml tag: {xml_tag}"
                )
            if value not in ALLOWED_DB_FIELDS:
                raise HTTPException(
                    status_code=400, detail=f"Field not allowed: {value}"
                )
        return values


class Feed(BaseModel):
    id: int
    name: str
    description: Optional[str]
    root_tag: str
    item_tag: str
    field_tags: Dict[str, str]
    criteria: Optional[Dict[str, Any]]
    url_token: str
    # Поля для синхронизации с Tilda
    tilda_url: Optional[str] = None
    tilda_username: Optional[str] = None
    tilda_password: Optional[str] = None
    tilda_sync_enabled: Optional[bool] = False
    tilda_sync_interval: Optional[int] = None
    tilda_price_id: Optional[str] = None
    tilda_discount_price_id: Optional[str] = None
    tilda_catalog_id: Optional[str] = None
    tilda_warehouse_id: Optional[Union[str, List[str]]] = None


class GetFeeds(BaseModel):
    count: int
    feeds: List[Feed]


class FeedUpdate(FeedCreate):
    name: Optional[str]
    root_tag: Optional[XML_TAG]
    item_tag: Optional[XML_TAG]
    field_tags: Optional[Dict[str, str]]
    # Поля для синхронизации с Tilda (все опциональны при обновлении)
    tilda_url: Optional[str] = None
    tilda_username: Optional[str] = None
    tilda_password: Optional[str] = None
    tilda_sync_enabled: Optional[bool] = None
    tilda_sync_interval: Optional[int] = None
    tilda_price_id: Optional[str] = None
    tilda_discount_price_id: Optional[str] = None
    tilda_catalog_id: Optional[str] = None
    tilda_warehouse_id: Optional[Union[str, List[str]]] = None


class TildaSync(BaseModel):
    """Схема для отправки фида в Tilda"""

    tilda_url: Optional[str] = Field(
        None,
        description="URL для отправки в Tilda (https://store.tilda.ru/connectors/commerceml/). Если не указан, берется из настроек фида.",
    )
    username: Optional[str] = Field(
        None,
        description="Имя пользователя для Basic Auth. Если не указано, берется из настроек фида.",
    )
    password: Optional[str] = Field(
        None,
        description="Пароль для Basic Auth. Если не указан, берется из настроек фида.",
    )
    mode: Optional[str] = Field(
        "import",
        description="Режим работы CommerceML (import, checkauth, init, file, query)",
    )
    type: Optional[str] = Field(
        "catalog", description="Тип данных CommerceML (catalog, offers)"
    )
    filename: Optional[str] = Field("import.xml", description="Имя файла для загрузки")
