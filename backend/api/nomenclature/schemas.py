import re
from enum import Enum
from typing import List, Optional

from api.nomenclature.video.schemas import NomenclatureVideoRead
from api.prices.schemas import PriceGetWithNomenclature
from api.warehouse_balances.schemas import WarehouseWithNomenclature
from database.db import NomenclatureCashbackType
from pydantic import BaseModel, Field, validator


class NomenclatureType(str, Enum):
    product = "product"
    service = "service"
    offer = "offer"
    resourse = "resourse"
    rental = "rental"
    property = "property"
    work = "work"


class NomenclatureBarcodeCreate(BaseModel):
    barcode: str


class NomenclatureCreate(BaseModel):
    name: str
    type: Optional[NomenclatureType]
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[str]
    unit: Optional[int]
    category: Optional[int]
    manufacturer: Optional[int]
    global_category_id: Optional[int] = None
    chatting_percent: Optional[int] = Field(
        default=None,
        le=100,
        gt=0,
        description="Комиссия маркета (округление кратно 4%)",
    )
    cashback_type: Optional[NomenclatureCashbackType] = (
        NomenclatureCashbackType.lcard_cashback
    )
    cashback_value: Optional[int] = 0

    external_id: Optional[str]
    tags: Optional[List[str]] = []
    seo_title: Optional[str]
    seo_description: Optional[str]
    seo_keywords: Optional[List[str]] = []

    production_time_min_from: Optional[int] = Field(
        None, description="Срок производства от (в минутах)", ge=0
    )
    production_time_min_to: Optional[int] = Field(
        None, description="Срок производства до (в минутах)", ge=0
    )
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    video_link: Optional[str] = None

    class Config:
        orm_mode = True

    @validator("tags")
    def validate_tags(cls, tag_list):
        if tag_list is None:
            return []
        if len(tag_list) > 10:
            raise ValueError("Максимум 10 тегов")

        if len(set(tag_list)) < len(tag_list):
            raise ValueError("Теги не должны повторяться")

        pattern = re.compile(r"^[a-zA-Zа-яА-Я0-9_-]{2,20}$")
        for tag in tag_list:
            if not pattern.match(tag):
                raise ValueError(
                    f"Тег '{tag}' содержит недопустимые символы или некорректную длину (2–20 символов)"
                )

        return tag_list

    @validator("production_time_min_to")
    def validate_production_time_range(cls, v, values):
        """Проверяем, что 'до' не меньше 'от' если оба указаны"""
        if v is not None and "production_time_min_from" in values:
            from_val = values.get("production_time_min_from")
            if from_val is not None and v < from_val:
                raise ValueError(
                    "'production_time_min_to' не может быть меньше 'production_time_min_from'"
                )
        return v

    @validator("chatting_percent")
    def validate_chatting_percent(cls, v):
        """Округляет процент комиссии маркета кратно 4"""
        if v is None:
            return v
        # Округляем до ближайшего кратного 4
        # Если v=5, то (5 + 2) // 4 * 4 = 7 // 4 * 4 = 1 * 4 = 4
        # Если v=6, то (6 + 2) // 4 * 4 = 8 // 4 * 4 = 2 * 4 = 8
        # Если v=7, то (7 + 2) // 4 * 4 = 9 // 4 * 4 = 2 * 4 = 8
        # Если v=8, то (8 + 2) // 4 * 4 = 10 // 4 * 4 = 2 * 4 = 8
        rounded = ((v + 2) // 4) * 4
        # Ограничиваем максимум 100
        if rounded > 100:
            rounded = 100
        # Минимум 4, если значение было больше 0
        if rounded < 4 and v > 0:
            rounded = 4
        return rounded


class NomenclatureCreateMass(BaseModel):
    __root__: List[NomenclatureCreate]

    class Config:
        orm_mode = True


class NomenclatureEdit(NomenclatureCreate):
    name: Optional[str]


class NomenclatureEditMass(NomenclatureEdit):
    id: int


class Nomenclature(NomenclatureCreate):
    id: int
    updated_at: int
    created_at: int
    qr_hash: Optional[str] = None
    video_link: Optional[str] = None
    rating: Optional[int] = None

    class Config:
        orm_mode = True


class NomenclatureAttributeValue(BaseModel):
    id: int
    attribute_id: int
    name: str
    alias: Optional[str]
    value: str

    class Config:
        orm_mode = True


class NomenclatureGet(NomenclatureCreate):
    id: int
    unit_name: Optional[str]
    barcodes: Optional[List[str]]
    prices: Optional[List[PriceGetWithNomenclature]]
    balances: Optional[List[WarehouseWithNomenclature]]
    attributes: Optional[List[NomenclatureAttributeValue]] = None
    photos: Optional[List[dict]] = None
    videos: Optional[List[NomenclatureVideoRead]] = None
    group_id: Optional[int]
    group_name: Optional[str]
    is_main: Optional[bool]
    chatting_percent: Optional[int] = Field(None, gt=0, le=100)
    qr_hash: Optional[str] = None
    qr_url: Optional[str] = None
    updated_at: int
    created_at: int

    class Config:
        orm_mode = True


class NomenclatureList(BaseModel):
    __root__: Optional[List[Nomenclature]]

    class Config:
        orm_mode = True


class NomenclatureListGet(BaseModel):
    __root__: Optional[List[NomenclatureGet]]

    class Config:
        orm_mode = True


class NomenclatureListGetRes(BaseModel):
    result: Optional[List[NomenclatureGet]]
    count: int


class NomenclaturesListPatch(BaseModel):
    idx: int
    old_barcode: Optional[str]
    new_barcode: str
