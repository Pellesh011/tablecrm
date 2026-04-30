from typing import List, Optional

from pydantic import BaseModel, validator


class PriceCreate(BaseModel):
    price: float
    nomenclature: int
    price_type: Optional[int]
    date_from: Optional[int]
    date_to: Optional[int]
    warehouse_id: Optional[int] = None
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius: Optional[float] = None
    hide_outside_radius: bool = False

    @validator("latitude", "longitude", pre=True)
    def convert_to_float(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, str):
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return v

    class Config:
        orm_mode = True


class PriceCreateMass(BaseModel):
    __root__: List[PriceCreate]

    class Config:
        orm_mode = True


class PriceEdit(BaseModel):
    id: int
    price: Optional[float]
    nomenclature: Optional[int]
    price_type: Optional[int]
    date_from: Optional[int]
    date_to: Optional[int]
    warehouse_id: Optional[int] = None
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius: Optional[float] = None
    hide_outside_radius: bool = None

    @validator("latitude", "longitude", pre=True)
    def convert_to_float(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, str):
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return v


class PriceEditOne(BaseModel):
    price: Optional[float]
    price_type: Optional[int]
    date_from: Optional[int]
    date_to: Optional[int]
    warehouse_id: Optional[int] = None
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    radius: Optional[float] = None
    hide_outside_radius: bool = None

    @validator("latitude", "longitude", pre=True)
    def convert_to_float(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, str):
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return v

    @validator("price_type", pre=True)
    def convert_price_type_to_int(cls, v):
        if v is None or v == "":
            return None
        if isinstance(v, str):
            # Если это строка, которая является числом - преобразуем
            try:
                return int(v)
            except (ValueError, TypeError):
                # Если это не число (например, название типа цены) - игнорируем
                # Роутер уже обработает это и удалит из price_values
                return None
        return v


class PriceInList(BaseModel):
    id: int
    nomenclature_id: int
    nomenclature_name: str
    type: Optional[str]
    warehouse_id: Optional[int] = None
    # Адрес и координаты цены (чтобы фронт мог их отображать в таблице)
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description_short: Optional[str]
    description_long: Optional[str]
    code: Optional[str]
    unit: Optional[int]
    unit_name: Optional[str]
    category: Optional[int]
    category_name: Optional[str]
    manufacturer: Optional[int]
    manufacturer_name: Optional[str]
    price: float
    price_type: Optional[str]
    date_from: Optional[int]
    date_to: Optional[int]
    radius: Optional[float] = None
    hide_outside_radius: bool = None
    photos: Optional[List[dict]] = []

    class Config:
        orm_mode = True


class Price(PriceInList):
    updated_at: int
    created_at: int


class PriceList(BaseModel):
    __root__: Optional[List[PriceInList]]

    class Config:
        orm_mode = True


class PriceListGet(BaseModel):
    result: Optional[List[PriceInList]]
    count: int


class PricePure(BaseModel):
    id: int
    price_type: Optional[int]
    price: float
    nomenclature: Optional[int]
    date_from: Optional[int]
    date_to: Optional[int]
    warehouse_id: Optional[int] = None
    updated_at: int
    created_at: int


class PriceListPure(BaseModel):
    __root__: Optional[List[PricePure]]

    class Config:
        orm_mode = True


class PriceGetWithNomenclature(BaseModel):
    price: Optional[float]
    price_type: Optional[str]


class FilterSchema(BaseModel):
    price_type_id: Optional[int]
