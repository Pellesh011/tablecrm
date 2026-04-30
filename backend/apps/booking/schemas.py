from typing import Any, List, Mapping, Optional

from api.nomenclature.schemas import NomenclatureCreate
from database.db import BookingEventStatus, BookingStatus, DocSalesStatus, Tariff
from pydantic import BaseModel, Field


class NomenclatureBookingCreate(BaseModel):
    nomenclature_id: int
    tariff: Optional[Tariff] = None


class NomenclatureBookingEditView(NomenclatureBookingCreate):
    is_deleted: bool = None


class NomenclatureBookingEdit(NomenclatureBookingEditView):
    id: int


class NomenclatureBookingPatch(NomenclatureCreate):
    id: Optional[int] = None
    is_deleted: Optional[bool] = None
    nomenclature_id: Optional[int] = None
    tariff: Optional[Tariff] = None


class BookingView(BaseModel):
    title: Optional[str] = None
    contragent: Optional[int] = None
    contragent_accept: Optional[int] = None
    address: Optional[str] = None
    date_booking: Optional[int] = None
    start_booking: Optional[int] = None
    end_booking: Optional[int] = None
    booking_user_id: Optional[int] = None
    booking_driver_id: Optional[int] = None
    docs_sales_id: Optional[int] = None
    status_doc_sales: Optional[DocSalesStatus] = None
    status_booking: Optional[BookingStatus] = None
    comment: Optional[str] = None
    is_deleted: Optional[bool] = None
    sale_payload: Optional[dict] = None

    class Config:
        orm_mode = True


class BookingEdit(BookingView):
    id: Optional[int]

    class Config:
        orm_mode = True


class BookingEditGoods(BookingEdit):
    goods: Optional[List[NomenclatureBookingEditView]]


class BookingEditList(BaseModel):
    __root__: Optional[List[BookingEditGoods]]


class BookingCreate(BookingView):
    tags: Optional[str]
    goods: List[NomenclatureBookingCreate]


class BookingCreateList(BaseModel):
    __root__: Optional[List[BookingCreate]]


class Booking(BookingView):
    id: int
    is_deleted: Optional[bool] = None
    goods: Optional[List[NomenclatureBookingPatch]]
    tags: Optional[List[str]] = None
    contragent_name: Optional[str] = None


class BookingList(BaseModel):
    __root__: Optional[List[Booking]]


class ResponseCreate(BaseModel):
    data: List[BookingCreate]
    errors: List[Mapping[str, Any]]


class BookingEventCreate(BaseModel):
    booking_nomenclature_id: Optional[int]
    type: Optional[BookingEventStatus] = None
    value: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    is_deleted: Optional[bool] = False


class BookingEventView(BookingEventCreate):
    id: int


class BookingEventPatch(BookingEventCreate):
    id: int


class BookingEventViewList(BaseModel):
    __root__: Optional[List[BookingEventView]]


class BookingEventCreateList(BaseModel):
    __root__: Optional[List[BookingEventCreate]]


class BookingEventPathList(BaseModel):
    __root__: Optional[List[BookingEventPatch]]


class ResponseBookingEventCreateList(BaseModel):
    status: str
    data: Optional[List[BookingEventCreate]]


class BookingFiltersList(BaseModel):
    title: Optional[str] = Field(default=None)
    # good_category: Optional[int] = None
    contragent: Optional[int] = None
    start_booking: Optional[int] = None
    end_booking: Optional[int] = None
    status_doc_sales: Optional[DocSalesStatus] = None
    status_booking: Optional[BookingStatus] = None
    tags: Optional[str] = None
