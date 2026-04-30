from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class LocationCalc(BaseModel):
    code: Optional[int] = None
    fias_guid: Optional[str] = None
    postal_code: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    country_code: Optional[str] = None
    region: Optional[str] = None
    sub_region: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None


class LkLoginRequest(BaseModel):
    login: str
    password: str
    rememberMe: bool = False


class LkTokenRequest(BaseModel):
    token: str


class PackageCalc(BaseModel):
    weight: int
    length: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None


class AdditionalService(BaseModel):
    code: str
    parameter: Optional[Any] = None


class CalculateRequest(BaseModel):
    from_location: LocationCalc
    to_location: LocationCalc
    packages: List[PackageCalc]
    tariff_code: Optional[int] = None
    date: Optional[datetime] = None
    type: int = 1
    currency: Optional[int] = None
    lang: Optional[str] = Field(None, regex="^(rus|eng|zho)$")
    additional_order_types: Optional[List[int]] = None
    services: Optional[List[AdditionalService]] = None


class CalculateWithServicesRequest(CalculateRequest):
    services: Optional[List[dict]] = None


class CalculateResponse(BaseModel):
    delivery_sum: float
    period_min: Optional[int] = None
    period_max: Optional[int] = None
    calendar_min: Optional[int] = None
    calendar_max: Optional[int] = None
    weight_calc: Optional[int] = None
    total_sum: float
    currency: str
    services: Optional[List[dict]] = None
    errors: List[Any] = []
    warnings: List[Any] = []
    delivery_date_range: Optional[dict] = None


class Contact(BaseModel):
    name: str
    company: Optional[str] = None
    email: Optional[str] = None
    phones: List[dict]


class LocationOrder(BaseModel):
    address: str
    code: Optional[int] = None
    fias_guid: Optional[str] = None
    postal_code: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    country_code: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None


class UpdateOrderRequest(BaseModel):
    type: Optional[int] = None
    number: Optional[str] = None
    accompanying_number: Optional[str] = None
    tariff_code: Optional[int] = None
    comment: Optional[str] = None
    shipment_point: Optional[str] = None
    delivery_point: Optional[str] = None
    delivery_recipient_cost: Optional[Dict[str, Any]] = None
    delivery_recipient_cost_adv: Optional[List[Dict[str, Any]]] = None
    sender: Optional[Dict[str, Any]] = None
    seller: Optional[Dict[str, Any]] = None
    recipient: Optional[Dict[str, Any]] = None
    from_location: Optional[Dict[str, Any]] = None
    to_location: Optional[Dict[str, Any]] = None
    services: Optional[List[Dict[str, Any]]] = None
    packages: Optional[List[Dict[str, Any]]] = None


class Item(BaseModel):
    name: str
    ware_key: str
    payment: dict
    cost: float
    weight: int
    amount: int


class PackageOrder(BaseModel):
    number: str
    weight: int
    length: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    comment: Optional[str] = None
    items: Optional[List[Item]] = None


class OrderRequest(BaseModel):
    tariff_code: int
    recipient: Contact
    from_location: Optional[LocationOrder] = None
    to_location: Optional[LocationOrder] = None
    shipment_point: Optional[str] = None
    delivery_point: Optional[str] = None
    packages: List[PackageOrder]
    number: Optional[str] = None
    comment: Optional[str] = None
    sender: Optional[Contact] = None
    seller: Optional[dict] = None
    services: Optional[List[dict]] = None
    type: int = 1
    doc_sales_id: Optional[int] = None


class OrderResponse(BaseModel):
    entity: dict
    requests: List[dict]
    related_entities: List[dict]


class ConnectRequest(BaseModel):
    account: str
    secure_password: str


class DeliveryPointFilter(BaseModel):
    city_code: Optional[int] = None
    postal_code: Optional[str] = None
    type: Optional[str] = None
    have_cashless: Optional[bool] = None
    allowed_cod: Optional[bool] = None


class IntegrationStatus(BaseModel):
    isAuth: bool
    integration_status: bool


class CdekTemplate(BaseModel):
    order_type: Optional[str] = None

    sender_company: Optional[str] = None
    sender_fio: Optional[str] = None
    sender_phone: Optional[str] = None
    sender_city_data: Optional[Dict[str, Any]] = None
    sender_inn: Optional[str] = None

    im_number: Optional[str] = None
    seller_name: Optional[str] = None
    seller_ownership_form: Optional[int] = None
    seller_inn: Optional[str] = None
    seller_phone: Optional[str] = None
    recipient_legal: Optional[bool] = None
    recipient_company: Optional[str] = None
    recipient_inn: Optional[str] = None
    recipient_fio: Optional[str] = None
    recipient_phone: Optional[str] = None
    recipient_city_data: Optional[Dict[str, Any]] = None
    recipient_email: Optional[str] = None

    pay_delivery_recipient: Optional[bool] = None
    pay_bank_commission: Optional[bool] = None

    pickup_mode: Optional[str] = None
    delivery_mode: Optional[str] = None

    pickup_address: Optional[str] = None
    pickup_address_data: Optional[Dict[str, Any]] = None
    pickup_date: Optional[str] = None
    pickup_time_from: Optional[str] = None
    pickup_time_to: Optional[str] = None
    pickup_break_from: Optional[str] = None
    pickup_break_to: Optional[str] = None
    pickup_need_authority: Optional[bool] = None
    pickup_need_passport: Optional[bool] = None
    pickup_comment: Optional[str] = None

    pickup_pvz_data: Optional[Dict[str, Any]] = None
    delivery_address: Optional[str] = None
    delivery_address_data: Optional[Dict[str, Any]] = None
    delivery_comment: Optional[str] = None

    delivery_point_data: Optional[Dict[str, Any]] = None


class AddressSuggestionRequest(BaseModel):
    value: str
    cityUuid: str
    limit: Optional[int] = 10


class LinkSalesRequest(BaseModel):
    doc_sales_id: int


class PrintOrderDto(BaseModel):
    order_uuid: Optional[str] = None
    cdek_number: Optional[str] = None


class WaybillRequest(BaseModel):
    orders: List[PrintOrderDto]
    copy_count: Optional[int] = 2
    type: Optional[
        Literal[
            "tpl_china",
            "tpl_armenia",
            "tpl_russia",
            "tpl_english",
            "tpl_italian",
            "tpl_korean",
            "tpl_latvian",
            "tpl_lithuanian",
            "tpl_german",
            "tpl_turkish",
            "tpl_czech",
            "tpl_thailand",
            "tpl_invoice",
        ]
    ] = None


class BarcodeRequest(BaseModel):
    orders: List[PrintOrderDto]
    copy_count: Optional[int] = 1
    format: Optional[Literal["A4", "A5", "A6", "A7"]] = "A4"
    lang: Optional[Literal["RUS", "ENG"]] = "RUS"


class ClientOrderItem(BaseModel):
    name: str
    ware_key: str
    cost: float
    weight: int
    amount: int


class ClientCalculateRequest(BaseModel):
    items: List[ClientOrderItem]
    delivery_type: Literal["courier", "pvz", "postamat"]
    address: Optional[str] = None
    delivery_point: Optional[str] = None
    recipient_city_code: Optional[int] = None


class ClientOrderRequest(ClientCalculateRequest):
    recipient_name: str
    recipient_phone: str
    recipient_email: Optional[str] = None
    tariff_code: Optional[int] = None
    doc_sales_id: Optional[int] = None
