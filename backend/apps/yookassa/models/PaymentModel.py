from datetime import datetime
from enum import Enum
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, Field, validator


class EventWebhookPayment(str, Enum):
    pending = "pending"
    succeeded = "payment.succeeded"
    waiting_for_capture = "payment.waiting_for_capture"
    canceled = "payment.canceled"


class AmountModel(BaseModel):
    value: str
    currency: Literal["RUB"]


class CustomerModel(BaseModel):
    full_name: str = None
    inn: str = None
    email: str = None
    phone: str = None


class MarkQuantityModel(BaseModel):
    numerator: int
    denominator: int


class ItemModel(BaseModel):
    description: str
    amount: AmountModel
    vat_code: int = Field(ge=1, le=10, description="Код НДС ЮKassa 1–6")
    quantity: float
    measure: Optional[str] = None
    mark_quantity: Optional[MarkQuantityModel] = None
    payment_subject: Optional[str] = None
    payment_mode: Optional[str] = None

    @validator("vat_code", pre=True)
    def vat_code_to_int(cls, v):
        if v is None:
            return 1
        return int(v) if isinstance(v, str) else v


class ItemModelView(ItemModel):
    id: int = None


class ReceiptModel(BaseModel):
    type: Optional[str] = None
    tax_system: Optional[str] = None
    customer: CustomerModel = None
    items: List[ItemModel]


class ReceiptModelView(BaseModel):
    customer: CustomerModel = None
    items: List[ItemModelView]


class RecipientModel(BaseModel):
    account_id: str
    gateway_id: str


class InvoiceDetails(BaseModel):
    id: str = None


class CardBank(BaseModel):
    number: str
    expiry_year: str
    expiry_month: str


class MethodBankCard(BaseModel):
    type: Literal["bank_card"]
    card: CardBank = None


class MethodSbp(BaseModel):
    type: Literal["sbp"]


class ConfirmationRedirect(BaseModel):
    type: Literal["redirect"]
    return_url: str


class ConfirmationEmbedded(BaseModel):
    type: Literal["embedded"] = None


class ConfirmationRedirectResponce(BaseModel):
    type: Literal["redirect"] = None
    confirmation_url: str = None


class PaymentCreateModel(BaseModel):
    amount: AmountModel = None
    description: str = None
    receipt: Optional[ReceiptModel] = None
    tax_system_code: int = None
    capture: bool = None
    merchant_customer_id: str = None
    payment_method_data: Union[MethodBankCard, MethodSbp] = None
    test: bool = True
    confirmation: Union[
        ConfirmationRedirect, ConfirmationEmbedded, ConfirmationRedirectResponce
    ] = None


class PaymentCreateModelView(PaymentCreateModel):
    receipt: Optional[ReceiptModelView] = None


class PaymentBaseModel(PaymentCreateModel):
    id: str = None
    status: str = None
    amount: AmountModel = None
    income_amount: AmountModel = None
    description: str = None
    recipient: RecipientModel = None
    captured_at: datetime = None
    created_at: datetime = None
    expires_at: datetime = None
    test: bool = None
    refundable: bool = None
    receipt_registration: str = None
    merchant_customer_id: str = None
    invoice_details: InvoiceDetails = None
    payment_crm_id: Optional[int] = None


class PaymentWebhookEventModel(BaseModel):
    type: str
    event: EventWebhookPayment
    object: PaymentBaseModel
