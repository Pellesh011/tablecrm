from typing import Optional

from pydantic import BaseModel, Field, validator


class TransactionCreate(BaseModel):
    amount: float = Field(..., gt=0, description="Amount to deposit (must be >= 1)")
    tariff_id: Optional[int] = Field(
        None,
        description="Tariff ID (optional, will use current tariff if not provided)",
    )

    @validator("amount")
    def validate_amount(cls, v):
        if v < 1:
            raise ValueError("Amount must be at least 1 ruble")
        return v


class TransactionResponse(BaseModel):
    id: int
    cashbox: int
    tariff: int
    users: Optional[int]
    amount: float
    status: str
    type: str
    is_manual_deposit: bool
    created_at: int
    updated_at: int

    class Config:
        orm_mode = True


class PaymentCreateRequest(BaseModel):
    amount: float = Field(..., gt=0, description="Amount to pay")
    tariff_id: Optional[int] = Field(None, description="Tariff ID (optional)")

    @validator("amount")
    def validate_amount(cls, v):
        if v < 1:
            raise ValueError("Amount must be at least 1 ruble")
        return v


class PaymentCreateResponse(BaseModel):
    payment_id: str
    payment_url: str
    status: str
    message: str


class TinkoffCallbackData(BaseModel):
    TerminalKey: str
    OrderId: str
    Success: bool
    Status: str
    PaymentId: Optional[str] = None
    Amount: Optional[int] = None
    ErrorCode: Optional[str] = None
    Message: Optional[str] = None
