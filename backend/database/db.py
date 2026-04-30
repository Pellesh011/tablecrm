import datetime
import enum
import os
from enum import Enum as ENUM

import databases
import sqlalchemy
from database.enums import (
    ContragentType,
    DebitCreditType,
    Gender,
    Repeatability,
    TriggerTime,
    TriggerType,
)
from dotenv import load_dotenv
from sqlalchemy import (
    ARRAY,
    BIGINT,
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import func

load_dotenv()


class OperationType(str, ENUM):
    plus = "+"
    minus = "-"


class Operation(str, ENUM):
    incoming = "Приход"
    outgoing = "Расход"
    transfer = "Перемещение"


class CostType(str, ENUM):
    per_user = "per_user"
    per_account = "per_account"


class Trial(ENUM):
    secon = "secon"
    link: str


class Contragent_types(str, ENUM):
    Supplier = "Поставщик"
    Buyer = "Покупатель"


class InstalledByRole(str, ENUM):
    Partner = "Партнёр"
    Client = "Клиент"


class TypeCustomField(str, ENUM):
    Contact = "Контакт"
    Lead = "Сделка"


class BookingStatus(str, ENUM):
    new = "Новый"
    confirmed = "Подтвержден"
    paid = "Оплачен"
    taken = "Забран"
    delivered = "Доставлен"
    uploaded = "Выгружен"
    extended = "Пролонгирован"
    completed = "Завершен"


class DocSalesStatus(str, ENUM):
    partially_paid = "Частично оплачен"
    not_paid = "Не оплачен"
    paid = "Оплачен"


class Tariff(str, ENUM):
    month = "Месяц"
    day = "День"
    week = "Неделя"


class BookingEventStatus(str, ENUM):
    give = "Забрал"
    take = "Привез"


class TgBillStatus(str, ENUM):
    NEW = "NEW"
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
    APPROVED = "APPROVED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    REQUESTED = "REQUESTED"
    PAID = "PAID"
    ERROR = "ERROR"


class TgBillApproveStatus(str, ENUM):
    NEW = "NEW"
    APPROVED = "APPROVED"
    CANCELED = "CANCELED"


class NomenclatureCashbackType(str, ENUM):
    percent = "percent"
    const = "const"
    no_cashback = "no_cashback"
    lcard_cashback = "lcard_cashback"


class OrderStatus(str, ENUM):
    received = "received"
    processed = "processed"
    collecting = "collecting"
    collected = "collected"
    picked = "picked"
    delivered = "delivered"
    closed = "closed"
    success = "success"


class SegmentStatus(str, ENUM):
    created = "created"
    in_process = "in_process"
    calculated = "calculated"


class ChannelType(str, ENUM):
    AVITO = "AVITO"
    WHATSAPP = "WHATSAPP"
    TELEGRAM = "TELEGRAM"
    MAX = "MAX"


class CustomerSource(str, ENUM):
    AVITO = "AVITO"
    WHATSAPP = "WHATSAPP"
    TELEGRAM = "TELEGRAM"
    MAX = "MAX"


class ChatStatus(str, ENUM):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    ARCHIVED = "ARCHIVED"


class MessageType(str, ENUM):
    TEXT = "TEXT"
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"
    DOCUMENT = "DOCUMENT"
    VOICE = "VOICE"
    SYSTEM = "SYSTEM"


class MessageStatus(str, ENUM):
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    READ = "READ"
    FAILED = "FAILED"


class MessageSenderType(str, ENUM):
    CLIENT = "CLIENT"
    OPERATOR = "OPERATOR"


metadata = sqlalchemy.MetaData()
Base = declarative_base(metadata=metadata)

from api.tech_cards.models import (  # noqa: F401
    TechCardDB,
    TechCardItemDB,
    TechCardOutputItemDB,
)
from api.tech_operations.models import (  # noqa: F401
    TechOperationComponentDB,
    TechOperationDB,
    TechOperationPaymentDB,
)

cashbox_settings = sqlalchemy.Table(
    "cashbox_settings",
    metadata,
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column(
        "require_photo_for_writeoff",
        Boolean,
        nullable=False,
        server_default=sqlalchemy.false(),
    ),
    sqlalchemy.Column(
        "check_nomenclature_duplicates_by_name",
        Boolean,
        nullable=False,
        server_default=sqlalchemy.false(),
    ),
    sqlalchemy.Column(
        "check_nomenclature_duplicates_by_code",
        Boolean,
        nullable=False,
        server_default=sqlalchemy.false(),
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column(
        "is_deleted", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
)

yookassa_install = sqlalchemy.Table(
    "yookassa_install",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True, autoincrement=True),
    sqlalchemy.Column("warehouse_id", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("access_token", String),
    sqlalchemy.Column("expires_in", String),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("is_deleted", Boolean),
)

yookassa_payments = sqlalchemy.Table(
    "yookassa_payments",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True, autoincrement=True),
    sqlalchemy.Column("payment_crm_id", Integer, ForeignKey("payments.id")),
    sqlalchemy.Column("payment_id", String),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("amount_value", Float),
    sqlalchemy.Column("amount_currency", String),
    sqlalchemy.Column("income_amount_value", Float),
    sqlalchemy.Column("income_amount_currency", String),
    sqlalchemy.Column("confirmation_url", String),
    sqlalchemy.Column("payment_capture", Boolean),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

booking_tags = sqlalchemy.Table(
    "booking_tags",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True, autoincrement=True),
    sqlalchemy.Column("booking_id", Integer, ForeignKey("bookikng.id")),
    sqlalchemy.Column("name", String, nullable=False),
)

booking = sqlalchemy.Table(
    "bookikng",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("title", String),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("contragent_accept", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("address", String),
    sqlalchemy.Column("date_booking", Integer, index=True),
    sqlalchemy.Column("start_booking", Integer, index=True),
    sqlalchemy.Column("end_booking", Integer, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column(
        "docs_sales_id", Integer, ForeignKey("docs_sales.id"), nullable=True
    ),
    sqlalchemy.Column("booking_user_id", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("booking_driver_id", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("status_doc_sales", Enum(DocSalesStatus), nullable=False),
    sqlalchemy.Column("status_booking", Enum(BookingStatus), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("comment", String),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("sale_payload", JSON),
)

booking_nomenclature = sqlalchemy.Table(
    "booking_nomenclature",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("booking_id", Integer, ForeignKey("bookikng.id")),
    sqlalchemy.Column("nomenclature_id", Integer, ForeignKey("nomenclature.id")),
    sqlalchemy.Column("tariff", Enum(Tariff), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("is_deleted", Boolean),
)

booking_events = sqlalchemy.Table(
    "booking_events",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "booking_nomenclature_id", Integer, ForeignKey("booking_nomenclature.id")
    ),
    sqlalchemy.Column("type", Enum(BookingEventStatus), index=True, nullable=False),
    sqlalchemy.Column("value", String),
    sqlalchemy.Column("latitude", String),
    sqlalchemy.Column("longitude", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("is_deleted", Boolean),
)

booking_events_photo = sqlalchemy.Table(
    "booking_events_photo",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "booking_event_id", Integer, ForeignKey("booking_events.id"), nullable=False
    ),
    sqlalchemy.Column("photo_id", Integer, ForeignKey("pictures.id"), nullable=False),
    sqlalchemy.UniqueConstraint("booking_event_id", "photo_id"),
)

module_bank_credentials = sqlalchemy.Table(
    "module_bank_credentials",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "integration_cashboxes", ForeignKey("integrations_to_cashbox.id")
    ),
    sqlalchemy.Column("access_token", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

module_bank_accounts = sqlalchemy.Table(
    "module_bank_accounts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("payboxes_id", ForeignKey("payboxes.id"), index=True),
    sqlalchemy.Column(
        "module_bank_credential_id",
        ForeignKey("module_bank_credentials.id"),
        index=True,
    ),
    sqlalchemy.Column("accountName", String),
    sqlalchemy.Column("bankBic", String),
    sqlalchemy.Column("bankInn", String),
    sqlalchemy.Column("bankKpp", String),
    sqlalchemy.Column("bankCorrespondentAccount", String),
    sqlalchemy.Column("bankName", String),
    sqlalchemy.Column("beginDate", String),
    sqlalchemy.Column("category", String),
    sqlalchemy.Column("currency", String),
    sqlalchemy.Column("accountId", String, nullable=False, unique=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("is_active", Boolean, default=False),
)

module_bank_operations = sqlalchemy.Table(
    "module_bank_operations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("accountId", String),
    sqlalchemy.Column("payment_crm_id", ForeignKey("payments.id"), index=True),
    sqlalchemy.Column("contragent_crm_id", ForeignKey("contragents.id")),
    sqlalchemy.Column("operationId", String),
    sqlalchemy.Column("cardId", String),
    sqlalchemy.Column("companyId", String),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("category", String),
    sqlalchemy.Column("contragentName", String),
    sqlalchemy.Column("contragentInn", String),
    sqlalchemy.Column("contragentKpp", String),
    sqlalchemy.Column("contragentBankAccountNumber", String),
    sqlalchemy.Column("contragentBankName", String),
    sqlalchemy.Column("contragentBankBic", String),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("bankAccountNumber", String),
    sqlalchemy.Column("paymentPurpose", String),
    sqlalchemy.Column("executed", String),
    sqlalchemy.Column("created", String),
    sqlalchemy.Column("absId", String),
    sqlalchemy.Column("ibsoId", String),
    sqlalchemy.Column("kbk", String),
    sqlalchemy.Column("oktmo", String),
    sqlalchemy.Column("paymentBasis", String),
    sqlalchemy.Column("taxCode", String),
    sqlalchemy.Column("taxDocNum", String),
    sqlalchemy.Column("taxDocDate", String),
    sqlalchemy.Column("payerStatus", String),
    sqlalchemy.Column("uin", String),
    sqlalchemy.Column("currency", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

evotor_credentials = sqlalchemy.Table(
    "evotor_credentials",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "integration_cashboxes", ForeignKey("integrations_to_cashbox.id"), nullable=True
    ),
    sqlalchemy.Column("evotor_token", String),
    sqlalchemy.Column("userId", String),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

tochka_bank_credentials = sqlalchemy.Table(
    "tochka_bank_credentials",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "integration_cashboxes", ForeignKey("integrations_to_cashbox.id")
    ),
    sqlalchemy.Column("access_token", String),
    sqlalchemy.Column("refresh_token", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

tochka_bank_payments = sqlalchemy.Table(
    "tochka_bank_payments",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("accountId", String),
    sqlalchemy.Column("payment_crm_id", ForeignKey("payments.id"), index=True),
    sqlalchemy.Column("contragent_crm_id", ForeignKey("contragents.id")),
    sqlalchemy.Column("statementId", String),
    sqlalchemy.Column("statement_creation_datetime", String),
    sqlalchemy.Column("transactionTypeCode", String),
    sqlalchemy.Column("transactionId", String),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("payment_id", String, index=True),
    sqlalchemy.Column("documentProcessDate", String),
    sqlalchemy.Column("documentNumber", String),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("creditDebitIndicator", String),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("amountNat", Float),
    sqlalchemy.Column("currency", String),
    sqlalchemy.Column("creditor_party_inn", String),
    sqlalchemy.Column("creditor_party_name", String),
    sqlalchemy.Column("creditor_party_kpp", String),
    sqlalchemy.Column("creditor_account_identification", String),
    sqlalchemy.Column("creditor_account_schemeName", String),
    sqlalchemy.Column("creditor_agent_schemeName", String),
    sqlalchemy.Column("creditor_agent_name", String),
    sqlalchemy.Column("creditor_agent_identification", String),
    sqlalchemy.Column("creditor_agent_accountIdentification", String),
    sqlalchemy.Column("debitor_party_inn", String),
    sqlalchemy.Column("debitor_party_name", String),
    sqlalchemy.Column("debitor_party_kpp", String),
    sqlalchemy.Column("debitor_account_identification", String),
    sqlalchemy.Column("debitor_account_schemeName", String),
    sqlalchemy.Column("debitor_agent_schemeName", String),
    sqlalchemy.Column("debitor_agent_name", String),
    sqlalchemy.Column("debitor_agent_identification", String),
    sqlalchemy.Column("debitor_agent_accountIdentification", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

tochka_bank_accounts = sqlalchemy.Table(
    "tochka_bank_accounts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("payboxes_id", ForeignKey("payboxes.id"), index=True),
    sqlalchemy.Column(
        "tochka_bank_credential_id",
        ForeignKey("tochka_bank_credentials.id"),
        index=True,
    ),
    sqlalchemy.Column("customerCode", String),
    sqlalchemy.Column("accountId", String, nullable=False, unique=True),
    sqlalchemy.Column("transitAccount", String),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("statusUpdateDateTime", String),
    sqlalchemy.Column("currency", String),
    sqlalchemy.Column("accountType", String),
    sqlalchemy.Column("accountSubType", String),
    sqlalchemy.Column("registrationDate", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("is_active", Boolean, default=False),
)

entity_type = sqlalchemy.Table(
    "entity_type",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("code", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

tasks = sqlalchemy.Table(
    "tasks",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description", Text),
    sqlalchemy.Column("report", String),
    sqlalchemy.Column("integration_id", ForeignKey("integrations.id")),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("creator", Integer),  # 0 - robot
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

jwt_scopes = sqlalchemy.Table(
    "jwt_scopes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("interaction", String, nullable=False),
    sqlalchemy.Column("scope", String, nullable=False),
)

integrations_type = sqlalchemy.Table(
    "integrations_type",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

integrations_to_cashbox = sqlalchemy.Table(
    "integrations_to_cashbox",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("integration_id", ForeignKey("integrations.id"), nullable=False),
    sqlalchemy.Column(
        "installed_by", ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column(
        "deactivated_by", ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

integrations = sqlalchemy.Table(
    "integrations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("integrations_type", ForeignKey("integrations_type.id")),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description_short", String),
    sqlalchemy.Column("description_long", Text),
    sqlalchemy.Column("folder_name", String),
    sqlalchemy.Column("microservice_id", Integer),
    sqlalchemy.Column("image", String),
    sqlalchemy.Column("images", ARRAY(item_type=String)),
    sqlalchemy.Column("owner", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_public", Boolean),
    sqlalchemy.Column("cost", Integer),
    sqlalchemy.Column("cost_type", String),
    sqlalchemy.Column("cost_percent", Float),
    sqlalchemy.Column("payed_to", Date),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("is_payed", Boolean),
    sqlalchemy.Column("trial", String),
    sqlalchemy.Column("client_app_id", String),
    sqlalchemy.Column("client_secret", String),
    sqlalchemy.Column("code", String),
    sqlalchemy.Column("scopes", Text),
    sqlalchemy.Column("redirect_uri", String),
    sqlalchemy.Column("url", String),
)

cboxes = sqlalchemy.Table(
    "cashboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("balance", Float),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("admin", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("invite_token", String, unique=True),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("seller_name", String, nullable=True),
    sqlalchemy.Column("seller_description", Text, nullable=True),
    sqlalchemy.Column("seller_photo", String, nullable=True),
)

organizations = sqlalchemy.Table(
    "organizations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String, nullable=False),
    sqlalchemy.Column("short_name", String, nullable=False),
    sqlalchemy.Column("full_name", String),
    sqlalchemy.Column("work_name", String),
    sqlalchemy.Column("prefix", String),
    sqlalchemy.Column("inn", BigInteger),
    sqlalchemy.Column("kpp", BigInteger),
    sqlalchemy.Column("okved", BigInteger),
    sqlalchemy.Column("okved2", BigInteger),
    sqlalchemy.Column("okpo", BigInteger),
    sqlalchemy.Column("ogrn", BigInteger),
    sqlalchemy.Column("registration_date", Integer),
    sqlalchemy.Column("org_type", String),
    sqlalchemy.Column("tax_type", String),
    sqlalchemy.Column("tax_percent", Float),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

contracts = sqlalchemy.Table(
    "contracts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String, nullable=False),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("print_name", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("used_from", Integer),
    sqlalchemy.Column("used_to", Integer),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("payment_type", String),
    sqlalchemy.Column("payment_time", String),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

nomenclature = sqlalchemy.Table(
    "nomenclature",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("description_short", String),
    sqlalchemy.Column("description_long", String),
    sqlalchemy.Column(
        "cashback_type",
        Enum(NomenclatureCashbackType),
        nullable=False,
        server_default="lcard_cashback",
    ),
    sqlalchemy.Column("cashback_value", Integer),
    sqlalchemy.Column("code", String),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("category", Integer, ForeignKey("categories.id")),
    sqlalchemy.Column("tags", ARRAY(item_type=String), nullable=True),
    sqlalchemy.Column("segment_tags", ARRAY(item_type=String), nullable=True),
    sqlalchemy.Column("manufacturer", Integer, ForeignKey("manufacturers.id")),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column(
        "global_category_id",
        Integer,
        ForeignKey("global_categories.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    ),
    sqlalchemy.Column("chatting_percent", Integer, nullable=True),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("seo_title", String),
    sqlalchemy.Column("seo_description", String),
    sqlalchemy.Column("seo_keywords", ARRAY(item_type=String)),
    sqlalchemy.Column("production_time_min_from", Integer, nullable=True),
    sqlalchemy.Column("production_time_min_to", Integer, nullable=True),
    sqlalchemy.Column("address", String, nullable=True),
    sqlalchemy.Column("latitude", Float, nullable=True),
    sqlalchemy.Column("longitude", Float, nullable=True),
    sqlalchemy.Column("rating", Integer, nullable=True),
    sqlalchemy.Column("video_link", String, nullable=True),
)

nomenclature_attributes = sqlalchemy.Table(
    "nomenclature_attributes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("alias", String, nullable=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint(
        "name", "cashbox", name="uq_nomenclature_attributes_name_cashbox"
    ),
)

nomenclature_attributes_value = sqlalchemy.Table(
    "nomenclature_attributes_value",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "attribute_id",
        Integer,
        ForeignKey("nomenclature_attributes.id"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "nomenclature_id", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("value", String, nullable=False),
    sqlalchemy.UniqueConstraint(
        "attribute_id",
        "nomenclature_id",
        name="uq_nomenclature_attributes_value_attribute_id_nomenclature_id",
    ),
)

nomenclature_groups_value = sqlalchemy.Table(
    "nomenclature_groups_value",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "nomenclature_id", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column(
        "group_id", Integer, ForeignKey("nomenclature_groups.id"), nullable=False
    ),
    sqlalchemy.Column("is_main", Boolean, nullable=False, server_default="false"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint(
        "nomenclature_id", name="uq_nomenclature_groups_value_nomenclature_id"
    ),
    sqlalchemy.Index(
        "uq_nomenclature_groups_value_group_id_is_main",
        "group_id",
        unique=True,
        postgresql_where=text("is_main IS TRUE"),
    ),
)

nomenclature_groups = sqlalchemy.Table(
    "nomenclature_groups",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=False),
    sqlalchemy.Column("name", String, nullable=True),
)

nomenclature_barcodes = sqlalchemy.Table(
    "nomenclature_barcodes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("nomenclature_id", Integer, ForeignKey("nomenclature.id")),
    sqlalchemy.Column("code", String),
)

categories = sqlalchemy.Table(
    "categories",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("photo_id", Integer, ForeignKey("pictures.id"), nullable=True),
    sqlalchemy.Column("parent", Integer, ForeignKey("categories.id")),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

units = sqlalchemy.Table(
    "units",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("convent_national_view", String),
    sqlalchemy.Column("convent_international_view", String),
    sqlalchemy.Column("symbol_national_view", String),
    sqlalchemy.Column("symbol_international_view", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
)

warehouses = sqlalchemy.Table(
    "warehouses",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("address", String),
    sqlalchemy.Column("latitude", Float),
    sqlalchemy.Column("longitude", Float),
    sqlalchemy.Column("phone", String),
    sqlalchemy.Column("parent", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("is_public", Boolean, server_default="false"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

manufacturers = sqlalchemy.Table(
    "manufacturers",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("photo_id", Integer, ForeignKey("pictures.id"), nullable=True),
    sqlalchemy.Column("external_id", String, nullable=True),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

prices = sqlalchemy.Table(
    "prices",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column(
        "nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column("date_from", Integer),
    sqlalchemy.Column("date_to", Integer),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("address", String, nullable=True),
    sqlalchemy.Column(
        "radius", Float, nullable=True, comment="Радиус действия цены в км."
    ),
    sqlalchemy.Column(
        "hide_outside_radius",
        Boolean,
        nullable=True,
        comment="Скрывать товар, если пользователь вне радиуса.",
    ),
    sqlalchemy.Column("latitude", Float, nullable=True),
    sqlalchemy.Column("longitude", Float, nullable=True),
    sqlalchemy.Column(
        "warehouse_id", Integer, ForeignKey("warehouses.id"), nullable=True
    ),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

price_types = sqlalchemy.Table(
    "price_types",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("tags", ARRAY(item_type=String), nullable=True),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("is_system", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

pictures = sqlalchemy.Table(
    "pictures",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity", String, nullable=False),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("url", String, nullable=False),
    # путь в S3: photos/2025/12/10/123/abc123.jpg
    sqlalchemy.Column("size", Integer),
    sqlalchemy.Column("is_main", Boolean),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

files = sqlalchemy.Table(
    "files",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("title", String, nullable=True),
    sqlalchemy.Column("description", Text, nullable=True),
    sqlalchemy.Column("tags", JSONB, nullable=True),
    sqlalchemy.Column(
        "url", String, nullable=False
    ),  # путь в S3: files/2025/12/10/123/abc123.pdf
    sqlalchemy.Column("size", Integer, nullable=True),
    sqlalchemy.Column("mime_type", String, nullable=False),
    sqlalchemy.Column("extension", String(10), nullable=False),
    sqlalchemy.Column(
        "owner", Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column(
        "is_deleted", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

entity_or_function = sqlalchemy.Table(
    "entity_or_function",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False, unique=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

status_entity_function = sqlalchemy.Table(
    "status_entity_function",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "entity_or_function",
        String,
        ForeignKey("entity_or_function.name", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column("status", Boolean, nullable=False),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    UniqueConstraint("entity_or_function", "cashbox", name="function_cashbox_unique"),
)

pboxes = sqlalchemy.Table(
    "payboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, default="default"),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("start_balance", Float, default=0),
    sqlalchemy.Column("balance", Float, default=0),
    sqlalchemy.Column("balance_date", Integer),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("update_start_balance", Integer),
    sqlalchemy.Column("update_start_balance_date", Integer),
    sqlalchemy.Column("organization_id", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("deleted_at", DateTime(timezone=True), nullable=True),
)

users = sqlalchemy.Table(
    "tg_accounts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("is_admin", Boolean, server_default="false"),
    sqlalchemy.Column("is_blocked", Boolean, server_default="false"),
    sqlalchemy.Column("chat_id", String, unique=True),
    sqlalchemy.Column("owner_id", String),
    sqlalchemy.Column("phone_number", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("photo", String),
    sqlalchemy.Column("first_name", String),
    sqlalchemy.Column("last_name", String),
    sqlalchemy.Column("username", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("ref_id", String),
)

events = sqlalchemy.Table(
    "events",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("method", String),
    sqlalchemy.Column("url", String),
    sqlalchemy.Column("payload", JSON),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("user_id", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("token", String),
    sqlalchemy.Column("ip", String),
    sqlalchemy.Column("status_code", Integer),
    sqlalchemy.Column("request_time", Float, nullable=True, default=0),
    sqlalchemy.Column("promoimage", String),
    sqlalchemy.Column("promodata", JSON),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

installs = sqlalchemy.Table(
    "installs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("iosversion", String),
    sqlalchemy.Column("phone", String),
    sqlalchemy.Column("devicetoken", String, unique=True),
    sqlalchemy.Column("md5key", String),
    sqlalchemy.Column("geolocation", String),
    sqlalchemy.Column("push", Boolean),
    sqlalchemy.Column("my_push", String),
    sqlalchemy.Column("foreign_push", String),
    sqlalchemy.Column("contacts", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

links = sqlalchemy.Table(
    "links",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("user_id", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("install_id", Integer, ForeignKey("installs.id"), unique=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tg_token", String, unique=True),
    sqlalchemy.Column("delinked", Boolean, default=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

users_cboxes_relation = sqlalchemy.Table(
    "relation_tg_cashboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("user", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("token", String),
    sqlalchemy.Column("status", Boolean, default=True),
    sqlalchemy.Column("tags", ARRAY(item_type=String), nullable=True),
    sqlalchemy.Column("is_owner", Boolean, default=True),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("timezone", String),
    sqlalchemy.Column("payment_past_edit_days", Integer),
    sqlalchemy.Column("shift_work_enabled", Boolean, default=False),
    sqlalchemy.Column("rating", Float, nullable=True, server_default="0.0"),
)

contragents = sqlalchemy.Table(
    "contragents",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("phone", String, nullable=True),
    sqlalchemy.Column("phone_code", String, nullable=True),
    sqlalchemy.Column("inn", String, nullable=True),
    sqlalchemy.Column("description", Text),
    sqlalchemy.Column("contragent_type", Enum(Contragent_types)),
    sqlalchemy.Column("type", Enum(ContragentType), nullable=True),
    sqlalchemy.Column("birth_date", Date),
    sqlalchemy.Column("data", JSON),
    sqlalchemy.Column("additional_phones", String, nullable=True),
    sqlalchemy.Column("gender", Enum(Gender), nullable=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column(
        "is_phone_formatted", Boolean, server_default="false", nullable=False
    ),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("email", String),
)

articles = sqlalchemy.Table(
    "articles",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("icon_file", String),
    sqlalchemy.Column("emoji", String),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("expenses_for", String),
    sqlalchemy.Column("distribute_according", String),
    sqlalchemy.Column("distribute_for", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("dc", Enum(DebitCreditType), nullable=True),
)

payments = sqlalchemy.Table(
    "payments",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("name", String, nullable=True),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("article", String, nullable=True),
    sqlalchemy.Column("project_id", Integer, ForeignKey("projects.id"), nullable=True),
    sqlalchemy.Column("article_id", Integer, ForeignKey("articles.id"), nullable=True),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("amount_without_tax", Float),
    sqlalchemy.Column("description", Text, nullable=True),
    sqlalchemy.Column("date", Integer),
    sqlalchemy.Column("repeat_freq", Integer, nullable=True),
    sqlalchemy.Column("parent_id", Integer, nullable=True),
    sqlalchemy.Column(
        "repeat_parent_id", Integer, ForeignKey("payments.id"), nullable=True
    ),
    sqlalchemy.Column("repeat_period", String, nullable=True),
    sqlalchemy.Column("repeat_first", Integer, nullable=True),
    sqlalchemy.Column("repeat_last", Integer, nullable=True),
    sqlalchemy.Column("repeat_number", Integer, nullable=True),
    sqlalchemy.Column("repeat_day", Integer, nullable=True),
    sqlalchemy.Column("repeat_month", Integer, nullable=True),
    sqlalchemy.Column("repeat_seconds", Integer, nullable=True),
    sqlalchemy.Column("repeat_weekday", String, nullable=True),
    sqlalchemy.Column("stopped", Boolean, default=False),
    sqlalchemy.Column("status", Boolean, default=True),
    sqlalchemy.Column("tax", Float, nullable=True),
    sqlalchemy.Column("tax_type", String, nullable=True),
    sqlalchemy.Column("deb_cred", Boolean, default=False),
    sqlalchemy.Column("raspilen", Boolean, default=False),
    sqlalchemy.Column(
        "contragent", Integer, ForeignKey("contragents.id"), nullable=True
    ),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), index=True),
    sqlalchemy.Column("paybox", Integer, ForeignKey("payboxes.id"), index=True),
    sqlalchemy.Column("paybox_to", Integer, ForeignKey("payboxes.id"), nullable=True),
    sqlalchemy.Column("account", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("is_deleted", Boolean, default=False),
    sqlalchemy.Column("cheque", Integer, ForeignKey("cheques.id"), nullable=True),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("contract_id", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column("docs_purchases_id", Integer, ForeignKey("docs_purchases.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

projects = sqlalchemy.Table(
    "projects",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("incoming", Float),
    sqlalchemy.Column("outgoing", Float),
    sqlalchemy.Column("profitability", Float),
    sqlalchemy.Column("proj_sum", Float),
    sqlalchemy.Column("icon_file", String),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

tariffs = sqlalchemy.Table(
    "pay_tariffs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, unique=True),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("per_user", Boolean, default=False, nullable=False),
    sqlalchemy.Column("frequency", Integer, default=30, nullable=False),
    sqlalchemy.Column("archived", Boolean, default=False, nullable=False),
    sqlalchemy.Column("actual", Boolean, default=True, nullable=False),
    sqlalchemy.Column("demo_days", Integer, nullable=False),
    sqlalchemy.Column("offer_hours", Integer, nullable=True, server_default="0"),
    sqlalchemy.Column("discount_percent", Float, nullable=True, server_default="0"),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

transactions = sqlalchemy.Table(
    "pay_transactions",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tariff", Integer, ForeignKey("pay_tariffs.id")),
    sqlalchemy.Column("users", Integer),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("type", String, nullable=False),
    sqlalchemy.Column(
        "is_manual_deposit", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column("external_id", String, nullable=True),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

accounts_balances = sqlalchemy.Table(
    "pay_accounts_balances",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tariff", Integer, ForeignKey("pay_tariffs.id"), nullable=False),
    sqlalchemy.Column(
        "last_transaction", Integer, ForeignKey("pay_transactions.id"), nullable=True
    ),
    sqlalchemy.Column("balance", Float),
    sqlalchemy.Column("tariff_type", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

cheques = sqlalchemy.Table(
    "cheques",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("user", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("data", JSON),
    sqlalchemy.Column("created_at", Integer),
)

amo_bots = sqlalchemy.Table(
    "amo_bots",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("ext_id", Integer, index=True),
    sqlalchemy.Column(
        "install_group_id", Integer, ForeignKey("amo_install_groups.id"), index=True
    ),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("type_functionality", String),
    sqlalchemy.Column("type", Integer),
    sqlalchemy.Column("amo_bot_handler_id", Integer),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint("install_group_id", "ext_id"),
    extend_existing=True,
)

amo_bots_settings = sqlalchemy.Table(
    "amo_bots_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("silent", Integer),
    sqlalchemy.Column("active", Boolean),
    sqlalchemy.Column("is_working_without_chat", Boolean),
    sqlalchemy.Column("type_functionality", Integer),
    sqlalchemy.Column("amo_bots_id", Integer, ForeignKey("amo_bots.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

table_triggers = sqlalchemy.Table(
    "table_triggers",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("amo_bots_id", Integer, ForeignKey("amo_bots.id")),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("type", Enum(TriggerType)),
    sqlalchemy.Column("time_variant", Enum(TriggerTime)),
    sqlalchemy.Column("time", BigInteger),
    sqlalchemy.Column("key", String),
    sqlalchemy.Column("active", Boolean),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

table_triggers_events = sqlalchemy.Table(
    "table_triggers_events",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column(
        "table_triggers_id", Integer, ForeignKey("table_triggers.id"), index=True
    ),
    sqlalchemy.Column("install_group_id", Integer, ForeignKey("amo_install_groups.id")),
    sqlalchemy.Column("loyality_transactions_id", Integer, index=True),
    sqlalchemy.Column("event", String),
    sqlalchemy.Column("before_event", Boolean),
    sqlalchemy.Column("after_event", Boolean),
    sqlalchemy.Column("body", JSON),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint("table_triggers_id", "loyality_transactions_id"),
    extend_existing=True,
)

amo_install = sqlalchemy.Table(
    "amo_install",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("code", String),
    sqlalchemy.Column("referrer", String),
    sqlalchemy.Column("platform", Integer),
    sqlalchemy.Column("amo_account_id", Integer),
    sqlalchemy.Column("client_id", String),
    sqlalchemy.Column("client_secret", String),
    sqlalchemy.Column("refresh_token", String),
    sqlalchemy.Column("access_token", String),
    sqlalchemy.Column("expires_in", Integer),
    sqlalchemy.Column("active", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("field_id", Integer),
    sqlalchemy.Column("from_widget", Integer, ForeignKey("amo_settings.id")),
    sqlalchemy.Column("is_refresh", Boolean, server_default="false"),
    sqlalchemy.Column("install_group_id", Integer, ForeignKey("amo_install_groups.id")),
)

amo_install_groups = sqlalchemy.Table(
    "amo_install_groups",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("referrer", String, unique=True),
    sqlalchemy.Column("pair_token", String),
    sqlalchemy.Column("setup_custom_fields", Boolean, server_default="false"),
)

amo_integrations = sqlalchemy.Table(
    "amo_integrations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

amo_install_table_cashboxes = sqlalchemy.Table(
    "amo_install_table_cashboxes",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("last_token", String),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("additional_info", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

amo_events_type = sqlalchemy.Table(
    "amo_events_type",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("event_id", Integer, ForeignKey("events.id")),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

custom_fields = sqlalchemy.Table(
    "custom_fields",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity", String),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("type", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

custom_fields_values = sqlalchemy.Table(
    "custom_fields_values",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cf_id", Integer, ForeignKey("custom_fields.id")),
    sqlalchemy.Column("value", String),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
)

docs_sales = sqlalchemy.Table(
    "docs_sales",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("operation", String),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("comment", String),
    sqlalchemy.Column("track_number", String),
    sqlalchemy.Column("delivery_company", String),
    sqlalchemy.Column("order_source", String),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), index=True),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("contract", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column(
        "organization", Integer, ForeignKey("organizations.id"), nullable=False
    ),
    sqlalchemy.Column("warehouse", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("parent_docs_sales", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("settings", Integer, ForeignKey("docs_sales_settings.id")),
    sqlalchemy.Column("autorepeat", Boolean, default=False),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("tax_included", Boolean),
    sqlalchemy.Column("tax_active", Boolean),
    sqlalchemy.Column("sales_manager", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("sum", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean, index=True),
    sqlalchemy.Column(
        "order_status", Enum(OrderStatus), nullable=True, server_default="received"
    ),
    sqlalchemy.Column(
        "assigned_picker",
        Integer,
        ForeignKey("relation_tg_cashboxes.id"),
        nullable=True,
    ),
    sqlalchemy.Column("picker_started_at", DateTime(timezone=True), nullable=True),
    sqlalchemy.Column("picker_finished_at", DateTime(timezone=True), nullable=True),
    sqlalchemy.Column(
        "assigned_courier",
        Integer,
        ForeignKey("relation_tg_cashboxes.id"),
        nullable=True,
    ),
    sqlalchemy.Column("courier_picked_at", DateTime(timezone=True), nullable=True),
    sqlalchemy.Column("courier_delivered_at", DateTime(timezone=True), nullable=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("priority", Integer, nullable=True),
    sqlalchemy.Column("is_marketplace_order", Boolean, server_default="false"),
)

docs_sales_tags = sqlalchemy.Table(
    "docs_sales_tags",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "docs_sales_id", Integer, ForeignKey("docs_sales.id"), nullable=False
    ),
    sqlalchemy.Column("name", String, index=True),
)

docs_sales_delivery_info = sqlalchemy.Table(
    "docs_sales_delivery_info",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("address", String),
    sqlalchemy.Column("delivery_date", DateTime(timezone=True)),
    sqlalchemy.Column("delivery_price", Float),
    sqlalchemy.Column("recipient", JSON),
    sqlalchemy.Column("note", String),
)

docs_sales_goods = sqlalchemy.Table(
    "docs_sales_goods",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "docs_sales_id", Integer, ForeignKey("docs_sales.id"), nullable=False
    ),
    sqlalchemy.Column(
        "nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("quantity", Float, nullable=False),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("tax", Float),
    sqlalchemy.Column("discount", Float),
    sqlalchemy.Column("sum_discounted", Float),
    sqlalchemy.Column("status", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

docs_purchases = sqlalchemy.Table(
    "docs_purchases",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("operation", String),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("comment", String),
    sqlalchemy.Column("status", Boolean),
    sqlalchemy.Column("client", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("contract", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column(
        "organization", Integer, ForeignKey("organizations.id"), nullable=False
    ),
    sqlalchemy.Column("warehouse", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("purchased_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("sum", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

docs_purchases_goods = sqlalchemy.Table(
    "docs_purchases_goods",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "docs_purchases_id", Integer, ForeignKey("docs_purchases.id"), nullable=False
    ),
    sqlalchemy.Column(
        "nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("quantity", Float, nullable=False),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

docs_warehouse = sqlalchemy.Table(
    "docs_warehouse",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column("operation", String),
    sqlalchemy.Column("comment", String),
    sqlalchemy.Column("status", Boolean, default=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("contract", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column("contragent", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("docs_purchases", Integer, ForeignKey("docs_purchases.id")),
    sqlalchemy.Column(
        "organization", Integer, ForeignKey("organizations.id"), nullable=False
    ),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("warehouse", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("to_warehouse", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("sum", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("tg_accounts.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

docs_warehouse_goods = sqlalchemy.Table(
    "docs_warehouse_goods",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "docs_warehouse_id", Integer, ForeignKey("docs_warehouse.id"), nullable=False
    ),
    sqlalchemy.Column(
        "nomenclature", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column("price_type", Integer, ForeignKey("price_types.id")),
    sqlalchemy.Column("price", Float, nullable=False),
    sqlalchemy.Column("quantity", Float, nullable=False),
    sqlalchemy.Column("unit", Integer, ForeignKey("units.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

warehouse_register_movement = sqlalchemy.Table(
    "warehouse_register_movement",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type_amount", Enum(OperationType)),
    sqlalchemy.Column("organization_id", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("warehouse_id", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("nomenclature_id", Integer, ForeignKey("nomenclature.id")),
    sqlalchemy.Column("document_sale_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("document_purchase_id", Integer, ForeignKey("docs_purchases.id")),
    sqlalchemy.Column(
        "document_warehouse_id", Integer, ForeignKey("docs_warehouse.id")
    ),
    sqlalchemy.Column("amount", Float, nullable=False),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

docs_reconciliation = sqlalchemy.Table(
    "docs_reconciliation",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("number", String),
    sqlalchemy.Column("dated", Integer),
    sqlalchemy.Column(
        "organization", Integer, ForeignKey("organizations.id"), nullable=False
    ),
    sqlalchemy.Column(
        "contragent", Integer, ForeignKey("contragents.id"), nullable=False
    ),
    sqlalchemy.Column("contract", Integer, ForeignKey("contracts.id")),
    sqlalchemy.Column("organization_name", String),
    sqlalchemy.Column("contragent_name", String),
    sqlalchemy.Column("period_from", Integer),
    sqlalchemy.Column("period_to", Integer),
    sqlalchemy.Column("documents", JSON),
    sqlalchemy.Column("documents_grouped", JSON),
    sqlalchemy.Column("organization_period_debt", Float),
    sqlalchemy.Column("organization_period_credit", Float),
    sqlalchemy.Column("contragent_period_debt", Float),
    sqlalchemy.Column("contragent_period_credit", Float),
    sqlalchemy.Column("organization_initial_balance", Float),
    sqlalchemy.Column("contragent_initial_balance", Float),
    sqlalchemy.Column("organization_closing_balance", Float),
    sqlalchemy.Column("contragent_closing_balance", Float),
    sqlalchemy.Column("created_by", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

entity_to_entity = sqlalchemy.Table(
    "entity_to_entity",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String, nullable=False),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column(
        "from_entity", Integer, ForeignKey("entity_or_function.id"), nullable=False
    ),
    sqlalchemy.Column("from_id", Integer, nullable=False),
    sqlalchemy.Column(
        "to_entity", Integer, ForeignKey("entity_or_function.id"), nullable=False
    ),
    sqlalchemy.Column("to_id", Integer, nullable=False),
    sqlalchemy.Column("status", Boolean, default=sqlalchemy.true),
    sqlalchemy.Column("delinked", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

distribution_docs = sqlalchemy.Table(
    "distribution_docs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("period_start", Integer, nullable=False),
    sqlalchemy.Column("period_end", Integer, nullable=False),
    sqlalchemy.Column("is_preview", Boolean),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

distribution_docs_operations = sqlalchemy.Table(
    "distribution_fifo_operations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "distribution_fifo",
        Integer,
        ForeignKey("distribution_docs.id", ondelete="CASCADE"),
    ),
    sqlalchemy.Column("document_sale", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("document_purchase", Integer, ForeignKey("docs_purchases.id")),
    sqlalchemy.Column("document_warehouse", Integer, ForeignKey("docs_warehouse.id")),
    sqlalchemy.Column("nomenclature", Integer, ForeignKey("nomenclature.id")),
    sqlalchemy.Column("dated", Integer, nullable=False),
    sqlalchemy.Column("start_amount", Integer, nullable=False),
    sqlalchemy.Column("start_price", Float, nullable=False),
    sqlalchemy.Column("incoming_amount", Integer),
    sqlalchemy.Column("incoming_price", Float),
    sqlalchemy.Column("outgoing_amount", Integer),
    sqlalchemy.Column("outgoing_price", Float),
    sqlalchemy.Column("end_amount", Integer, nullable=False),
    sqlalchemy.Column("end_price", Float, nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

gross_profit_docs = sqlalchemy.Table(
    "gross_profit_docs",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id"), nullable=True),
    sqlalchemy.Column("period_start", Integer, nullable=False),
    sqlalchemy.Column("period_end", Integer, nullable=False),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

gross_profit_docs_operations = sqlalchemy.Table(
    "gross_profit_docs_operations",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "gross_profit_doc_id",
        Integer,
        ForeignKey("gross_profit_docs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "document_sale", Integer, ForeignKey("docs_sales.id"), nullable=False
    ),
    sqlalchemy.Column("net_cost", Float, nullable=False),
    sqlalchemy.Column("sum", Float, nullable=False),
    sqlalchemy.Column("actual_revenue", Float, nullable=False),
    sqlalchemy.Column("direct_costs", Float, nullable=False),
    sqlalchemy.Column("indirect_costs", Float, nullable=False),
    sqlalchemy.Column("gross_profit", Float, nullable=False),
    sqlalchemy.Column("rentability", Float, nullable=False),
    sqlalchemy.Column("sales_manager", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

fifo_settings = sqlalchemy.Table(
    "fifo_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("fully_closed_date", Integer, nullable=False),
    sqlalchemy.Column("temporary_closed_date", Integer),
    sqlalchemy.Column("blocked_date", Integer),
    sqlalchemy.Column("month_closing_delay_days", Integer),
    sqlalchemy.Column("preview_close_period_seconds", Integer, nullable=False),
    sqlalchemy.Column(
        "organization_id",
        Integer,
        ForeignKey("organizations.id"),
        nullable=False,
        unique=True,
    ),
    sqlalchemy.Column("in_progress", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

warehouse_balances = sqlalchemy.Table(
    "warehouse_balances",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("organization_id", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("warehouse_id", Integer, ForeignKey("warehouses.id")),
    sqlalchemy.Column("nomenclature_id", Integer, ForeignKey("nomenclature.id")),
    sqlalchemy.Column("document_sale_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("document_purchase_id", Integer, ForeignKey("docs_purchases.id")),
    sqlalchemy.Column(
        "document_warehouse_id", Integer, ForeignKey("docs_warehouse.id")
    ),
    sqlalchemy.Column("incoming_amount", Integer),
    sqlalchemy.Column("outgoing_amount", Integer),
    sqlalchemy.Column("current_amount", Integer, nullable=False),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

warehouse_balances_latest = sqlalchemy.Table(
    "warehouse_balances_latest",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "organization_id", Integer, ForeignKey("organizations.id"), nullable=False
    ),
    sqlalchemy.Column(
        "warehouse_id", Integer, ForeignKey("warehouses.id"), nullable=False
    ),
    sqlalchemy.Column(
        "nomenclature_id", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("current_amount", Float, nullable=False, default=0),
    sqlalchemy.Column("incoming_amount", Float, nullable=False, default=0),
    sqlalchemy.Column("outgoing_amount", Float, nullable=False, default=0),
    sqlalchemy.Column("updated_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.UniqueConstraint(
        "organization_id",
        "warehouse_id",
        "nomenclature_id",
        name="uq_wbl_org_wh_nom",
    ),
)

messages = sqlalchemy.Table(
    "messages",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("tg_message_id", Integer),
    sqlalchemy.Column("tg_user_or_chat", String),
    sqlalchemy.Column("body", String),
    sqlalchemy.Column("from_or_to", String),
    sqlalchemy.Column("created_at", String),
    sqlalchemy.Column("updated_at", String),
)

loyality_cards = sqlalchemy.Table(
    "loyality_cards",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("card_number", BigInteger),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("balance", Float),
    sqlalchemy.Column("income", Integer),
    sqlalchemy.Column("outcome", Integer),
    sqlalchemy.Column("cashback_percent", Integer),
    sqlalchemy.Column("minimal_checque_amount", Integer),
    sqlalchemy.Column("start_period", DateTime),
    sqlalchemy.Column("end_period", DateTime),
    sqlalchemy.Column("max_percentage", Integer),
    sqlalchemy.Column("max_withdraw_percentage", Integer),
    sqlalchemy.Column("contragent_id", ForeignKey("contragents.id")),
    sqlalchemy.Column("organization_id", ForeignKey("organizations.id"), default=1),
    sqlalchemy.Column("cashbox_id", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_by_id", ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("status_card", Boolean),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column(
        "apple_wallet_advertisement",
        String,
        nullable=False,
        default="TableCRM",
        server_default="TableCRM",
    ),
    sqlalchemy.Column("lifetime", BigInteger, index=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

loyality_transactions = sqlalchemy.Table(
    "loyality_transactions",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("type", String, default="accrual"),
    sqlalchemy.Column("dated", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column("amount", Float),
    sqlalchemy.Column("loyality_card_id", ForeignKey("loyality_cards.id"), index=True),
    sqlalchemy.Column("loyality_card_number", BigInteger),
    sqlalchemy.Column("created_by_id", ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column("card_balance", Float),
    sqlalchemy.Column("docs_sales_id", ForeignKey("docs_sales.id")),
    sqlalchemy.Column("cashbox", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("status", Boolean, default=True),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("cashier_name", String),
    sqlalchemy.Column("dead_at", DateTime),
    sqlalchemy.Column("autoburned", Boolean, index=True),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

loyality_settings = sqlalchemy.Table(
    "loyality_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("organization", Integer, ForeignKey("organizations.id")),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("cashback_percent", Integer),
    sqlalchemy.Column("minimal_checque_amount", Integer),
    sqlalchemy.Column("start_period", DateTime),
    sqlalchemy.Column("end_period", DateTime),
    sqlalchemy.Column("max_withdraw_percentage", Integer),
    sqlalchemy.Column("max_percentage", Integer),
    sqlalchemy.Column("lifetime", Integer),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

doc_generated = sqlalchemy.Table(
    "doc_generated",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("template_id", ForeignKey("doc_template.id")),
    sqlalchemy.Column("doc_link", String),
    sqlalchemy.Column("entity", String),
    sqlalchemy.Column("entity_id", Integer),
    sqlalchemy.Column("type_doc", String),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
)

doc_templates = sqlalchemy.Table(
    "doc_template",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("template_data", String),
    sqlalchemy.Column("tags", String),
    sqlalchemy.Column("user_id", Integer),
    sqlalchemy.Column("cashbox", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("type", Integer, ForeignKey("type_template.id"), nullable=True),
)

tag_templates = sqlalchemy.Table(
    "tag_template",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("name", String, nullable=False),
)

type_template = sqlalchemy.Table(
    "type_template",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String, nullable=False),
)

areas = sqlalchemy.Table(
    "areas",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("cashbox", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

pages = sqlalchemy.Table(
    "pages",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("cashbox", ForeignKey("cashboxes.id")),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

amo_settings = sqlalchemy.Table(
    "amo_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("scope", String),
    sqlalchemy.Column("redirect_uri", String),
    sqlalchemy.Column("client_secret", String),
    sqlalchemy.Column("integration_id", String),
    sqlalchemy.Column(
        "load_type_id", Integer, ForeignKey("amo_settings_load_types.id")
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    extend_existing=True,
)

amo_settings_load_types = sqlalchemy.Table(
    "amo_settings_load_types",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("contacts", Boolean, server_default="false"),
    sqlalchemy.Column("leads", Boolean, server_default="false"),
    extend_existing=True,
)

amo_contacts = sqlalchemy.Table(
    "amo_contacts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("phone", String),
    sqlalchemy.Column("phone_code", String),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id"), index=True
    ),
    sqlalchemy.Column("formatted_phone", String),
    sqlalchemy.Column("ext_id", Integer, index=True),
    sqlalchemy.Column("created_at", BigInteger),
    sqlalchemy.Column("updated_at", BigInteger),
    sqlalchemy.UniqueConstraint("amo_install_group_id", "ext_id"),
    extend_existing=True,
)

amo_contacts_double = sqlalchemy.Table(
    "amo_contacts_double",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("orig_id", Integer, ForeignKey("amo_contacts.id")),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("phone", String),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("formatted_phone", String),
    sqlalchemy.Column("ext_id", Integer),
    sqlalchemy.Column("created_at", BigInteger),
    sqlalchemy.Column("updated_at", BigInteger),
    extend_existing=True,
)

amo_table_contacts = sqlalchemy.Table(
    "amo_table_contacts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("amo_id", Integer, ForeignKey("amo_contacts.id")),
    sqlalchemy.Column("table_id", Integer, ForeignKey("contragents.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("is_main", Boolean, server_default="true", nullable=False),
    UniqueConstraint("amo_id"),
    sqlalchemy.Index(
        "uq_table_id_main",
        "table_id",
        unique=True,
        postgresql_where=sqlalchemy.Column("is_main") == True,
    ),
    extend_existing=True,
)

amo_lead_pipelines = sqlalchemy.Table(
    "amo_lead_pipelines",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("amo_id", Integer),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("sort", Integer),
    sqlalchemy.Column("is_main", Boolean),
    sqlalchemy.Column("is_unsorted_on", Boolean),
    sqlalchemy.Column("is_archive", Boolean),
    sqlalchemy.Column("account_id", Integer),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint("amo_install_group_id", "amo_id"),
    extend_existing=True,
)

amo_lead_statuses = sqlalchemy.Table(
    "amo_lead_statuses",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("sort", Integer),
    sqlalchemy.Column("is_editable", Boolean),
    sqlalchemy.Column("pipeline_id", Integer),
    sqlalchemy.Column("amo_id", Integer),
    sqlalchemy.Column("color", String),
    sqlalchemy.Column("type", Integer),
    sqlalchemy.Column("account_id", Integer),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint("pipeline_id", "amo_id"),
    extend_existing=True,
)

amo_leads = sqlalchemy.Table(
    "amo_leads",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id"), index=True
    ),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("price", BigInteger),
    sqlalchemy.Column("status_id", BigInteger),
    sqlalchemy.Column("pipeline_id", BigInteger),
    sqlalchemy.Column("contact_id", Integer),
    sqlalchemy.Column("closed_at", DateTime(timezone=True)),
    sqlalchemy.Column("is_deleted", Boolean),
    sqlalchemy.Column("account_id", BigInteger),
    sqlalchemy.Column("score", BigInteger),
    sqlalchemy.Column("labor_cost", BigInteger),
    sqlalchemy.Column("amo_id", BigInteger, index=True),
    sqlalchemy.Column("created_at", BigInteger),
    sqlalchemy.Column("updated_at", BigInteger),
    sqlalchemy.Column("is_edit", Boolean, server_default="false"),
    sqlalchemy.UniqueConstraint("amo_install_group_id", "amo_id"),
    extend_existing=True,
)

amo_leads_docs_sales_mapping = sqlalchemy.Table(
    "amo_leads_docs_sales_mapping",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "docs_sales_id", Integer, ForeignKey("docs_sales.id"), nullable=False
    ),
    sqlalchemy.Column(
        "lead_id", Integer, ForeignKey("amo_leads.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("table_status", SmallInteger),
    sqlalchemy.Column("is_sync", Boolean),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    extend_existing=True,
)

docs_sales_settings = sqlalchemy.Table(
    "docs_sales_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("repeatability_period", Enum(Repeatability), nullable=False),
    sqlalchemy.Column("repeatability_value", Integer, nullable=False),
    sqlalchemy.Column("date_next_created", Integer, default=0),
    sqlalchemy.Column("transfer_from_weekends", Boolean, default=True),
    sqlalchemy.Column("skip_current_month", Boolean, default=True),
    sqlalchemy.Column("repeatability_count", Integer, default=0),
    sqlalchemy.Column("default_payment_status", Boolean, default=False),
    sqlalchemy.Column("repeatability_tags", Boolean, default=False),
    sqlalchemy.Column("repeatability_status", Boolean, default=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

amo_users = sqlalchemy.Table(
    "amo_users",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("name", String),
    sqlalchemy.Column("email", String),
    sqlalchemy.Column("is_admin", Boolean),
    sqlalchemy.Column("is_active", Boolean),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("ext_id", Integer),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint("amo_install_group_id", "ext_id"),
    extend_existing=True,
)

amo_custom_fields_comparison = sqlalchemy.Table(
    "amo_custom_fields_comparison",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("field_amo_id", BIGINT),
    sqlalchemy.Column("field_detection_name", String),
    extend_existing=True,
)

amo_install_widget_installer = sqlalchemy.Table(
    "amo_install_widget_installer",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("amo_account_id", BigInteger, unique=True),
    sqlalchemy.Column("installed_by_role", Enum(InstalledByRole)),
    sqlalchemy.Column("client_name", String),
    sqlalchemy.Column("partner_name", String),
    sqlalchemy.Column("client_cashbox", Integer),
    sqlalchemy.Column("partner_cashbox", Integer),
    sqlalchemy.Column("client_number_phone", String),
    sqlalchemy.Column("partner_number_phone", String),
    sqlalchemy.Column("client_inn", String),
)

tbank_accounts = sqlalchemy.Table(
    "tbank_accounts",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True)),
    sqlalchemy.Column("updated_at", DateTime(timezone=True)),
    sqlalchemy.Column("deleted_at", DateTime(timezone=True)),
    sqlalchemy.Column("terminal_key", Text),
    sqlalchemy.Column("invite_token", Text),
    sqlalchemy.Column("password", Text),
)

tbank_orders = sqlalchemy.Table(
    "tbank_orders",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True)),
    sqlalchemy.Column("updated_at", DateTime(timezone=True)),
    sqlalchemy.Column("deleted_at", DateTime(timezone=True)),
    sqlalchemy.Column("amount", BigInteger),
    sqlalchemy.Column("token", Text),
    sqlalchemy.Column("description", Text),
    sqlalchemy.Column("invite_token", Text),
    sqlalchemy.Column("payment_id", BigInteger),
    sqlalchemy.Column("tbank_payment_id", Text),
    sqlalchemy.Column("payment_url", Text),
    sqlalchemy.Column(
        "docs_sales_id", Integer, ForeignKey("docs_sales.id"), nullable=True
    ),
)

amo_custom_fields = sqlalchemy.Table(
    "amo_custom_fields",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("type_entity", Enum(TypeCustomField), nullable=False),
    sqlalchemy.Column("code", String, nullable=False),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("type", String, nullable=False),
    extend_existing=True,
)

amo_install_custom_fields = sqlalchemy.Table(
    "amo_install_custom_fields",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "custom_field_id", Integer, ForeignKey("amo_custom_fields.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox_id", Integer, nullable=False),
    sqlalchemy.Column("active", Boolean, server_default="true"),
    sqlalchemy.UniqueConstraint("custom_field_id", "cashbox_id"),
    extend_existing=True,
)

amo_entity_custom_fields = sqlalchemy.Table(
    "amo_entity_custom_fields",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "custom_field_id", Integer, ForeignKey("amo_custom_fields.id"), nullable=False
    ),
    sqlalchemy.Column("value", String, nullable=False),
    sqlalchemy.Column(
        "lead_id", Integer, ForeignKey("amo_leads.id"), nullable=True, index=True
    ),
    sqlalchemy.Column(
        "contact_id", Integer, ForeignKey("amo_contacts.id"), nullable=True, index=True
    ),
    sqlalchemy.UniqueConstraint("custom_field_id", "lead_id", "contact_id"),
    extend_existing=True,
)

tg_bot_bills = sqlalchemy.Table(
    "tg_bot_bills",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("payment_date", Date),
    sqlalchemy.Column(
        "created_by", Integer, ForeignKey("tg_accounts.id"), nullable=False
    ),
    sqlalchemy.Column("s3_url", String, nullable=False),
    sqlalchemy.Column("plain_text", String, nullable=False),
    sqlalchemy.Column("file_name", String, nullable=False),
    sqlalchemy.Column(
        "tochka_bank_account_id",
        Integer,
        ForeignKey("tochka_bank_accounts.id"),
        nullable=True,
    ),
    sqlalchemy.Column("payment_amount", Float, nullable=True),
    sqlalchemy.Column("counterparty_account_number", String, nullable=True),
    sqlalchemy.Column("payment_purpose", String, nullable=True),
    sqlalchemy.Column("counterparty_bank_bic", String, nullable=True),
    sqlalchemy.Column("counterparty_name", String, nullable=True),
    sqlalchemy.Column("corr_account", String, nullable=True),
    sqlalchemy.Column("status", Enum(TgBillStatus), nullable=False),
    sqlalchemy.Column("request_id", String, nullable=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), default=func.now()),
    sqlalchemy.Column(
        "updated_at", DateTime(timezone=True), default=func.now(), onupdate=func.now()
    ),
    sqlalchemy.Column("deleted_at", DateTime(timezone=True)),
    extend_existing=True,
)
tg_bot_bill_approvers = sqlalchemy.Table(
    "tg_bot_bill_approvers",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "approver_id", Integer, ForeignKey("tg_accounts.id"), nullable=False
    ),
    sqlalchemy.Column(
        "bill_id", Integer, ForeignKey("tg_bot_bills.id"), nullable=False
    ),
    sqlalchemy.Column("status", Enum(TgBillApproveStatus), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), default=func.now()),
    sqlalchemy.Column(
        "updated_at", DateTime(timezone=True), default=func.now(), onupdate=func.now()
    ),
    sqlalchemy.Column("deleted_at", DateTime(timezone=True)),
    extend_existing=True,
)

docs_sales_utm_tags = sqlalchemy.Table(
    "docs_sales_utm_tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("docs_sales_id", Integer, ForeignKey("docs_sales.id")),
    sqlalchemy.Column("utm_source", String),
    sqlalchemy.Column("utm_medium", String),
    sqlalchemy.Column("utm_campaign", String),
    sqlalchemy.Column("utm_term", ARRAY(item_type=String)),
    sqlalchemy.Column("utm_content", String),
    sqlalchemy.Column("utm_name", String),
    sqlalchemy.Column("utm_phone", String),
    sqlalchemy.Column("utm_email", String),
    sqlalchemy.Column("utm_leadid", String),
    sqlalchemy.Column("utm_yclientid", String),
    sqlalchemy.Column("utm_gaclientid", String),
)


segments_types = sqlalchemy.Table(
    "segments_types",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

segments = sqlalchemy.Table(
    "segments",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("description", String, nullable=True),
    sqlalchemy.Column("hash_tag", String, nullable=True),
    sqlalchemy.Column("criteria", JSON, nullable=False),
    sqlalchemy.Column("actions", JSON, nullable=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id"), index=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), default=func.now()),
    sqlalchemy.Column("updated_at", DateTime(timezone=True)),
    sqlalchemy.Column("type_of_update", String, nullable=False),
    sqlalchemy.Column("update_settings", JSON, nullable=True),
    sqlalchemy.Column("previous_update_at", DateTime(timezone=True)),
    sqlalchemy.Column(
        "status",
        Enum(SegmentStatus),
        nullable=False,
        server_default=SegmentStatus.created.value,
    ),
    sqlalchemy.Column("is_archived", Boolean, server_default="false", nullable=False),
    sqlalchemy.Column("is_deleted", Boolean, server_default="false", nullable=False),
    sqlalchemy.Column(
        "type", ForeignKey("segments_types.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("auto_reply", JSON, nullable=True),
)


class SegmentObjectType(enum.Enum):
    docs_sales = "docs_sales"
    contragents = "contragents"
    loyality_cards = "loyality_cards"
    nomenclatures = "nomenclatures"


segment_objects = sqlalchemy.Table(
    "segment_objects",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "segment_id", BigInteger, ForeignKey("segments.id"), nullable=False
    ),
    sqlalchemy.Column("object_id", BigInteger, nullable=False, index=True),
    sqlalchemy.Column(
        "object_type",
        Enum(SegmentObjectType, name="segment_object_type"),
        nullable=False,
        index=True,
    ),
    sqlalchemy.Column("valid_from", DateTime(timezone=True), nullable=False),
    sqlalchemy.Column("valid_to", DateTime(timezone=True), nullable=True),
    sqlalchemy.UniqueConstraint(
        "segment_id",
        "object_id",
        "object_type",
        "valid_from",
        "valid_to",
        name="uq_svo_unique_object_per_move",
    ),
)

Index(
    "ix_svo_segment_valid_from",
    segment_objects.c.segment_id,
    segment_objects.c.valid_from,
)
Index(
    "ix_svo_segment_valid_to", segment_objects.c.segment_id, segment_objects.c.valid_to
)

user_permissions = sqlalchemy.Table(
    "user_permissions",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True, autoincrement=True),
    sqlalchemy.Column("user_id", Integer, ForeignKey("relation_tg_cashboxes.id")),
    sqlalchemy.Column(
        "section", String, nullable=False
    ),  # Название раздела (payments, payboxes)
    sqlalchemy.Column("can_view", Boolean, default=True),
    sqlalchemy.Column("can_edit", Boolean, default=False),
    sqlalchemy.Column(
        "paybox_id", Integer, ForeignKey("payboxes.id"), nullable=True
    ),  # Доступ к конкретному счету
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

amo_docs_sales_delivery_contragents = sqlalchemy.Table(
    "amo_docs_sales_delivery_contragents",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True, autoincrement=True),
    sqlalchemy.Column(
        "delivery_info_id",
        Integer,
        ForeignKey("docs_sales_delivery_info.id"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "contragent_id", Integer, ForeignKey("contragents.id"), nullable=False
    ),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

tags = sqlalchemy.Table(
    "tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("emoji", String, nullable=True),
    sqlalchemy.Column("color", String, nullable=True),
    sqlalchemy.Column("description", String, nullable=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint(
        "name", "cashbox_id", name="unique_name_cashbox_id_tags"
    ),
)

contragents_tags = sqlalchemy.Table(
    "contragents_tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("tag_id", Integer, ForeignKey("tags.id"), nullable=False),
    sqlalchemy.Column(
        "contragent_id", Integer, ForeignKey("contragents.id"), nullable=False
    ),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint(
        "tag_id", "contragent_id", name="unique_tag_id_contragent_id"
    ),
)

segments_tags = sqlalchemy.Table(
    "segments_tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("tag_id", Integer, ForeignKey("tags.id"), nullable=False),
    sqlalchemy.Column("segment_id", Integer, ForeignKey("segments.id"), nullable=False),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint(
        "tag_id", "segment_id", name="unique_tag_id_segment_id"
    ),
)

amo_lead_contacts = sqlalchemy.Table(
    "amo_lead_contacts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("lead_id", BigInteger),
    sqlalchemy.Column("contact_id", BigInteger),
    sqlalchemy.Column("is_main", Boolean, server_default="true"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    UniqueConstraint(
        "amo_install_group_id",
        "lead_id",
        "contact_id",
        name="uq_amo_lead_contacts_group_lead_contact",
    ),
)

amo_webhooks = sqlalchemy.Table(
    "amo_webhooks",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "amo_install_group_id", Integer, ForeignKey("amo_install_groups.id")
    ),
    sqlalchemy.Column("amo_id", BigInteger),
    sqlalchemy.Column("created_by", BigInteger),
    sqlalchemy.Column("created_at", Integer),
    sqlalchemy.Column("updated_at", Integer),
    sqlalchemy.Column("sort", Integer),
    sqlalchemy.Column("disabled", Boolean, default=False),
    sqlalchemy.Column("destination", String),
    sqlalchemy.Column(
        "settings",
        ARRAY(String()),
        nullable=False,
        default=list,
        server_default=text("'{}'::text[]"),
    ),
    UniqueConstraint(
        "amo_install_group_id",
        "amo_id",
        name="uq_amo_webhooks_amo_install_group_id_amo_id",
    ),
)

SQLALCHEMY_DATABASE_URL = f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASS')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/cash_2"
SQLALCHEMY_DATABASE_URL_ASYNC = f"postgresql+asyncpg://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASS')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/cash_2"
SQLALCHEMY_DATABASE_URL_JOB_STORE = f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASS')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/cash_job_store"
database = databases.Database(
    SQLALCHEMY_DATABASE_URL,
    min_size=5,
    max_size=20,
    statement_cache_size=0,
)
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URL)
engine_job_store = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URL_JOB_STORE)

async_engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL_ASYNC, pool_pre_ping=True, poolclass=NullPool
)
async_session_maker = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)


class Role(enum.Enum):
    general = "general"
    picker = "picker"
    courier = "courier"


docs_sales_links = sqlalchemy.Table(
    "docs_sales_links",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column(
        "docs_sales_id",
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("docs_sales.id"),
        nullable=False,
    ),
    sqlalchemy.Column("role", sqlalchemy.Enum(Role), nullable=False),
    sqlalchemy.Column("hash", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("url", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, default=datetime.datetime.now),
    sqlalchemy.Column(
        "updated_at",
        sqlalchemy.DateTime,
        default=datetime.datetime.now,
        onupdate=datetime.datetime.now,
    ),
    sqlalchemy.UniqueConstraint(
        "docs_sales_id", "role", name="uix_docs_sales_links_docs_sales_id_role"
    ),
)

employee_shifts = sqlalchemy.Table(
    "employee_shifts",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "user_id",
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("relation_tg_cashboxes.id"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "cashbox_id",
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("cashboxes.id"),
        nullable=False,
    ),
    sqlalchemy.Column("shift_start", sqlalchemy.DateTime, nullable=False),
    sqlalchemy.Column("shift_end", sqlalchemy.DateTime, nullable=True),
    sqlalchemy.Column(
        "status",
        Enum("on_shift", "off_shift", "on_break", name="shiftstatus"),
        nullable=False,
        server_default="off_shift",
    ),
    sqlalchemy.Column("break_start", sqlalchemy.DateTime, nullable=True),
    sqlalchemy.Column("break_duration", sqlalchemy.Integer, nullable=True),  # в минутах
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        sqlalchemy.DateTime,
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

employee_shifts_events = sqlalchemy.Table(
    "employee_shifts_events",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "relation_id",
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("relation_tg_cashboxes.id"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "cashbox_id",
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("cashboxes.id"),
        nullable=False,
    ),
    sqlalchemy.Column("shift_status", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("event_start", sqlalchemy.DateTime, nullable=False),
    sqlalchemy.Column("event_end", sqlalchemy.DateTime, nullable=True),
)

nomenclature_hash = sqlalchemy.Table(
    "nomenclature_hash",
    metadata,
    sqlalchemy.Column("id", BigInteger, primary_key=True),
    sqlalchemy.Column(
        "nomenclature_id", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column("hash", String, nullable=False),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, default=datetime.datetime.now),
    sqlalchemy.Column(
        "updated_at",
        sqlalchemy.DateTime,
        default=datetime.datetime.now,
        onupdate=datetime.datetime.now,
    ),
)

warehouse_hash = sqlalchemy.Table(
    "warehouse_hash",
    metadata,
    sqlalchemy.Column("id", BigInteger, primary_key=True),
    sqlalchemy.Column(
        "warehouses_id", Integer, ForeignKey("warehouses.id"), nullable=False
    ),
    sqlalchemy.Column("hash", String, nullable=False),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, default=datetime.datetime.now),
    sqlalchemy.Column(
        "updated_at",
        sqlalchemy.DateTime,
        default=datetime.datetime.now,
        onupdate=datetime.datetime.now,
    ),
)

apple_push_tokens = sqlalchemy.Table(
    "apple_push_tokens",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column(
        "card_id",
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("loyality_cards.id"),
        nullable=False,
    ),
    sqlalchemy.Column("device_library_identifier", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("pass_type_id", String, nullable=False),
    sqlalchemy.Column("serial_number", String, nullable=False),
    sqlalchemy.Column("push_token", String, nullable=False),
    # sqlalchemy.Column("have_updates", Boolean, nullable=False, default=False, server_default=sqlalchemy.sql.expression.false()),
)

apple_wallet_card_settings = sqlalchemy.Table(
    "apple_wallet_card_settings",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column(
        "cashbox_id",
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("cashboxes.id"),
        nullable=False,
        unique=True,
    ),
    sqlalchemy.Column("data", sqlalchemy.JSON, nullable=True),
)

feeds = sqlalchemy.Table(
    "feeds",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("url_token", String, unique=True, nullable=False, index=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("description", String, nullable=True),
    sqlalchemy.Column("root_tag", String, nullable=False),
    sqlalchemy.Column("item_tag", String, nullable=False),
    sqlalchemy.Column("field_tags", JSON, nullable=True),
    sqlalchemy.Column("criteria", JSON, nullable=True),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id"), index=True),
    # Поля для синхронизации с Tilda
    sqlalchemy.Column("tilda_url", String, nullable=True),
    sqlalchemy.Column("tilda_username", String, nullable=True),
    sqlalchemy.Column("tilda_password", String, nullable=True),
    sqlalchemy.Column(
        "tilda_sync_enabled", sqlalchemy.Boolean, default=False, nullable=False
    ),
    sqlalchemy.Column(
        "tilda_sync_interval", Integer, nullable=True
    ),  # Интервал в минутах
    sqlalchemy.Column(
        "tilda_price_id", String, nullable=True
    ),  # ID цены в системе учета
    sqlalchemy.Column(
        "tilda_discount_price_id", String, nullable=True
    ),  # ID цены со скидкой
    sqlalchemy.Column(
        "tilda_catalog_id", String, nullable=True
    ),  # ID каталога в системе учета
    sqlalchemy.Column(
        "tilda_warehouse_id", String, nullable=True
    ),  # ID склада (пусто = все склады)
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, default=datetime.datetime.now),
    sqlalchemy.Column(
        "updated_at",
        sqlalchemy.DateTime,
        default=datetime.datetime.now,
        onupdate=datetime.datetime.now,
    ),
)

commerceml_connections = sqlalchemy.Table(
    "commerceml_connections",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("url", String, nullable=False),
    sqlalchemy.Column("username", String, nullable=False),
    sqlalchemy.Column("password", String, nullable=False),
    sqlalchemy.Column("active", Boolean, default=True, nullable=False),
    sqlalchemy.Column("import_products", Boolean, default=True, nullable=False),
    sqlalchemy.Column("export_products", Boolean, default=True, nullable=False),
    sqlalchemy.Column("import_orders", Boolean, default=True, nullable=False),
    sqlalchemy.Column("export_orders", Boolean, default=True, nullable=False),
    sqlalchemy.Column("products_loaded_count", Integer, default=0, nullable=False),
    sqlalchemy.Column("orders_exported_count", Integer, default=0, nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.Column("is_deleted", Boolean, default=False, nullable=False),
)

feeds_tags = sqlalchemy.Table(
    "feeds_tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("tag_id", Integer, ForeignKey("tags.id"), nullable=False),
    sqlalchemy.Column("feed_id", Integer, ForeignKey("feeds.id"), nullable=False),
    sqlalchemy.Column("cashbox_id", Integer, ForeignKey("cashboxes.id")),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint("tag_id", "feed_id", name="unique_tag_id_feed_id"),
)

marketplace_clients_list = sqlalchemy.Table(
    "marketplace_clients_list",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("phone", String, nullable=False, unique=True, index=True),
    sqlalchemy.Column("first_name", String, nullable=True),
    sqlalchemy.Column("last_name", String, nullable=True),
    sqlalchemy.Column("utm_source", String, nullable=True),
    sqlalchemy.Column("utm_medium", String, nullable=True),
    sqlalchemy.Column("utm_campaign", String, nullable=True),
    sqlalchemy.Column("utm_term", ARRAY(item_type=String), nullable=True),
    sqlalchemy.Column("ref_user", String, nullable=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

marketplace_favorites = sqlalchemy.Table(
    "marketplace_favorites",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "phone",
        String,
        nullable=True,  # Deprecated, use client_id instead
        index=True,
    ),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey(
            "marketplace_clients_list.id", ondelete="CASCADE", onupdate="CASCADE"
        ),
        nullable=False,
        index=True,
    ),
    sqlalchemy.Column("entity_type", String(50), nullable=False),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    UniqueConstraint(
        "client_id",
        "entity_type",
        "entity_id",
        name="uq_marketplace_favorites_client_entity",
    ),
)

marketplace_carts = sqlalchemy.Table(
    "marketplace_carts",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "phone",
        String,
        nullable=True,  # Deprecated, use client_id instead
        unique=True,  # один активный cart на телефон
        index=True,
    ),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey(
            "marketplace_clients_list.id", ondelete="CASCADE", onupdate="CASCADE"
        ),
        nullable=False,
        unique=True,  # один активный cart на клиента
        index=True,
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

marketplace_cart_goods = sqlalchemy.Table(
    "marketplace_cart_goods",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "nomenclature_id", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column(
        "warehouse_id", Integer, ForeignKey("warehouses.id"), nullable=True
    ),
    sqlalchemy.Column("quantity", Integer, nullable=False),
    sqlalchemy.Column(
        "cart_id",
        BigInteger,
        ForeignKey("marketplace_carts.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint(
        "nomenclature_id",
        "warehouse_id",
        "cart_id",
        name="ux_marketplace_cart_goods_nomenclature_id_warehouse_id_cart_id",
    ),
)

marketplace_orders = sqlalchemy.Table(
    "marketplace_orders",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "phone",
        String,
        nullable=True,  # Deprecated, use client_id instead
        index=True,
    ),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey(
            "marketplace_clients_list.id", ondelete="RESTRICT", onupdate="CASCADE"
        ),
        nullable=False,
        index=True,
    ),
    sqlalchemy.Column(
        "docs_sales_id",
        BigInteger,
        ForeignKey("docs_sales.id", ondelete="SET NULL", onupdate="CASCADE"),
        nullable=True,
        index=True,
    ),
    sqlalchemy.Column("status", String(32), nullable=False, server_default="created"),
    sqlalchemy.Column("delivery_info", JSONB, nullable=False),
    sqlalchemy.Column(
        "additional_data", JSONB, nullable=False, server_default=text("'[]'::jsonb")
    ),
    sqlalchemy.Column("error", Text, nullable=True),
    sqlalchemy.Column(
        "recipient_name",
        String,
        nullable=True,  # Имя получателя из конкретного заказа (может отличаться от имени в marketplace_clients_list)
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

marketplace_searches = sqlalchemy.Table(
    "marketplace_searches",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "phone",
        String,
        nullable=True,  # Deprecated, use client_id instead
        index=True,
    ),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey(
            "marketplace_clients_list.id", ondelete="SET NULL", onupdate="CASCADE"
        ),
        nullable=True,
        index=True,
    ),
    sqlalchemy.Column("query", Text, nullable=False),
    sqlalchemy.Column("filters", JSONB, nullable=True),
    sqlalchemy.Column("results_count", Integer, nullable=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
)

global_categories = sqlalchemy.Table(
    "global_categories",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("description", String),
    sqlalchemy.Column("code", Integer),
    sqlalchemy.Column("parent_id", Integer),
    sqlalchemy.Column("external_id", String),
    sqlalchemy.Column("image_url", String),
    sqlalchemy.Column("is_active", Boolean, default=True),
    sqlalchemy.Column("has_products", Boolean, nullable=True, server_default="false"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

channels = sqlalchemy.Table(
    "channels",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("name", String(50), nullable=False),
    sqlalchemy.Column("type", Enum(ChannelType), nullable=False),
    sqlalchemy.Column("description", String(255), nullable=True),
    sqlalchemy.Column("svg_icon", Text, nullable=True),
    sqlalchemy.Column("tags", JSON, nullable=True),
    sqlalchemy.Column("api_config_name", String(100), nullable=True),
    sqlalchemy.Column("is_active", Boolean, nullable=False, server_default="true"),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    sqlalchemy.Column(
        "created_at", DateTime, nullable=False, server_default=func.now()
    ),
    sqlalchemy.Column(
        "updated_at", DateTime, nullable=False, server_default=func.now()
    ),
    sqlalchemy.Index(
        "uq_channels_name_when_active",
        "name",
        unique=True,
        postgresql_where=text("is_active IS TRUE"),
    ),
)

chat_contacts = sqlalchemy.Table(
    "chat_contacts",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("external_contact_id", String(255), nullable=True),
    sqlalchemy.Column("name", String(100), nullable=True),
    sqlalchemy.Column("phone", String(20), nullable=True),
    sqlalchemy.Column("email", String(255), nullable=True),
    sqlalchemy.Column("avatar", String(500), nullable=True),
    sqlalchemy.Column("last_activity", BigInteger, nullable=True),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    sqlalchemy.Column(
        "contragent_id",
        Integer,
        ForeignKey("contragents.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sqlalchemy.Column(
        "created_at", DateTime, nullable=False, server_default=func.now()
    ),
    sqlalchemy.Column(
        "updated_at", DateTime, nullable=False, server_default=func.now()
    ),
    UniqueConstraint(
        "cashbox_id", "external_contact_id", name="uq_chat_contacts_cashbox_external"
    ),
)

chats = sqlalchemy.Table(
    "chats",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "channel_id",
        Integer,
        ForeignKey("channels.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "chat_contact_id",
        Integer,
        ForeignKey("chat_contacts.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sqlalchemy.Column(
        "cashbox_id",
        Integer,
        ForeignKey("cashboxes.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column("external_chat_id", String(255), nullable=False),
    sqlalchemy.Column(
        "status", Enum(ChatStatus), nullable=False, server_default="ACTIVE"
    ),
    sqlalchemy.Column(
        "assigned_operator_id",
        Integer,
        ForeignKey("relation_tg_cashboxes.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sqlalchemy.Column("first_message_time", DateTime, nullable=True),
    sqlalchemy.Column("first_response_time_seconds", Integer, nullable=True),
    sqlalchemy.Column("last_message_time", DateTime, nullable=True),
    sqlalchemy.Column("last_response_time_seconds", Integer, nullable=True),
    sqlalchemy.Column(
        "from_qr", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column(
        "qr_page_id",
        Integer,
        ForeignKey("qr_pages.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sqlalchemy.Column("metadata", JSON, nullable=True),
    sqlalchemy.Column("name", String(100), nullable=True),
    sqlalchemy.Column(
        "created_at", DateTime, nullable=False, server_default=func.now()
    ),
    sqlalchemy.Column(
        "updated_at", DateTime, nullable=False, server_default=func.now()
    ),
    UniqueConstraint(
        "channel_id",
        "external_chat_id",
        "cashbox_id",
        name="uq_chats_channel_external_cashbox",
    ),
)

chat_contact_links = sqlalchemy.Table(
    "chat_contact_links",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "chat_id",
        Integer,
        ForeignKey("chats.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "contact_id",
        Integer,
        ForeignKey("chat_contacts.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column("role", String(32), nullable=False, server_default="participant"),
    sqlalchemy.Column(
        "created_at", DateTime, nullable=False, server_default=func.now()
    ),
    UniqueConstraint(
        "chat_id", "contact_id", name="uq_chat_contact_links_chat_contact"
    ),
)

chat_messages = sqlalchemy.Table(
    "chat_messages",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "chat_id", Integer, ForeignKey("chats.id", ondelete="CASCADE"), nullable=False
    ),
    sqlalchemy.Column("sender_type", Enum(MessageSenderType), nullable=False),
    sqlalchemy.Column(
        "message_type", Enum(MessageType), nullable=False, server_default="TEXT"
    ),
    sqlalchemy.Column("content", Text, nullable=False),
    sqlalchemy.Column("external_message_id", String(255), nullable=True),
    sqlalchemy.Column(
        "status", Enum(MessageStatus), nullable=False, server_default="SENT"
    ),
    sqlalchemy.Column(
        "source",
        String(50),
        nullable=True,
        comment="Источник отправки сообщения: ios, android, api, web, avito, etc.",
    ),
    sqlalchemy.Column(
        "created_at", DateTime, nullable=False, server_default=func.now()
    ),
    sqlalchemy.Column(
        "updated_at", DateTime, nullable=False, server_default=func.now()
    ),
)

relation_message_files = sqlalchemy.Table(
    "relation_message_files",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "message_id",
        Integer,
        ForeignKey("chat_messages.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "file_id", Integer, ForeignKey("files.id", ondelete="CASCADE"), nullable=False
    ),
)

channel_credentials = sqlalchemy.Table(
    "channel_credentials",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "channel_id",
        Integer,
        ForeignKey("channels.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column(
        "cashbox_id",
        Integer,
        ForeignKey("cashboxes.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sqlalchemy.Column("api_key", String(500), nullable=False),
    sqlalchemy.Column("api_secret", String(500), nullable=False),
    sqlalchemy.Column("access_token", String(1000), nullable=True),
    sqlalchemy.Column("refresh_token", String(1000), nullable=True),
    sqlalchemy.Column("token_expires_at", DateTime, nullable=True),
    sqlalchemy.Column("avito_user_id", Integer, nullable=True),
    sqlalchemy.Column("redirect_uri", String(500), nullable=True),
    sqlalchemy.Column("is_active", Boolean, nullable=False, server_default="true"),
    sqlalchemy.Column(
        "auto_sync_chats_enabled", Boolean, nullable=False, server_default="false"
    ),
    sqlalchemy.Column("last_status_code", Integer, nullable=True),
    sqlalchemy.Column("last_status_check_at", DateTime, nullable=True),
    sqlalchemy.Column("connection_status", String(50), nullable=True),
    sqlalchemy.Column("tariff_paid_status", Boolean, nullable=True),
    sqlalchemy.Column("tariff_last_check_at", DateTime, nullable=True),
    sqlalchemy.Column(
        "created_at", DateTime, nullable=False, server_default=func.now()
    ),
    sqlalchemy.Column(
        "updated_at", DateTime, nullable=False, server_default=func.now()
    ),
    UniqueConstraint(
        "channel_id", "cashbox_id", name="uq_channel_credentials_channel_cashbox"
    ),
    sqlalchemy.Index(
        "ix_channel_credentials_cashbox_active", "cashbox_id", "is_active"
    ),
)

marketplace_rating_aggregates = sqlalchemy.Table(
    "marketplace_rating_aggregates",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("entity_type", String, nullable=False),
    sqlalchemy.Column("avg_rating", Float, nullable=False),
    sqlalchemy.Column("reviews_count", Integer, nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

marketplace_utm_tags = sqlalchemy.Table(
    "marketplace_utm_tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("entity_type", String, nullable=True),
    sqlalchemy.Column("entity_id", BigInteger, nullable=False),
    sqlalchemy.Column("utm_source", String, nullable=True),
    sqlalchemy.Column("utm_medium", String, nullable=True),
    sqlalchemy.Column("utm_campaign", String, nullable=True),
    sqlalchemy.Column("utm_term", ARRAY(item_type=String), nullable=True),
    sqlalchemy.Column("utm_content", String, nullable=True),
    sqlalchemy.Column("utm_name", String, nullable=True),
    sqlalchemy.Column("utm_phone", String, nullable=True),
    sqlalchemy.Column("utm_email", String, nullable=True),
    sqlalchemy.Column("utm_leadid", String, nullable=True),
    sqlalchemy.Column("utm_yclientid", String, nullable=True),
    sqlalchemy.Column("utm_gaclientid", String, nullable=True),
    sqlalchemy.Column("ref_user", String, nullable=True),
    sqlalchemy.Column(
        "created_at", DateTime(timezone=True), server_default=func.now(), nullable=False
    ),
)

marketplace_reviews = sqlalchemy.Table(
    "marketplace_reviews",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("entity_type", String, nullable=False),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey("marketplace_clients_list.id"),
        nullable=False,
    ),
    sqlalchemy.Column("rating", Integer, nullable=False),
    sqlalchemy.Column("text", Text, nullable=False),
    sqlalchemy.Column("status", String, nullable=False, server_default="visible"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

marketplace_view_events = sqlalchemy.Table(
    "marketplace_view_events",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("entity_type", String, nullable=False),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("listing_pos", Integer, nullable=True),
    sqlalchemy.Column("listing_page", Integer, nullable=True),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey("marketplace_clients_list.id"),
        nullable=False,
    ),
    sqlalchemy.Column("event", String, nullable=False, server_default="view"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
)


system_settings = sqlalchemy.Table(
    "system_settings",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("key", String, unique=True, nullable=False, index=True),
    sqlalchemy.Column("value", Text, nullable=False),
    sqlalchemy.Column("description", Text, nullable=True),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

blog_sites = sqlalchemy.Table(
    "blog_sites",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("name", String(255), nullable=False),
    sqlalchemy.Column("token", String(255), nullable=False, unique=True, index=True),
    sqlalchemy.Column(
        "is_active", Boolean, nullable=False, server_default=sqlalchemy.true()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

blog_folders = sqlalchemy.Table(
    "blog_folders",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("name", String(255), nullable=False),
    sqlalchemy.Column("slug", String(255), nullable=False),
    sqlalchemy.Column(
        "parent_id", BigInteger, ForeignKey("blog_folders.id"), nullable=True
    ),
    sqlalchemy.Column("path", String(1024), nullable=False),
    sqlalchemy.Column("settings", JSONB, nullable=True),
    sqlalchemy.Column(
        "is_active", Boolean, nullable=False, server_default=sqlalchemy.true()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    UniqueConstraint(
        "cashbox_id", "parent_id", "slug", name="uq_blog_folders_cashbox_parent_slug"
    ),
)

Index(
    "ix_blog_folders_cashbox_parent",
    blog_folders.c.cashbox_id,
    blog_folders.c.parent_id,
)
Index("ix_blog_folders_cashbox_path", blog_folders.c.cashbox_id, blog_folders.c.path)

blog_site_folders = sqlalchemy.Table(
    "blog_site_folders",
    metadata,
    sqlalchemy.Column(
        "site_id", BigInteger, ForeignKey("blog_sites.id"), nullable=False
    ),
    sqlalchemy.Column(
        "folder_id", BigInteger, ForeignKey("blog_folders.id"), nullable=False
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.PrimaryKeyConstraint(
        "site_id", "folder_id", name="pk_blog_site_folders"
    ),
)

Index("ix_blog_site_folders_folder", blog_site_folders.c.folder_id)


blog_posts = sqlalchemy.Table(
    "blog_posts",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    sqlalchemy.Column(
        "folder_id",
        BigInteger,
        ForeignKey("blog_folders.id"),
        nullable=False,
        index=True,
    ),
    sqlalchemy.Column("title", String(512), nullable=False),
    sqlalchemy.Column("slug", String(255), nullable=False),
    sqlalchemy.Column("excerpt", Text, nullable=True),
    sqlalchemy.Column("content", Text, nullable=False),
    sqlalchemy.Column(
        "status", String(32), nullable=False, server_default=text("'DRAFT'")
    ),
    sqlalchemy.Column("published_at", DateTime(timezone=True), nullable=True),
    sqlalchemy.Column("meta", JSONB, nullable=True),
    sqlalchemy.Column(
        "is_deleted", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    UniqueConstraint("cashbox_id", "slug", name="uq_blog_posts_cashbox_slug"),
)
Index(
    "ix_blog_posts_status_published_at", blog_posts.c.status, blog_posts.c.published_at
)

blog_tags = sqlalchemy.Table(
    "blog_tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("name", String(255), nullable=False),
    sqlalchemy.Column("slug", String(255), nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    UniqueConstraint("cashbox_id", "slug", name="uq_blog_tags_cashbox_slug"),
)

blog_post_tags = sqlalchemy.Table(
    "blog_post_tags",
    metadata,
    sqlalchemy.Column(
        "post_id", BigInteger, ForeignKey("blog_posts.id"), nullable=False
    ),
    sqlalchemy.Column("tag_id", BigInteger, ForeignKey("blog_tags.id"), nullable=False),
    sqlalchemy.PrimaryKeyConstraint("post_id", "tag_id", name="pk_blog_post_tags"),
)

Index("ix_blog_post_tags_tag", blog_post_tags.c.tag_id)

folders = sqlalchemy.Table(
    "folders",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, autoincrement=True, index=True
    ),
    sqlalchemy.Column("name", String, nullable=False),
    sqlalchemy.Column("slug", String, nullable=False),
    sqlalchemy.Column(
        "cashbox_id", ForeignKey("cashboxes.id"), index=True, nullable=False
    ),
    sqlalchemy.Column("owner_id", ForeignKey("relation_tg_cashboxes.id"), index=True),
    sqlalchemy.Column(
        "previous_id", ForeignKey("folders.id"), nullable=True, index=True
    ),
    sqlalchemy.Column("settings", JSONB, nullable=True),
    sqlalchemy.Column(
        "is_active", Boolean, nullable=False, server_default=sqlalchemy.true()
    ),
    sqlalchemy.Column(
        "is_deleted", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

Index(
    "ix_folders_cashbox_previous",
    folders.c.cashbox_id,
    folders.c.previous_id,
)

folders_entities = sqlalchemy.Table(
    "folders_entities",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, autoincrement=True, index=True
    ),
    sqlalchemy.Column("entity_type", String, nullable=False),
    sqlalchemy.Column("entity_id", BigInteger, nullable=False, index=True),
    sqlalchemy.Column(
        "folder_id", ForeignKey("folders.id"), nullable=False, index=True
    ),
    sqlalchemy.Column("owner_id", ForeignKey("relation_tg_cashboxes.id"), index=True),
    sqlalchemy.Column(
        "is_deleted", Boolean, nullable=False, server_default=sqlalchemy.false()
    ),
    sqlalchemy.Column(
        "is_active", Boolean, nullable=False, server_default=sqlalchemy.true()
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    UniqueConstraint(
        "entity_type",
        "entity_id",
        "folder_id",
        name="uq_folder_entity_type_id_folder_id",
    ),
)

Index(
    "ix_folder_entity_id_folder",
    folders_entities.c.entity_id,
    folders_entities.c.folder_id,
)


tg_bot_settings = sqlalchemy.Table(
    "tg_bot_settings",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, autoincrement=True, index=True
    ),
    sqlalchemy.Column("tg_account_id", ForeignKey("tg_accounts.id"), index=True),
    sqlalchemy.Column("settings", JSONB, nullable=True),
)

Index("ix_tg_bot_settings_tg_account_id", tg_bot_settings.c.tg_account_id)
