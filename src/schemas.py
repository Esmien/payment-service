"""Pydantic v2 схемы запросов, ответов, сообщений очереди и вебхуков."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field

from core.constants import Currency, PaymentStatus


class CreatePaymentRequest(BaseModel):
    """Тело запроса на создание платежа.

    Attributes:
        amount: Сумма платежа, строго больше нуля, не более двух знаков после запятой.
        currency: Валюта платежа (RUB, USD, EUR).
        description: Описание платежа.
        metadata: Дополнительные метаданные платежа.
        webhook_url: URL для уведомления о результате обработки.
    """

    # max_digits=12 и decimal_places=2 совпадают с Numeric(12, 2) в модели Payment.
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    currency: Currency
    description: str
    # default_factory вместо литерала {}, чтобы каждый экземпляр получал собственный словарь.
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl


class PaymentCreatedResponse(BaseModel):
    """Ответ на создание платежа (202 Accepted).

    Attributes:
        payment_id: Идентификатор созданного платежа.
        status: Текущий статус платежа.
        created_at: Дата и время создания платежа.
    """

    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    """Детальная информация о платеже (200 OK).

    Attributes:
        payment_id: Идентификатор платежа.
        amount: Сумма платежа.
        currency: Валюта платежа.
        description: Описание платежа.
        metadata: Дополнительные метаданные платежа.
        status: Текущий статус платежа.
        idempotency_key: Ключ идемпотентности, с которым создан платеж.
        webhook_url: URL для уведомления о результате обработки.
        created_at: Дата и время создания платежа.
        processed_at: Дата и время завершения обработки шлюзом, если обработка завершена.
    """

    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    # В модели Payment атрибут называется metadata_, т.к. metadata зарезервировано в DeclarativeBase.
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentEventPayload(BaseModel):
    """Тело сообщения очереди payments.new.

    Attributes:
        payment_id: Идентификатор платежа, который нужно обработать.
    """

    payment_id: UUID


class WebhookPayload(BaseModel):
    """Тело webhook-уведомления, отправляемого на webhook_url клиента.

    Attributes:
        payment_id: Идентификатор платежа.
        status: Итоговый статус платежа (succeeded или failed).
        amount: Сумма платежа.
        currency: Валюта платежа.
        metadata: Дополнительные метаданные платежа.
        processed_at: Дата и время завершения обработки шлюзом.
    """

    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any]
    processed_at: datetime
