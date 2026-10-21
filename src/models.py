"""Модели SQLAlchemy 2.0: платежи и события outbox."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Numeric, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from core.constants import OutboxStatus, PaymentStatus
from core.engine import Base


class Payment(Base):
    """Платеж, созданный клиентом через API.

    Attributes:
        id: Уникальный идентификатор платежа.
        amount: Сумма платежа с двумя знаками после запятой.
        currency: Код валюты (RUB, USD, EUR).
        description: Описание платежа.
        metadata_: Дополнительные метаданные (колонка `metadata`).
        status: Статус обработки платежа.
        idempotency_key: Ключ идемпотентности из заголовка запроса.
        webhook_url: URL для уведомления о результате.
        created_at: Дата и время создания.
        processed_at: Дата и время завершения обработки шлюзом.
    """

    __tablename__ = "payments"
    __table_args__ = (CheckConstraint("amount > 0", name="ck_payments_amount_positive"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    # Атрибут назван metadata_, т.к. имя metadata зарезервировано в DeclarativeBase.
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True, default=PaymentStatus.PENDING)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OutboxEvent(Base):
    """Событие transactional outbox, которое нужно опубликовать в брокер.

    Attributes:
        id: Уникальный идентификатор события (используется как message_id).
        event_type: Тип события (например, `payment.created`).
        payload: Тело события в формате JSON.
        status: Статус публикации (pending, processed).
        created_at: Дата и время создания записи.
        processed_at: Дата и время успешной публикации в брокер.
    """

    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True, default=OutboxStatus.PENDING)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
