"""Тесты моделей Payment и OutboxEvent на реальной PostgreSQL."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.constants import Currency, OutboxStatus, PaymentStatus, WebhookStatus
from models import OutboxEvent, Payment
from services.outbox import cleanup_processed_events


def _build_payment(*, idempotency_key: str) -> Payment:
    """Собирает валидный платеж для тестов.

    Args:
        idempotency_key: Ключ идемпотентности платежа.

    Returns:
        Несохраненный объект Payment.
    """
    return Payment(
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Тестовый платеж",
        metadata_={"order": 1},
        idempotency_key=idempotency_key,
        webhook_url="https://example.com/webhook",
    )


async def test_payment_is_persisted_with_defaults(session: AsyncSession) -> None:
    """Платеж сохраняется со статусом pending и корректными значениями по умолчанию."""
    payment = _build_payment(idempotency_key="key-defaults")
    session.add(payment)
    await session.commit()

    payment_id = payment.id
    session.expire_all()
    stored = await session.get(Payment, payment_id)

    assert stored is not None
    assert stored.status == PaymentStatus.PENDING
    assert stored.metadata_ == {"order": 1}
    assert stored.amount == Decimal("100.00")
    assert stored.created_at is not None
    assert stored.processed_at is None
    assert stored.webhook_status == WebhookStatus.PENDING
    assert stored.webhook_attempts == 0
    assert stored.webhook_last_error is None


async def test_outbox_event_is_persisted_with_pending_status(session: AsyncSession) -> None:
    """Событие outbox сохраняется со статусом pending и исходным payload."""
    payment = _build_payment(idempotency_key="key-outbox")
    session.add(payment)
    # flush назначает id платежу до того, как он попадет в payload события.
    await session.flush()
    payment_id = payment.id
    event = OutboxEvent(event_type="payment.created", payload={"payment_id": str(payment_id)})
    session.add(event)
    await session.commit()

    event_id = event.id
    session.expire_all()
    stored = await session.get(OutboxEvent, event_id)

    assert stored is not None
    assert stored.status == OutboxStatus.PENDING
    assert stored.payload == {"payment_id": str(payment_id)}
    assert stored.processed_at is None


async def test_idempotency_key_must_be_unique(session: AsyncSession) -> None:
    """Повторный idempotency_key приводит к IntegrityError на уровне БД."""
    session.add(_build_payment(idempotency_key="duplicate-key"))
    await session.commit()

    session.add(_build_payment(idempotency_key="duplicate-key"))
    with pytest.raises(IntegrityError):
        await session.commit()


async def test_amount_must_be_positive(session: AsyncSession) -> None:
    """Нулевая сумма отклоняется CHECK-ограничением БД."""
    payment = _build_payment(idempotency_key="key-zero-amount")
    payment.amount = Decimal("0")
    session.add(payment)
    with pytest.raises(IntegrityError):
        await session.commit()


async def test_cleanup_processed_events_deletes_only_old_processed(session: AsyncSession) -> None:
    """Retention удаляет старые processed-события, не трогая pending и свежие processed."""
    now = datetime.now(tz=UTC)
    old_processed = OutboxEvent(
        event_type="payment.created",
        payload={"payment_id": str(uuid4())},
        status=OutboxStatus.PROCESSED.value,
        created_at=now - timedelta(days=10),
        processed_at=now - timedelta(days=10),
    )
    fresh_processed = OutboxEvent(
        event_type="payment.created",
        payload={"payment_id": str(uuid4())},
        status=OutboxStatus.PROCESSED.value,
        created_at=now - timedelta(days=1),
        processed_at=now - timedelta(days=1),
    )
    old_pending = OutboxEvent(
        event_type="payment.created",
        payload={"payment_id": str(uuid4())},
        status=OutboxStatus.PENDING.value,
        created_at=now - timedelta(days=10),
    )
    session.add_all([old_processed, fresh_processed, old_pending])
    await session.commit()

    deleted_count = await cleanup_processed_events(
        session=session,
        older_than=now - timedelta(days=7),
    )

    assert deleted_count == 1
    remaining = (await session.execute(select(OutboxEvent))).scalars().all()
    assert {event.id for event in remaining} == {fresh_processed.id, old_pending.id}
