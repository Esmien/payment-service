"""Тесты моделей Payment и OutboxEvent на реальной PostgreSQL."""

from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.constants import Currency, OutboxStatus, PaymentStatus
from models import OutboxEvent, Payment


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
