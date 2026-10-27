"""Тесты ручного replay webhook: HTTP вне транзакции и at-least-once."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.constants import Currency, PaymentStatus, WebhookStatus
from core.uow import UnitOfWork
from models import Payment
from schemas import PaymentDetail, WebhookPayload
from services.webhook_replay import WebhookReplayService


def _build_payment() -> Payment:
    """Собирает завершенный платёж с неудачной доставкой webhook.

    Returns:
        Несохраненная ORM-модель платежа.
    """
    now = datetime.now(tz=UTC)
    return Payment(
        id=uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB.value,
        description="Тестовый платеж",
        metadata_={},
        status=PaymentStatus.SUCCEEDED.value,
        idempotency_key=f"key-{uuid4()}",
        webhook_url="https://example.com/webhook",
        created_at=now,
        processed_at=now,
        webhook_status=WebhookStatus.FAILED.value,
        webhook_attempts=3,
        webhook_last_error="previous failure",
    )


class BlockingWebhookSender:
    """Отправитель, который ждет нужного числа вызовов и проверяет транзакцию."""

    def __init__(self, *, uow: UnitOfWork | None = None, required_calls: int = 1) -> None:
        """Инициализирует отправитель.

        Args:
            uow: Unit of Work, у которого проверяется отсутствие транзакции.
            required_calls: Сколько вызовов нужно дождаться перед release.
        """
        self.calls = 0
        self.required_calls = required_calls
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.transaction_open_during_send: bool | None = None
        self._uow = uow

    async def send(self, *, url: str, payload: WebhookPayload) -> int:
        """Запоминает вызов, проверяет транзакцию и ждет разрешения.

        Args:
            url: URL клиента.
            payload: Тело уведомления.

        Returns:
            Количество выполненных попыток.
        """
        self.calls += 1
        if self._uow is not None:
            # В этот момент транзакция чтения уже закоммичена.
            self.transaction_open_during_send = self._uow.session.in_transaction()
        if self.calls >= self.required_calls:
            self.started.set()
        await self.release.wait()
        return 1


async def test_replay_commits_read_before_http_and_persists_result(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Replay коммитит чтение до HTTP и сохраняет delivered после отправки."""
    async with session_factory() as session:
        payment = _build_payment()
        session.add(payment)
        await session.commit()
        payment_id = payment.id

    async with session_factory() as session:
        uow = UnitOfWork(session=session)
        sender = BlockingWebhookSender(uow=uow)
        sender.release.set()
        service = WebhookReplayService(uow=uow, webhook_client=sender)

        result = await service.replay(payment_id=payment_id)

    assert sender.transaction_open_during_send is False
    assert result.webhook_status is WebhookStatus.DELIVERED

    async with session_factory() as session:
        stored = await session.get(Payment, payment_id)
        assert stored is not None
        assert stored.webhook_status == WebhookStatus.DELIVERED.value
        assert stored.webhook_attempts == 4
        assert stored.webhook_last_error is None


async def test_replay_allows_concurrent_deliveries(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Два параллельных replay могут доставить webhook дважды: at-least-once."""
    async with session_factory() as session:
        payment = _build_payment()
        session.add(payment)
        await session.commit()
        payment_id = payment.id

    sender = BlockingWebhookSender(required_calls=2)

    async def _run_replay() -> PaymentDetail:
        """Выполняет replay в отдельной сессии."""
        async with session_factory() as session:
            uow = UnitOfWork(session=session)
            service = WebhookReplayService(uow=uow, webhook_client=sender)
            return await service.replay(payment_id=payment_id)

    first_replay = asyncio.create_task(_run_replay())
    second_replay = asyncio.create_task(_run_replay())

    await sender.started.wait()
    sender.release.set()
    results = await asyncio.gather(first_replay, second_replay)

    assert sender.calls == 2
    assert all(result.webhook_status is WebhookStatus.DELIVERED for result in results)

    async with session_factory() as session:
        stored = await session.get(Payment, payment_id)
        assert stored is not None
        assert stored.webhook_status == WebhookStatus.DELIVERED.value
        # Оба replay прочитали attempts=3 и записали абсолютное значение 4.
        assert stored.webhook_attempts == 4
