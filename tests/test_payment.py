"""Тесты сервиса создания и получения платежей (TASK-3.1)."""

import asyncio
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import Select, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.constants import EVENT_TYPE_PAYMENT_CREATED, Currency, OutboxStatus, PaymentStatus
from core.exceptions import IdempotencyConflictError, PaymentNotFoundError
from core.uow import UnitOfWork
from models import OutboxEvent, Payment
from repositories.payment_repo import PaymentRepository
from schemas import CreatePaymentRequest
from services.payment_service import PaymentService

TEST_IDEMPOTENCY_KEY = "test-key-1"


def _build_request(**overrides: Any) -> CreatePaymentRequest:
    """Собирает валидный запрос на создание платежа с возможностью переопределить поля.

    Args:
        **overrides: Поля запроса, которые нужно заменить.

    Returns:
        Экземпляр CreatePaymentRequest.
    """
    fields: dict[str, Any] = {
        "amount": Decimal("100.50"),
        "currency": Currency.RUB,
        "description": "Оплата заказа",
        "metadata": {"order_id": 42},
        "webhook_url": "https://example.com/webhook",
    }
    fields.update(overrides)
    return CreatePaymentRequest(**fields)


def _build_service(*, session: AsyncSession) -> PaymentService:
    """Создает сервис платежей поверх Unit of Work с переданной сессией.

    Args:
        session: Асинхронная сессия SQLAlchemy.

    Returns:
        Экземпляр PaymentService.
    """
    return PaymentService(uow=UnitOfWork(session=session))


class _FakeScalarSession:
    """Сессия, запоминающая SQL и не выполняющая его."""

    def __init__(self) -> None:
        """Инициализирует пустое хранилище запроса."""
        self.statement: Select[Any] | None = None

    async def scalar(self, statement: Select[Any]) -> None:
        """Запоминает переданный запрос.

        Args:
            statement: SQL-запрос для проверки.
        """
        self.statement = statement


async def test_get_by_id_for_update_uses_row_lock() -> None:
    """Загрузка платежа для consumer использует FOR UPDATE."""
    session = _FakeScalarSession()
    repository = PaymentRepository(session=session)  # type: ignore[arg-type]

    await repository.get_by_id_for_update(payment_id=uuid4())

    assert session.statement is not None
    compiled_sql = str(
        session.statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "FOR UPDATE" in compiled_sql


async def test_get_by_id_for_update_blocks_second_consumer(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Вторая транзакция ждет, пока первая держит FOR UPDATE на платеже."""
    async with session_factory() as session:
        service = _build_service(session=session)
        created_payment = await service.create_payment(
            idempotency_key="lock-key",
            request=_build_request(),
        )

    async with session_factory() as first_session, session_factory() as second_session:
        first_uow = UnitOfWork(session=first_session)
        second_uow = UnitOfWork(session=second_session)

        locked_payment = await first_uow.payment.get_by_id_for_update(payment_id=created_payment.payment_id)
        assert locked_payment is not None

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                second_uow.payment.get_by_id_for_update(payment_id=created_payment.payment_id),
                timeout=0.2,
            )

        await second_session.rollback()
        await first_session.rollback()


async def test_create_payment_saves_payment_and_outbox_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Создание платежа записывает платеж и событие outbox в одной транзакции."""
    async with session_factory() as session:
        service = _build_service(session=session)
        payment = await service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(),
        )

    async with session_factory() as session:
        saved_payment = await session.get(Payment, payment.payment_id)
        outbox_events = (await session.scalars(select(OutboxEvent))).all()

    assert saved_payment is not None
    assert saved_payment.status == PaymentStatus.PENDING
    assert saved_payment.idempotency_key == TEST_IDEMPOTENCY_KEY
    assert saved_payment.amount == Decimal("100.50")
    assert saved_payment.currency == Currency.RUB
    assert saved_payment.metadata_ == {"order_id": 42}
    assert saved_payment.webhook_url == "https://example.com/webhook"

    assert len(outbox_events) == 1
    outbox_event = outbox_events[0]
    assert outbox_event.event_type == EVENT_TYPE_PAYMENT_CREATED
    assert outbox_event.status == OutboxStatus.PENDING
    assert outbox_event.payload == {"payment_id": str(payment.payment_id)}


async def test_create_payment_with_same_body_returns_existing_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Повторный запрос с тем же ключом и телом возвращает существующий платеж без новых записей."""
    async with session_factory() as session:
        service = _build_service(session=session)
        first_payment = await service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(),
        )

    async with session_factory() as session:
        service = _build_service(session=session)
        second_payment = await service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(),
        )

    async with session_factory() as session:
        payments_count = await session.scalar(select(func.count()).select_from(Payment))
        outbox_count = await session.scalar(select(func.count()).select_from(OutboxEvent))

    assert second_payment.payment_id == first_payment.payment_id
    assert payments_count == 1
    assert outbox_count == 1


async def test_create_payment_with_different_body_raises_conflict(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Повтор ключа с другими параметрами поднимает IdempotencyConflictError и не создает записей."""
    async with session_factory() as session:
        service = _build_service(session=session)
        await service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(),
        )

    async with session_factory() as session:
        service = _build_service(session=session)
        with pytest.raises(IdempotencyConflictError) as error_info:
            await service.create_payment(
                idempotency_key=TEST_IDEMPOTENCY_KEY,
                request=_build_request(amount=Decimal("999.00")),
            )

    async with session_factory() as session:
        payments_count = await session.scalar(select(func.count()).select_from(Payment))
        outbox_count = await session.scalar(select(func.count()).select_from(OutboxEvent))

    assert error_info.value.idempotency_key == TEST_IDEMPOTENCY_KEY
    assert payments_count == 1
    assert outbox_count == 1


async def test_create_payment_race_condition_returns_concurrent_payment(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """При гонке параллельных запросов IntegrityError перехватывается и возвращается чужой платеж.

    Конкурентный запрос симулируется предварительной записью платежа с тем же ключом.
    Первый поиск по ключу подменяется, чтобы сервис не увидел уже сохраненный платеж.
    """
    async with session_factory() as session:
        concurrent_service = _build_service(session=session)
        concurrent_payment = await concurrent_service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(),
        )

    async with session_factory() as session:
        uow = UnitOfWork(session=session)
        service = PaymentService(uow=uow)
        original_lookup = uow.payment.get_by_idempotency_key
        lookup_calls: dict[str, int] = {"count": 0}

        async def _lookup_missing_on_first_call(*, idempotency_key: str) -> Payment | None:
            lookup_calls["count"] += 1
            if lookup_calls["count"] == 1:
                return None
            return await original_lookup(idempotency_key=idempotency_key)

        # Патчим метод на экземпляре репозитория конкретного Unit of Work:
        # так имитируется гонка, при которой первый поиск по ключу ничего не находит.
        monkeypatch.setattr(uow.payment, "get_by_idempotency_key", _lookup_missing_on_first_call)

        result = await service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(),
        )

    async with session_factory() as session:
        payments_count = await session.scalar(select(func.count()).select_from(Payment))

    assert lookup_calls["count"] == 2
    assert result.payment_id == concurrent_payment.payment_id
    assert payments_count == 1


async def test_create_payment_race_condition_with_different_body_raises_conflict(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Если конкурентный платеж создан с другими параметрами, при гонке поднимается IdempotencyConflictError."""
    async with session_factory() as session:
        concurrent_service = _build_service(session=session)
        await concurrent_service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(amount=Decimal("1.00")),
        )

    async with session_factory() as session:
        uow = UnitOfWork(session=session)
        service = PaymentService(uow=uow)
        original_lookup = uow.payment.get_by_idempotency_key
        lookup_calls: dict[str, int] = {"count": 0}

        async def _lookup_missing_on_first_call(*, idempotency_key: str) -> Payment | None:
            lookup_calls["count"] += 1
            if lookup_calls["count"] == 1:
                return None
            return await original_lookup(idempotency_key=idempotency_key)

        # Аналогично предыдущему тесту: первый поиск скрывает уже сохраненный платеж.
        monkeypatch.setattr(uow.payment, "get_by_idempotency_key", _lookup_missing_on_first_call)

        with pytest.raises(IdempotencyConflictError):
            await service.create_payment(
                idempotency_key=TEST_IDEMPOTENCY_KEY,
                request=_build_request(amount=Decimal("2.00")),
            )


async def test_get_payment_by_id_returns_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Получение существующего платежа по идентификатору возвращает его данные."""
    async with session_factory() as session:
        service = _build_service(session=session)
        created_payment = await service.create_payment(
            idempotency_key=TEST_IDEMPOTENCY_KEY,
            request=_build_request(),
        )

    async with session_factory() as session:
        service = _build_service(session=session)
        found_payment = await service.get_payment_by_id(payment_id=created_payment.payment_id)

    assert found_payment.payment_id == created_payment.payment_id
    assert found_payment.idempotency_key == TEST_IDEMPOTENCY_KEY


async def test_get_payment_by_id_raises_not_found(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Запрос несуществующего платежа поднимает PaymentNotFoundError с идентификатором в контексте."""
    missing_payment_id = uuid4()

    async with session_factory() as session:
        service = _build_service(session=session)
        with pytest.raises(PaymentNotFoundError) as error_info:
            await service.get_payment_by_id(payment_id=missing_payment_id)

    assert error_info.value.payment_id == missing_payment_id
