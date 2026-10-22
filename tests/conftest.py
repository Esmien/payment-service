"""Общие фикстуры интеграционных тестов API: Unit of Work в памяти и ASGI-клиент.

# DEVIATION: используется in-memory репозиторий вместо реального PostgreSQL,
# чтобы тесты API были детерминированными и не зависели от поднятой БД.
"""

from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Final
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy.exc import IntegrityError

from api.app import app
from api.deps import get_uow
from core.config import Settings, get_settings
from core.constants import EVENT_TYPE_PAYMENT_CREATED, OutboxStatus, PaymentStatus
from models import OutboxEvent, Payment
from schemas import CreatePaymentRequest, PaymentEventPayload

TEST_API_KEY: Final[str] = "test-api-key"
BASE_URL: Final[str] = "http://testserver"

PaymentFactory = Callable[..., Payment]


class InMemoryPaymentRepository:
    """Репозиторий платежей в памяти, повторяющий контракт `PaymentRepository`.

    Attributes:
        payments: Сохраненные платежи по ключу идемпотентности.
        outbox_events: Записанные события outbox.
    """

    def __init__(self) -> None:
        """Инициализирует пустое хранилище."""
        self.payments: dict[str, Payment] = {}
        self.outbox_events: list[OutboxEvent] = []
        self._concurrent_payment: Payment | None = None

    async def get_by_id(self, *, payment_id: UUID) -> Payment | None:
        """Возвращает платеж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            ORM-модель платежа либо None, если платеж не найден.
        """
        return next((payment for payment in self.payments.values() if payment.id == payment_id), None)

    async def get_by_idempotency_key(self, *, idempotency_key: str) -> Payment | None:
        """Возвращает платеж по ключу идемпотентности.

        Args:
            idempotency_key: Значение заголовка `Idempotency-Key`.

        Returns:
            ORM-модель платежа либо None, если платеж не найден.
        """
        return self.payments.get(idempotency_key)

    async def create_payment_with_outbox(self, *, request: CreatePaymentRequest, idempotency_key: str) -> Payment:
        """Создает платеж и событие outbox либо эмулирует гонку по ключу идемпотентности.

        Args:
            request: Валидированное тело запроса на создание платежа.
            idempotency_key: Значение заголовка `Idempotency-Key`.

        Returns:
            Созданная ORM-модель платежа.

        Raises:
            IntegrityError: Если запланирована эмуляция параллельного запроса с тем же ключом.
        """
        concurrent_payment = self._concurrent_payment
        if concurrent_payment is not None and concurrent_payment.idempotency_key == idempotency_key:
            # Конкурирующий запрос успел закоммитить платеж: наш INSERT падает на UNIQUE-индексе.
            # Платеж становится видимым сервису после rollback, как в реальной БД.
            self.payments[idempotency_key] = concurrent_payment
            self._concurrent_payment = None
            raise IntegrityError(
                statement="INSERT INTO payments ...",
                params={},
                orig=Exception('duplicate key value violates unique constraint "ix_payments_idempotency_key"'),
            )

        payment = Payment(
            id=uuid4(),
            amount=request.amount,
            currency=request.currency.value,
            description=request.description,
            metadata_=dict(request.metadata),
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            webhook_url=str(request.webhook_url),
            created_at=datetime.now(tz=UTC),
            processed_at=None,
        )
        self.payments[idempotency_key] = payment
        self.outbox_events.append(
            OutboxEvent(
                id=uuid4(),
                event_type=EVENT_TYPE_PAYMENT_CREATED,
                payload=PaymentEventPayload(payment_id=payment.id).model_dump(mode="json"),
                status=OutboxStatus.PENDING.value,
                created_at=datetime.now(tz=UTC),
            )
        )
        return payment

    def simulate_concurrent_create(self, *, payment: Payment) -> None:
        """Планирует продажу платежа «конкурирующим» запросом при следующем INSERT.

        Args:
            payment: Платеж, который якобы успел закоммитить параллельный запрос.
        """
        self._concurrent_payment = payment


class InMemoryUnitOfWork:
    """Подмена Unit of Work: репозиторий в памяти и счетчики вызовов commit/rollback."""

    def __init__(self, *, payment: InMemoryPaymentRepository) -> None:
        """Инициализирует Unit of Work.

        Args:
            payment: Репозиторий платежей в памяти.
        """
        self.payment = payment
        self.commit_calls = 0
        self.rollback_calls = 0

    async def commit(self) -> None:
        """Фиксирует транзакцию (счетчик для проверок в тестах)."""
        self.commit_calls += 1

    async def rollback(self) -> None:
        """Откатывает транзакцию (счетчик для проверок в тестах)."""
        self.rollback_calls += 1


@pytest.fixture
def payment_repository() -> InMemoryPaymentRepository:
    """Создает пустой репозиторий платежей в памяти.

    Returns:
        Новый экземпляр InMemoryPaymentRepository.
    """
    return InMemoryPaymentRepository()


@pytest.fixture
def uow(payment_repository: InMemoryPaymentRepository) -> InMemoryUnitOfWork:
    """Создает Unit of Work, работающий с репозиторием в памяти.

    Args:
        payment_repository: Репозиторий платежей в памяти.

    Returns:
        Новый экземпляр InMemoryUnitOfWork.
    """
    return InMemoryUnitOfWork(payment=payment_repository)


@pytest.fixture
def test_settings() -> Settings:
    """Возвращает настройки с фиксированным API-ключом.

    Returns:
        Экземпляр Settings с тестовым ключом.
    """
    return Settings(api_key=SecretStr(TEST_API_KEY))


@pytest.fixture
def make_payment() -> PaymentFactory:
    """Возвращает фабрику ORM-моделей платежа для сценариев с гонкой.

    Returns:
        Вызываемый объект, создающий Payment по запросу и ключу идемпотентности.
    """

    def _factory(*, request: CreatePaymentRequest, idempotency_key: str) -> Payment:
        """Создает transient-модель Payment без сохранения в репозиторий.

        Args:
            request: Тело запроса, из которого берутся поля платежа.
            idempotency_key: Ключ идемпотентности платежа.

        Returns:
            Несохраненная ORM-модель платежа.
        """
        return Payment(
            id=uuid4(),
            amount=request.amount,
            currency=request.currency.value,
            description=request.description,
            metadata_=dict(request.metadata),
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            webhook_url=str(request.webhook_url),
            created_at=datetime.now(tz=UTC),
            processed_at=None,
        )

    return _factory


@pytest.fixture
async def api_client(uow: InMemoryUnitOfWork, test_settings: Settings) -> AsyncIterator[AsyncClient]:
    """Выдает HTTP-клиент поверх ASGI-приложения с подмененными зависимостями.

    Args:
        uow: Unit of Work в памяти.
        test_settings: Настройки с тестовым API-ключом.

    Yields:
        Асинхронный HTTP-клиент httpx.
    """

    async def _override_get_uow() -> InMemoryUnitOfWork:
        """Возвращает Unit of Work в памяти вместо реальной сессии БД.

        Returns:
            Экземпляр Unit of Work в памяти.
        """
        return uow

    def _override_get_settings() -> Settings:
        """Возвращает тестовые настройки вместо чтения окружения.

        Returns:
            Экземпляр Settings с тестовым ключом.
        """
        return test_settings

    app.dependency_overrides[get_uow] = _override_get_uow
    app.dependency_overrides[get_settings] = _override_get_settings

    # raise_app_exceptions=False: необработанные исключения превращаются в 500-ответ, а не в трейсбек теста.
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url=BASE_URL) as client:
        yield client

    # app - модульный синглтон, поэтому подмены обязательно снимаем, чтобы они не протекали в другие тесты.
    app.dependency_overrides.clear()
