"""Общие фикстуры тестов: in-memory API, тестовая БД и ASGI-клиент."""

import asyncio
import os
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Final
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from api.app import app
from api.deps import get_uow, get_webhook_client
from core.config import Settings, get_settings
from core.constants import EVENT_TYPE_PAYMENT_CREATED, OutboxStatus, PaymentStatus, WebhookStatus
from core.engine import Base
from core.exceptions import WebhookDeliveryError
from models import OutboxEvent, Payment
from schemas import CreatePaymentRequest, PaymentEventPayload, WebhookPayload

TEST_API_KEY: Final[str] = "test-api-key"
BASE_URL: Final[str] = "http://testserver"
TEST_DATABASE_NAME: Final[str] = "payments_test"
ADMIN_DATABASE_NAME: Final[str] = "postgres"

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

    async def get_by_id_for_update(self, *, payment_id: UUID) -> Payment | None:
        """Эмулирует SELECT ... FOR UPDATE в памяти.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            ORM-модель платежа либо None, если платеж не найден.
        """
        return await self.get_by_id(payment_id=payment_id)

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
            webhook_status=WebhookStatus.PENDING.value,
            webhook_attempts=0,
            webhook_last_error=None,
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

    async def update_webhook_result(
        self,
        *,
        payment: Payment,
        status: WebhookStatus,
        attempts: int,
        last_error: str | None,
    ) -> None:
        """Записывает результат доставки webhook в ORM-модель.

        Args:
            payment: Платеж, который нужно обновить.
            status: Статус доставки webhook.
            attempts: Количество HTTP-попыток текущей доставки.
            last_error: Техническое описание ошибки или None при успехе.
        """
        payment.webhook_status = status.value
        payment.webhook_attempts = (payment.webhook_attempts or 0) + attempts
        payment.webhook_last_error = last_error

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


class InMemoryWebhookSender:
    """Отправитель webhook в памяти для API-тестов.

    Attributes:
        calls: Пары URL и payload, переданные в send.
        error: Ошибка, которую нужно поднять при следующей отправке.
        attempts: Количество попыток, которое вернет успешная отправка.
    """

    def __init__(self) -> None:
        """Инициализирует пустой журнал вызовов."""
        self.calls: list[tuple[str, WebhookPayload]] = []
        self.error: WebhookDeliveryError | None = None
        self.attempts = 1

    async def send(self, *, url: str, payload: WebhookPayload) -> int:
        """Запоминает вызов и имитирует успех или ошибку доставки.

        Args:
            url: URL клиента.
            payload: Тело уведомления.

        Returns:
            Количество попыток успешной доставки.

        Raises:
            WebhookDeliveryError: Если ошибка задана в атрибуте ``error``.
        """
        self.calls.append((url, payload))
        if self.error is not None:
            raise self.error
        return self.attempts


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
def webhook_sender() -> InMemoryWebhookSender:
    """Создает пустой отправитель webhook в памяти.

    Returns:
        Новый экземпляр InMemoryWebhookSender.
    """
    return InMemoryWebhookSender()


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
            webhook_status=WebhookStatus.PENDING.value,
            webhook_attempts=0,
            webhook_last_error=None,
        )

    return _factory


@pytest.fixture
async def api_client(
    uow: InMemoryUnitOfWork,
    test_settings: Settings,
    webhook_sender: InMemoryWebhookSender,
) -> AsyncIterator[AsyncClient]:
    """Выдает HTTP-клиент поверх ASGI-приложения с подмененными зависимостями.

    Args:
        uow: Unit of Work в памяти.
        test_settings: Настройки с тестовым API-ключом.
        webhook_sender: Отправитель webhook в памяти.

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

    def _override_get_webhook_client() -> InMemoryWebhookSender:
        """Возвращает отправитель webhook в памяти вместо реального httpx-клиента.

        Returns:
            Тестовый отправитель webhook.
        """
        return webhook_sender

    app.dependency_overrides[get_uow] = _override_get_uow
    app.dependency_overrides[get_settings] = _override_get_settings
    app.dependency_overrides[get_webhook_client] = _override_get_webhook_client

    # raise_app_exceptions=False: необработанные исключения превращаются в 500-ответ, а не в трейсбек теста.
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url=BASE_URL) as client:
        yield client

    # app - модульный синглтон, поэтому подмены обязательно снимаем, чтобы они не протекали в другие тесты.
    app.dependency_overrides.clear()


def _resolve_test_database_url() -> URL:
    """Возвращает URL отдельной тестовой БД.

    По умолчанию берется БД из ``DATABASE_URL`` и заменяется на ``payments_test``.
    Полный URL можно переопределить через ``TEST_DATABASE_URL``.

    Returns:
        URL тестовой БД.
    """
    override = os.getenv("TEST_DATABASE_URL")
    if override:
        return make_url(override)

    base_url = make_url(str(get_settings().database_url))
    return base_url.set(database=TEST_DATABASE_NAME)


async def _ensure_database(*, url: URL) -> None:
    """Создает тестовую БД, если ее еще нет.

    Args:
        url: URL тестовой БД.

    Raises:
        sqlalchemy.exc.DBAPIError: Если PostgreSQL недоступен или нет прав на создание БД.
    """
    admin_url = url.set(database=ADMIN_DATABASE_NAME)
    admin_engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with admin_engine.connect() as connection:
            exists = await connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": url.database},
            )
            if exists:
                return

            database_name = (url.database or TEST_DATABASE_NAME).replace('"', '""')
            await connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    finally:
        await admin_engine.dispose()


async def _prepare_test_schema(*, url: URL) -> None:
    """Пересоздает схему в тестовой БД.

    Args:
        url: URL тестовой БД.
    """
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
    finally:
        await engine.dispose()


async def _truncate_tables(*, engine: AsyncEngine) -> None:
    """Очищает таблицы приложения и сбрасывает sequences.

    Args:
        engine: Движок тестовой БД.
    """
    tables = Base.metadata.sorted_tables
    if not tables:
        return

    table_names = ", ".join(f'"{table.name}"' for table in tables)
    async with engine.begin() as connection:
        await connection.execute(text(f"TRUNCATE TABLE {table_names} RESTART IDENTITY CASCADE"))


@pytest.fixture(scope="session")
def test_database_url() -> URL:
    """Готовит отдельную тестовую БД и возвращает ее URL.

    Если PostgreSQL недоступен, тесты, которым нужна БД, пропускаются.

    Returns:
        URL тестовой БД.
    """
    url = _resolve_test_database_url()
    try:
        asyncio.run(_ensure_database(url=url))
        asyncio.run(_prepare_test_schema(url=url))
    except OSError as exc:
        pytest.skip(f"PostgreSQL is not available: {exc}")
    except OperationalError as exc:
        pytest.skip(f"PostgreSQL is not available: {exc}")

    return url


@pytest.fixture
async def db_engine(test_database_url: URL) -> AsyncIterator[AsyncEngine]:
    """Создает движок для тестовой БД.

    Args:
        test_database_url: URL тестовой БД.

    Yields:
        Асинхронный движок SQLAlchemy.
    """
    engine = create_async_engine(test_database_url, pool_pre_ping=True)
    yield engine
    await engine.dispose()


@pytest.fixture
def db_session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Возвращает фабрику сессий для тестовой БД.

    Args:
        db_engine: Движок тестовой БД.

    Returns:
        Фабрика асинхронных сессий.
    """
    return async_sessionmaker(bind=db_engine, expire_on_commit=False)


@pytest.fixture
async def session_factory(
    db_engine: AsyncEngine,
    db_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Отдает фабрику сессий и очищает таблицы до/после теста.

    Args:
        db_engine: Движок тестовой БД.
        db_session_factory: Фабрика сессий для тестовой БД.

    Yields:
        Фабрика сессий с чистыми таблицами.
    """
    await _truncate_tables(engine=db_engine)
    yield db_session_factory
    await _truncate_tables(engine=db_engine)


@pytest.fixture
async def session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    """Отдает сессию тестовой БД с очисткой таблиц до/после теста.

    Args:
        session_factory: Фабрика сессий с изоляцией таблиц.

    Yields:
        Асинхронная сессия SQLAlchemy.
    """
    async with session_factory() as db_session:
        yield db_session
