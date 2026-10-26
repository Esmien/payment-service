"""Репозитории для работы с платежами и outbox-событиями."""

from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.constants import EVENT_TYPE_PAYMENT_CREATED, OutboxStatus, PaymentStatus
from models import OutboxEvent, Payment
from schemas import CreatePaymentRequest, PaymentEventPayload

# Имена объектов PostgreSQL, нарушение которых означает конфликт идемпотентности.
# ix_* - так SQLAlchemy называет UNIQUE-индекс, созданный через unique=True, index=True;
# payments_idempotency_key_key - имя, которое PostgreSQL дал бы отдельному UNIQUE-констрейнту.
IDEMPOTENCY_KEY_UNIQUE_INDEX: Final[str] = "ix_payments_idempotency_key"
IDEMPOTENCY_KEY_UNIQUE_CONSTRAINT: Final[str] = "payments_idempotency_key_key"


def is_idempotency_key_violation(*, error: IntegrityError) -> bool:
    """Определяет, что IntegrityError вызван дублем по `payments.idempotency_key`.

    Args:
        error: Исключение целостности, поднятое драйвером БД.

    Returns:
        True, если нарушен уникальный индекс или констрейнт по ключу идемпотентности.
    """
    original_error = error.orig
    constraint_name = getattr(original_error, "constraint_name", None)
    if constraint_name is not None:
        # asyncpg отдает имя нарушенного объекта отдельным атрибутом.
        return constraint_name in {IDEMPOTENCY_KEY_UNIQUE_INDEX, IDEMPOTENCY_KEY_UNIQUE_CONSTRAINT}
    # Фолбэк для драйверов, не отдающих имя констрейнта отдельным атрибутом.
    return "idempotency_key" in str(original_error)


class PaymentRepository:
    """Репозиторий платежей: доступ к таблицам `payments` и `outbox` в рамках одной сессии."""

    def __init__(self, *, session: AsyncSession) -> None:
        """Инициализирует репозиторий.

        Args:
            session: Асинхронная сессия SQLAlchemy, общая для Unit of Work.
        """
        self._session = session

    async def get_by_id(self, *, payment_id: UUID) -> Payment | None:
        """Возвращает платеж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            ORM-модель платежа либо None, если платеж не найден.
        """
        return await self._session.get(Payment, ident=payment_id)

    async def get_by_idempotency_key(self, *, idempotency_key: str) -> Payment | None:
        """Возвращает платеж по ключу идемпотентности.

        Args:
            idempotency_key: Значение заголовка `Idempotency-Key`.

        Returns:
            ORM-модель платежа либо None, если платеж с таким ключом не найден.
        """
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self._session.scalar(stmt)

    async def create_payment_with_outbox(
        self,
        *,
        request: CreatePaymentRequest,
        idempotency_key: str,
    ) -> Payment:
        """Создает платеж и outbox-событие в текущей (еще не закоммиченной) транзакции.

        Args:
            request: Валидированное тело запроса на создание платежа.
            idempotency_key: Значение заголовка `Idempotency-Key`.

        Returns:
            ORM-модель созданного платежа с заполненными БД-полями (`id`, `created_at`).

        Raises:
            sqlalchemy.exc.IntegrityError: Если нарушено ограничение таблицы,
                в том числе UNIQUE по `payments.idempotency_key`. Исключение
                пробрасывается наверх — гонку обрабатывает сервисный слой.
        """
        # `id` намеренно не передается: значение генерирует default=uuid.uuid4.
        payment = Payment(
            amount=request.amount,
            currency=request.currency.value,
            description=request.description,
            metadata_=request.metadata,
            status=PaymentStatus.PENDING.value,
            idempotency_key=idempotency_key,
            webhook_url=str(request.webhook_url),
        )

        self._session.add(payment)
        # flush выполняет INSERT и наполняет объект значениями server_default (created_at).
        await self._session.flush()

        # Payload собирается только после flush, иначе `payment.id` еще пуст.
        outbox_event = OutboxEvent(
            event_type=EVENT_TYPE_PAYMENT_CREATED,
            payload=PaymentEventPayload(payment_id=payment.id).model_dump(mode="json"),
            status=OutboxStatus.PENDING.value,
        )

        self._session.add(outbox_event)
        # Оба INSERT уходят в БД в одной транзакции UoW.
        await self._session.flush()

        return payment

    async def update_processing_result(
        self,
        *,
        payment: Payment,
        status: PaymentStatus,
        processed_at: datetime,
    ) -> None:
        """Записывает результат обработки платежа в текущую транзакцию.

        Args:
            payment: Загруженная ORM-модель платежа.
            status: Итоговый статус обработки.
            processed_at: Время завершения обработки.

        Returns:
            None
        """
        payment.status = status.value
        payment.processed_at = processed_at
        # flush отправляет UPDATE в БД; commit остается за Unit of Work.
        await self._session.flush()
