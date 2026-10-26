"""Тесты воркера: топология RabbitMQ и consumer очереди payments.new."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Self
from uuid import UUID, uuid4

import httpx
import pytest
from faststream.exceptions import NackMessage
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

from core.constants import (
    DLQ_ARGS,
    PAYMENTS_DLQ_DELIVERY_LIMIT,
    PAYMENTS_DLQ_QUEUE,
    PAYMENTS_DLQ_ROUTING_KEY,
    PAYMENTS_DLX_EXCHANGE,
    PAYMENTS_NEW_DELIVERY_LIMIT,
    PAYMENTS_NEW_QUEUE,
    QUORUM_DELIVERY_ARGS,
    QUORUM_QUEUE_TYPE,
    Currency,
    PaymentStatus,
)
from core.exceptions import WebhookDeliveryError
from core.utils import RETRY_MAX_ATTEMPTS
from models import Payment
from schemas import PaymentEventPayload, WebhookPayload
from services.webhook import WebhookClient
from worker.broker import declare_topology, payments_dlq, payments_dlx, payments_new_queue
from worker.consumer import PaymentConsumer, PaymentProcessor, WebhookSender


def test_payments_new_queue_is_quorum_with_dead_lettering() -> None:
    args = payments_new_queue.arguments

    assert args == {**QUORUM_DELIVERY_ARGS, "x-queue-type": QUORUM_QUEUE_TYPE}
    assert args["x-dead-letter-routing-key"] == PAYMENTS_DLQ_ROUTING_KEY
    assert args["x-dead-letter-exchange"] == PAYMENTS_DLX_EXCHANGE

    assert args["x-delivery-limit"] == PAYMENTS_NEW_DELIVERY_LIMIT
    assert isinstance(args["x-delivery-limit"], int)
    assert args["x-delivery-limit"] > 0


def test_payments_dlq_keeps_events_without_ttl_and_own_dlx() -> None:
    """DLQ не теряет события по TTL и не имеет собственного DLX (иначе сообщение отбросится)."""
    args = payments_dlq.arguments

    assert payments_dlq.name == PAYMENTS_DLQ_QUEUE
    assert payments_dlq.durable is True
    assert payments_dlq.routing_key == PAYMENTS_DLQ_ROUTING_KEY
    assert args == {**DLQ_ARGS, "x-queue-type": QUORUM_QUEUE_TYPE}

    assert "x-message-ttl" not in args
    assert "x-expires" not in args

    assert "x-dead-letter-exchange" not in args
    assert "x-dead-letter-routing-key" not in args

    assert args["x-delivery-limit"] == PAYMENTS_DLQ_DELIVERY_LIMIT == -1


def test_payments_dlx_is_durable_direct_exchange() -> None:
    """DLX — durable direct-обменник, привязанный к routing key DLQ."""
    assert payments_dlx.name == PAYMENTS_DLX_EXCHANGE
    assert payments_dlx.type == ExchangeType.DIRECT
    assert payments_dlx.durable is True


class FakeDeclaredExchange:
    """Объявленный обменник — аналог aio_pika.AbstractExchange."""

    def __init__(self, *, name: str) -> None:
        """Сохраняет имя обменника.

        Args:
            name: Имя обменника.
        """
        self.name = name


class FakeDeclaredQueue:
    """Объявленная очередь — аналог aio_pika.RobustQueue с записью привязок."""

    def __init__(self, *, name: str) -> None:
        """Инициализирует очередь с пустым списком привязок.

        Args:
            name: Имя очереди.
        """
        self.name = name
        self.bindings: list[tuple[FakeDeclaredExchange, str]] = []

    async def bind(self, *, exchange: FakeDeclaredExchange, routing_key: str) -> None:
        """Запоминает привязку очереди к обменнику.

        Args:
            exchange: Обменник, к которому привязана очередь.
            routing_key: Ключ маршрутизации привязки.

        Returns:
            None
        """
        self.bindings.append((exchange, routing_key))


class RecordingBroker:
    """Заглушка RabbitBroker с фактическими сигнатурами faststream.

    ``declare_exchange(exchange)`` и ``declare_queue(queue)`` принимают только схему
    и возвращают объявленные объекты. Привязка очереди к обменнику выполняется отдельно,
    поэтому регрессия в способе биндинга DLQ ломает тест, а не продовый старт воркера.
    """

    def __init__(self) -> None:
        """Инициализирует пустые журналы вызовов и объявленных объектов."""
        self.exchange_calls: list[RabbitExchange] = []
        self.queue_calls: list[RabbitQueue] = []
        self.declared_exchanges: dict[str, FakeDeclaredExchange] = {}
        self.declared_queues: dict[str, FakeDeclaredQueue] = {}

    async def declare_exchange(self, exchange: RabbitExchange) -> FakeDeclaredExchange:
        """Запоминает объявление обменника и возвращает его аналог.

        Args:
            exchange: Объявляемый обменник.

        Returns:
            Объявленный обменник.
        """
        self.exchange_calls.append(exchange)
        declared_exchange = FakeDeclaredExchange(name=exchange.name)
        self.declared_exchanges[exchange.name] = declared_exchange
        return declared_exchange

    async def declare_queue(self, queue: RabbitQueue) -> FakeDeclaredQueue:
        """Запоминает объявление очереди и возвращает её аналог.

        Args:
            queue: Объявляемая очередь.

        Returns:
            Объявленная очередь.
        """
        self.queue_calls.append(queue)
        declared_queue = FakeDeclaredQueue(name=queue.name)
        self.declared_queues[queue.name] = declared_queue
        return declared_queue


async def test_declare_topology_binds_dlq_with_explicit_routing_key() -> None:
    """DLQ привязывается к DLX явным routing key, а основная очередь объявляется без привязки."""
    broker = RecordingBroker()

    await declare_topology(broker_instance=broker)  # type: ignore[arg-type]

    assert broker.exchange_calls == [payments_dlx]
    assert broker.queue_calls == [payments_dlq, payments_new_queue]

    declared_dlx = broker.declared_exchanges[PAYMENTS_DLX_EXCHANGE]
    assert broker.declared_queues[PAYMENTS_DLQ_QUEUE].bindings == [(declared_dlx, PAYMENTS_DLQ_ROUTING_KEY)]
    assert broker.declared_queues[PAYMENTS_NEW_QUEUE].bindings == []


class _SleepRecorder:
    """Заглушка asyncio.sleep: запоминает паузы вместо реального ожидания."""

    def __init__(self) -> None:
        """Инициализирует пустой список пауз."""
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        """Запоминает длительность паузы.

        Args:
            delay: Запрошенная длительность паузы в секундах.
        """
        self.delays.append(delay)


class FakeGateway:
    """Заглушка эмулятора платежного шлюза."""

    def __init__(self, *, status: PaymentStatus) -> None:
        """Инициализирует шлюз с фиксированным результатом.

        Args:
            status: Статус, который вернет ``process_payment``.
        """
        self._status = status
        self.calls = 0

    async def process_payment(self) -> PaymentStatus:
        """Возвращает заданный статус и считает вызовы.

        Returns:
            Заданный статус обработки.
        """
        self.calls += 1
        return self._status


class FakeWebhookSender:
    """Заглушка отправки webhook-уведомлений."""

    def __init__(self, *, error: WebhookDeliveryError | None = None) -> None:
        """Инициализирует отправитель.

        Args:
            error: Ошибка, которую нужно поднять при отправке, или None.
        """
        self._error = error
        self.calls: list[tuple[str, WebhookPayload]] = []

    async def send(self, *, url: str, payload: WebhookPayload) -> None:
        """Запоминает вызов и при необходимости поднимает ошибку.

        Args:
            url: URL клиента.
            payload: Тело уведомления.

        Raises:
            WebhookDeliveryError: Если ошибка задана в конструкторе.
        """
        self.calls.append((url, payload))
        if self._error is not None:
            raise self._error


class FakePaymentRepository:
    """Репозиторий платежей в памяти для тестов consumer."""

    def __init__(self, *, payment: Payment | None) -> None:
        """Сохраняет платеж, который будет возвращать ``get_by_id``.

        Args:
            payment: Платеж или None, чтобы имитировать отсутствие записи.
        """
        self._payment = payment

    async def get_by_id(self, *, payment_id: UUID) -> Payment | None:
        """Возвращает платеж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Платеж или None, если он не найден.
        """
        if self._payment is not None and self._payment.id == payment_id:
            return self._payment
        return None

    async def get_by_id_for_update(self, *, payment_id: UUID) -> Payment | None:
        """Эмулирует SELECT ... FOR UPDATE.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Платеж или None, если он не найден.
        """
        return await self.get_by_id(payment_id=payment_id)

    async def update_processing_result(
        self,
        *,
        payment: Payment,
        status: PaymentStatus,
        processed_at: datetime,
    ) -> None:
        """Записывает результат обработки в ORM-модель.

        Args:
            payment: Платеж, который нужно обновить.
            status: Итоговый статус.
            processed_at: Время завершения обработки.
        """
        payment.status = status.value
        payment.processed_at = processed_at


class FakeUnitOfWork:
    """Unit of Work в памяти для тестов consumer."""

    def __init__(self, *, payment: Payment | None) -> None:
        """Инициализирует репозиторий и счетчики транзакций.

        Args:
            payment: Платеж, доступный репозиторию.
        """
        self.payment = FakePaymentRepository(payment=payment)
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> Self:
        """Входит в контекст Unit of Work.

        Returns:
            Текущий экземпляр.
        """
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Выходит из контекста, считая откат при исключении.

        Args:
            *exc_info: Информация об исключении.
        """
        if exc_info[0] is not None:
            self.rollbacks += 1

    async def commit(self) -> None:
        """Фиксирует транзакцию (счетчик для проверок)."""
        self.commits += 1

    async def rollback(self) -> None:
        """Откатывает транзакцию (счетчик для проверок)."""
        self.rollbacks += 1


def _build_payment(
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    processed_at: datetime | None = None,
) -> Payment:
    """Создает transient-модель платежа для тестов consumer.

    Args:
        status: Статус платежа.
        processed_at: Время завершения обработки.

    Returns:
        Несохраненная ORM-модель платежа.
    """
    return Payment(
        id=uuid4(),
        amount=Decimal("100.50"),
        currency=Currency.RUB.value,
        description="Оплата заказа",
        metadata_={"order_id": "42"},
        status=status.value,
        idempotency_key=f"key-{uuid4()}",
        webhook_url="https://example.com/webhook",
        created_at=datetime.now(tz=UTC),
        processed_at=processed_at,
    )


def _build_consumer(
    *,
    payment: Payment | None,
    gateway: PaymentProcessor,
    webhook_client: WebhookSender,
) -> tuple[PaymentConsumer, FakeUnitOfWork]:
    """Собирает consumer с фейковым Unit of Work.

    Args:
        payment: Платеж, доступный репозиторию, или None.
        gateway: Заглушка шлюза.
        webhook_client: Заглушка или реальный клиент webhook.

    Returns:
        Consumer и его фейковый Unit of Work.
    """
    uow = FakeUnitOfWork(payment=payment)
    consumer = PaymentConsumer(
        gateway=gateway,
        webhook_client=webhook_client,
        uow_factory=lambda: uow,  # type: ignore[arg-type, return-value]
    )
    return consumer, uow


async def test_consumer_processes_pending_payment_and_sends_webhook() -> None:
    """Полный успешный цикл: шлюз, commit статуса и webhook с итоговыми данными."""
    payment = _build_payment()
    gateway = FakeGateway(status=PaymentStatus.SUCCEEDED)
    webhook = FakeWebhookSender()
    consumer, uow = _build_consumer(payment=payment, gateway=gateway, webhook_client=webhook)

    await consumer.handle(payload=PaymentEventPayload(payment_id=payment.id))

    assert gateway.calls == 1
    assert uow.commits == 1
    assert payment.status == PaymentStatus.SUCCEEDED.value
    assert payment.processed_at is not None
    assert len(webhook.calls) == 1

    url, payload = webhook.calls[0]
    assert url == payment.webhook_url
    assert payload.payment_id == payment.id
    assert payload.status == PaymentStatus.SUCCEEDED
    assert payload.amount == Decimal("100.50")
    assert payload.currency == Currency.RUB
    assert payload.metadata == {"order_id": "42"}
    assert payload.processed_at == payment.processed_at


async def test_consumer_skips_gateway_for_already_processed_payment() -> None:
    """Финальный статус не запускает шлюз повторно, webhook уходит с сохраненным результатом."""
    processed_at = datetime.now(tz=UTC)
    payment = _build_payment(status=PaymentStatus.SUCCEEDED, processed_at=processed_at)
    gateway = FakeGateway(status=PaymentStatus.FAILED)
    webhook = FakeWebhookSender()
    consumer, uow = _build_consumer(payment=payment, gateway=gateway, webhook_client=webhook)

    await consumer.handle(payload=PaymentEventPayload(payment_id=payment.id))

    assert gateway.calls == 0
    assert uow.commits == 0
    assert len(webhook.calls) == 1
    assert webhook.calls[0][1].status == PaymentStatus.SUCCEEDED
    assert webhook.calls[0][1].processed_at == processed_at


async def test_consumer_nacks_to_dlq_when_webhook_delivery_fails() -> None:
    """После трех неудачных попыток webhook consumer отправляет сообщение в DLQ."""
    payment = _build_payment()
    gateway = FakeGateway(status=PaymentStatus.SUCCEEDED)
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        """Имитирует недоступный webhook-сервер.

        Args:
            request: Входящий HTTP-запрос.

        Returns:
            Ответ 500.
        """
        nonlocal attempts
        attempts += 1
        return httpx.Response(status_code=httpx.codes.INTERNAL_SERVER_ERROR, request=request)

    sleep = _SleepRecorder()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        webhook = WebhookClient(client=client, sleep=sleep)
        consumer, uow = _build_consumer(payment=payment, gateway=gateway, webhook_client=webhook)
        with pytest.raises(NackMessage) as exc_info:
            await consumer.handle(payload=PaymentEventPayload(payment_id=payment.id))

    assert exc_info.value.extra_options == {"requeue": False}
    assert attempts == RETRY_MAX_ATTEMPTS
    assert len(sleep.delays) == RETRY_MAX_ATTEMPTS - 1
    # Статус закоммичен до webhook: повторная доставка не запустит шлюз заново.
    assert uow.commits == 1
    assert payment.status == PaymentStatus.SUCCEEDED.value


async def test_consumer_nacks_to_dlq_when_payment_not_found() -> None:
    """Событие с несуществующим платежом уходит в DLQ без вызова шлюза и webhook."""
    gateway = FakeGateway(status=PaymentStatus.SUCCEEDED)
    webhook = FakeWebhookSender()
    consumer, _ = _build_consumer(payment=None, gateway=gateway, webhook_client=webhook)

    with pytest.raises(NackMessage) as exc_info:
        await consumer.handle(payload=PaymentEventPayload(payment_id=uuid4()))

    assert exc_info.value.extra_options == {"requeue": False}
    assert gateway.calls == 0
    assert webhook.calls == []
