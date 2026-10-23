"""Тесты топологии RabbitMQ: quorum-очереди, DLX и политика хранения DLQ."""

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
)
from worker.broker import declare_topology, payments_dlq, payments_dlx, payments_new_queue


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
