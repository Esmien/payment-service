"""Топология RabbitMQ: Dead Letter Exchange/Queue и основная очередь платежей."""

from typing import Final

from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from core.config import get_settings
from core.constants import (
    DLQ_ARGS,
    PAYMENTS_DLQ_QUEUE,
    PAYMENTS_DLQ_ROUTING_KEY,
    PAYMENTS_DLX_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    QUORUM_DELIVERY_ARGS,
)

# DLX, куда RabbitMQ перенаправляет сообщения после Nack(requeue=False) из основной очереди.
payments_dlx: Final[RabbitExchange] = RabbitExchange(
    name=PAYMENTS_DLX_EXCHANGE,
    type=ExchangeType.DIRECT,
    durable=True,
)

# DLQ хранит сообщения, которые не удалось обработать после всех попыток.
# TTL намеренно не задан: события об отказах платежей нельзя терять по времени.
# Длину очереди отслеживаем метриками RabbitMQ (rabbitmq_prometheus) и алертом,
# а не автоочисткой. Своего DLX у DLQ нет: повторный Nack(requeue=False) отбросит сообщение.
payments_dlq: Final[RabbitQueue] = RabbitQueue(
    name=PAYMENTS_DLQ_QUEUE,
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=PAYMENTS_DLQ_ROUTING_KEY,
    arguments=DLQ_ARGS,
)

# Основная очередь: quorum переживает потерю узла, а x-delivery-limit страхует от
# бесконечных redelivery, если consumer падает до ручного Nack. После лимита — DLX.
# ВАЖНО: аргументы очереди неизменяемы после создания. Правка x-queue-type или
# x-dead-letter-* на существующей очереди вернёт PRECONDITION_FAILED (406) —
# очередь придётся пересоздавать (purge + delete) либо выпускать под новым именем.
payments_new_queue: Final[RabbitQueue] = RabbitQueue(
    name=PAYMENTS_NEW_QUEUE,
    durable=True,
    routing_key=PAYMENTS_NEW_QUEUE,
    queue_type=QueueType.QUORUM,
    arguments=QUORUM_DELIVERY_ARGS,
)


def create_broker() -> RabbitBroker:
    """Создаёт RabbitBroker по настройкам, актуальным на момент вызова.

    Настройки читаются не при импорте модуля: тесты успевают подменить окружение,
    а отсутствие обязательных переменных не ломает импорт модуля и сбор тестов.

    Returns:
        Новый экземпляр RabbitBroker без установленного соединения.

    Raises:
        pydantic.ValidationError: Если обязательные настройки отсутствуют или некорректны.
    """
    return RabbitBroker(url=str(get_settings().rabbitmq_url))


async def declare_topology(*, broker_instance: RabbitBroker) -> None:
    """Объявляет DLX, DLQ и основную очередь с привязкой к Dead Letter Exchange.

    Вызывается при старте воркера до запуска Outbox Relay, чтобы DLQ существовала
    и была привязана к DLX к моменту первой возможной ошибки обработки.

    Args:
        broker_instance: Экземпляр RabbitBroker, с которым будет объявлена топология.

    Returns:
        None
    """
    # Порядок важен: сначала обменник и DLQ с привязкой, затем основная очередь,
    # которая ссылается на DLX в аргументах x-dead-letter-exchange.
    declared_dlx = await broker_instance.declare_exchange(exchange=payments_dlx)
    declared_dlq = await broker_instance.declare_queue(queue=payments_dlq)
    # faststream объявляет очередь без привязки: declare_queue(queue) не принимает exchange/routing_key.
    # Поэтому биндинг делаем на aio_pika-объекте (RobustQueue), который возвращает declare_queue.
    # binding key обязан совпадать с x-dead-letter-routing-key, иначе dead-letter сообщения
    # будут отброшены как unroutable.
    await declared_dlq.bind(exchange=declared_dlx, routing_key=PAYMENTS_DLQ_ROUTING_KEY)
    await broker_instance.declare_queue(queue=payments_new_queue)
