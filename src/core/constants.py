"""Константы, перечисления и тексты сообщений домена платежей.

Модуль является единым источником истины для статусов, валют, имен очередей
и обменников RabbitMQ, типов событий и пользовательских сообщений об ошибках.
"""

from enum import StrEnum
from typing import Final


class Currency(StrEnum):
    """Поддерживаемые валюты платежа.

    Attributes:
        RUB: Российский рубль.
        USD: Доллар США.
        EUR: Евро.
    """

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Статусы жизненного цикла платежа.

    Attributes:
        PENDING: Платеж создан и ожидает обработки шлюзом.
        SUCCEEDED: Шлюз подтвердил списание.
        FAILED: Шлюз отклонил платеж.
    """

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class OutboxStatus(StrEnum):
    """Статусы записей транзакционного Outbox.

    Attributes:
        PENDING: Событие записано в БД и ещё не опубликовано в брокер.
        PROCESSED: Событие успешно опубликовано в брокер.
    """

    PENDING = "pending"
    PROCESSED = "processed"


# Тип события, который пишется в таблицу outbox при создании платежа.
EVENT_TYPE_PAYMENT_CREATED: Final[str] = "payment.created"

# Очередь основной обработки платежей и её маршрутизация при отказе.
PAYMENTS_NEW_QUEUE: Final[str] = "payments.new"
PAYMENTS_DLX_EXCHANGE: Final[str] = "payments.dlx"
PAYMENTS_DLQ_QUEUE: Final[str] = "payments.dlq"
PAYMENTS_DLQ_ROUTING_KEY: Final[str] = "payments.dlq"

# Уровень логирования по умолчанию, если он не задан в окружении.
DEFAULT_LOG_LEVEL: Final[str] = "INFO"

# Тексты ответов клиенту (русский язык, обращение на «Вы»).
PAYMENT_NOT_FOUND_MESSAGE: Final[str] = "Платеж не найден. Проверьте корректность идентификатора платежа."
IDEMPOTENCY_CONFLICT_MESSAGE: Final[str] = (
    "Запрос с этим Idempotency-Key уже был отправлен с другими параметрами. Используйте новый ключ."
)
UNAUTHORIZED_MESSAGE: Final[str] = "Неверный или отсутствующий API-ключ. Проверьте заголовок X-API-Key."
INTERNAL_ERROR_MESSAGE: Final[str] = "Внутренняя ошибка сервера. Повторите запрос позже."
