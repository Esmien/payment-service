"""Тесты клиента доставки webhook-уведомлений (services.webhook)."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest

from core.constants import Currency, PaymentStatus
from core.exceptions import WebhookDeliveryError
from core.utils import (
    EXPONENTIAL_BACKOFF_MAX_SECONDS,
    EXPONENTIAL_BACKOFF_MIN_SECONDS,
    RETRY_MAX_ATTEMPTS,
)
from schemas import WebhookPayload
from services.webhook import WebhookClient

WEBHOOK_URL = "https://hooks.example.com/payments"
PAYMENT_ID = UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6")
PROCESSED_AT = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
RETRY_AFTER_HEADER = "Retry-After"


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


class _MockWebhookServer:
    """HTTP-заглушка: отдает заданные статусы и записывает запросы."""

    def __init__(self, *, statuses: list[int], retry_after: str | None = None) -> None:
        """Инициализирует сервер с очередью статусов.

        Args:
            statuses: Статусы ответов по порядку; после исчерпания отдается 200.
            retry_after: Значение заголовка Retry-After для ответа 429.
        """
        self._statuses = list(statuses)
        self._retry_after = retry_after
        self.requests: list[httpx.Request] = []

    def _handler(self, request: httpx.Request) -> httpx.Response:
        """Отдает очередной статус и сохраняет запрос.

        Args:
            request: Входящий HTTP-запрос.

        Returns:
            Подготовленный HTTP-ответ.
        """
        self.requests.append(request)
        status_code = self._statuses.pop(0) if self._statuses else httpx.codes.OK
        headers: dict[str, str] = {}
        if status_code == httpx.codes.TOO_MANY_REQUESTS and self._retry_after is not None:
            headers[RETRY_AFTER_HEADER] = self._retry_after
        return httpx.Response(status_code=status_code, headers=headers, request=request)

    def client(self) -> httpx.AsyncClient:
        """Создает httpx-клиент поверх MockTransport.

        Returns:
            Асинхронный клиент, привязанный к этому серверу.
        """
        return httpx.AsyncClient(transport=httpx.MockTransport(self._handler))


def _build_payload() -> WebhookPayload:
    """Собирает тестовое тело webhook-уведомления.

    Returns:
        Валидный payload с предсказуемыми значениями.
    """
    return WebhookPayload(
        payment_id=PAYMENT_ID,
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        metadata={"order_id": "42"},
        processed_at=PROCESSED_AT,
    )


async def test_send_delivers_payload_on_first_attempt() -> None:
    """Успешный первый ответ 200 отправляет ровно один POST с JSON-payload."""
    server = _MockWebhookServer(statuses=[httpx.codes.OK])
    sleep = _SleepRecorder()
    payload = _build_payload()

    async with server.client() as client:
        attempts = await WebhookClient(client=client, sleep=sleep).send(url=WEBHOOK_URL, payload=payload)

    assert attempts == 1
    assert len(server.requests) == 1
    assert sleep.delays == []

    request = server.requests[0]
    assert request.method == "POST"
    assert str(request.url) == WEBHOOK_URL
    assert json.loads(request.content) == payload.model_dump(mode="json")


async def test_send_retries_transient_errors_until_success() -> None:
    """500 и 503 повторяются, успешный третий ответ завершает доставку."""
    server = _MockWebhookServer(
        statuses=[
            httpx.codes.INTERNAL_SERVER_ERROR,
            httpx.codes.SERVICE_UNAVAILABLE,
            httpx.codes.OK,
        ],
    )
    sleep = _SleepRecorder()

    async with server.client() as client:
        attempts = await WebhookClient(client=client, sleep=sleep).send(url=WEBHOOK_URL, payload=_build_payload())

    assert attempts == RETRY_MAX_ATTEMPTS
    assert len(server.requests) == RETRY_MAX_ATTEMPTS
    assert len(sleep.delays) == RETRY_MAX_ATTEMPTS - 1
    for delay in sleep.delays:
        assert EXPONENTIAL_BACKOFF_MIN_SECONDS <= delay <= EXPONENTIAL_BACKOFF_MAX_SECONDS


async def test_send_honors_retry_after_on_429() -> None:
    """При 429 пауза берется из Retry-After, а не из экспоненциального backoff."""
    server = _MockWebhookServer(
        statuses=[httpx.codes.TOO_MANY_REQUESTS, httpx.codes.OK],
        retry_after="7",
    )
    sleep = _SleepRecorder()

    async with server.client() as client:
        await WebhookClient(client=client, sleep=sleep).send(url=WEBHOOK_URL, payload=_build_payload())

    assert len(server.requests) == 2
    assert sleep.delays == [7.0]


async def test_send_treats_redirect_as_failure() -> None:
    """3xx не считается успехом: доставка повторяется."""
    server = _MockWebhookServer(statuses=[httpx.codes.FOUND, httpx.codes.OK])
    sleep = _SleepRecorder()

    async with server.client() as client:
        await WebhookClient(client=client, sleep=sleep).send(url=WEBHOOK_URL, payload=_build_payload())

    assert len(server.requests) == 2
    assert len(sleep.delays) == 1


async def test_send_raises_after_max_attempts() -> None:
    """После трех неудач выбрасывается WebhookDeliveryError с контекстом."""
    server = _MockWebhookServer(
        statuses=[httpx.codes.INTERNAL_SERVER_ERROR] * RETRY_MAX_ATTEMPTS,
    )
    sleep = _SleepRecorder()

    async with server.client() as client:
        with pytest.raises(WebhookDeliveryError) as exc_info:
            await WebhookClient(client=client, sleep=sleep).send(url=WEBHOOK_URL, payload=_build_payload())

    assert len(server.requests) == RETRY_MAX_ATTEMPTS
    assert len(sleep.delays) == RETRY_MAX_ATTEMPTS - 1
    assert exc_info.value.url == WEBHOOK_URL
    assert exc_info.value.attempts == RETRY_MAX_ATTEMPTS


async def test_send_retries_connection_errors_and_wraps_them() -> None:
    """Сетевые ошибки повторяются и после исчерпания попыток оборачиваются в WebhookDeliveryError."""
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        """Имитирует недоступность webhook-сервера.

        Args:
            request: Входящий HTTP-запрос.

        Raises:
            httpx.ConnectError: Всегда, чтобы проверить retry и оборачивание.
        """
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("connection refused", request=request)

    sleep = _SleepRecorder()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(WebhookDeliveryError):
            await WebhookClient(client=client, sleep=sleep).send(url=WEBHOOK_URL, payload=_build_payload())

    assert attempts == RETRY_MAX_ATTEMPTS
    assert len(sleep.delays) == RETRY_MAX_ATTEMPTS - 1
