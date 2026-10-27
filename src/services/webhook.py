"""Доставка webhook-уведомлений клиенту с retry-политикой."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol

import httpx
from loguru import logger

from core.constants import Currency, PaymentStatus
from core.exceptions import WebhookDeliveryError
from core.utils import external_api_retry
from models import Payment
from schemas import WebhookPayload


class WebhookSender(Protocol):
    """Контракт отправки webhook-уведомления."""

    async def send(self, *, url: str, payload: WebhookPayload) -> int:
        """Отправляет уведомление на URL клиента.

        Args:
            url: URL клиента для получения уведомления.
            payload: Тело уведомления.

        Returns:
            Количество выполненных HTTP-попыток.

        Raises:
            WebhookDeliveryError: Если доставить уведомление не удалось.
        """
        ...


def build_webhook_payload(*, payment: Payment) -> WebhookPayload:
    """Собирает тело webhook-уведомления из финального состояния платежа.

    Args:
        payment: Платеж с финальным статусом и заполненным ``processed_at``.

    Returns:
        Тело уведомления для отправки клиенту.

    Raises:
        RuntimeError: Если у платежа отсутствует ``processed_at`` — нарушен инвариант
            «финальный статус сохраняется вместе с временем обработки».
    """
    processed_at = payment.processed_at
    if processed_at is None:
        # Инвариант: финальный статус всегда сохраняется вместе с processed_at.
        raise RuntimeError(f"Payment {payment.id} has final status without processed_at")

    return WebhookPayload(
        payment_id=payment.id,
        status=PaymentStatus(payment.status),
        amount=payment.amount,
        currency=Currency(payment.currency),
        metadata=payment.metadata_,
        processed_at=processed_at,
    )


class WebhookClient:
    """Клиент отправки webhook-уведомлений через httpx.

    HTTP-клиент передается снаружи: владелец (lifespan воркера) создает его
    с таймаутом из настроек и закрывает при остановке. Так сохраняется пул
    соединений между доставками.

    Attributes:
        _client: Асинхронный HTTP-клиент.
        _sleep: Функция паузы между retry-попытками.
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Инициализирует клиент доставки webhook.

        Args:
            client: Асинхронный HTTP-клиент httpx.
            sleep: Функция паузы между попытками; в тестах подменяется.
        """
        self._client = client
        self._sleep = sleep

    async def send(self, *, url: str, payload: WebhookPayload) -> int:
        """Отправляет POST-запрос с результатом платежа на webhook_url.

        Вызов обернут в retry-политику ``external_api_retry``: до 3 попыток,
        экспоненциальный backoff и приоритет заголовка ``Retry-After`` при 429.
        Повторяются только ошибки ``httpx.HTTPError``; остальные исключения
        пробрасываются сразу.

        Args:
            url: URL клиента для получения уведомления.
            payload: Тело уведомления.

        Returns:
            Количество выполненных HTTP-попыток.

        Raises:
            WebhookDeliveryError: Если после всех попыток доставить уведомление
                не удалось.
        """
        attempts = 0

        @external_api_retry(sleep=self._sleep)
        async def _post() -> None:
            """Выполняет один POST-запрос и проверяет, что ответ 2xx."""
            nonlocal attempts
            attempts += 1
            response = await self._client.post(
                url=url,
                json=payload.model_dump(mode="json"),
            )
            # raise_for_status не считает 3xx ошибкой, а по контракту доставки
            # успешен только 2xx — иначе уведомление не дошло до клиента.
            if not response.is_success:
                raise httpx.HTTPStatusError(
                    message=f"Webhook responded with HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )

        try:
            await _post()
        except httpx.HTTPError as exc:
            logger.error(
                "Webhook delivery failed after {} attempts: url={}, error={}",
                attempts,
                url,
                repr(exc),
            )
            raise WebhookDeliveryError(
                internal_message=f"Webhook delivery failed after {attempts} attempts: url={url}",
                url=url,
                attempts=attempts,
            ) from exc

        return attempts
