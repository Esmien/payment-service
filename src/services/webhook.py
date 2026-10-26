"""Доставка webhook-уведомлений клиенту с retry-политикой."""

import asyncio
from collections.abc import Awaitable, Callable

import httpx
from loguru import logger

from core.exceptions import WebhookDeliveryError
from core.utils import RETRY_MAX_ATTEMPTS, external_api_retry
from schemas import WebhookPayload


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

    async def send(self, *, url: str, payload: WebhookPayload) -> None:
        """Отправляет POST-запрос с результатом платежа на webhook_url.

        Вызов обернут в retry-политику ``external_api_retry``: до 3 попыток,
        экспоненциальный backoff и приоритет заголовка ``Retry-After`` при 429.
        Повторяются только ошибки ``httpx.HTTPError``; остальные исключения
        пробрасываются сразу.

        Args:
            url: URL клиента для получения уведомления.
            payload: Тело уведомления.

        Raises:
            WebhookDeliveryError: Если после всех попыток доставить уведомление
                не удалось.
        """

        @external_api_retry(sleep=self._sleep)
        async def _post() -> None:
            """Выполняет один POST-запрос и проверяет, что ответ 2xx."""
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
                RETRY_MAX_ATTEMPTS,
                url,
                repr(exc),
            )
            raise WebhookDeliveryError(
                internal_message=f"Webhook delivery failed after {RETRY_MAX_ATTEMPTS} attempts: url={url}",
                url=url,
                attempts=RETRY_MAX_ATTEMPTS,
            ) from exc
