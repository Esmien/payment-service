"""Утилиты ядра: retry-политика для вызовов внешних API на базе tenacity."""

import asyncio
import math
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from typing import Any, Final

import httpx
from loguru import logger
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

RETRY_MAX_ATTEMPTS: Final[int] = 3
EXPONENTIAL_BACKOFF_MULTIPLIER: Final[int] = 1
EXPONENTIAL_BACKOFF_MIN_SECONDS: Final[int] = 2
EXPONENTIAL_BACKOFF_MAX_SECONDS: Final[int] = 10
# Верхняя граница ожидания по Retry-After, чтобы сервер не заблокировал воркер на часы.
RETRY_AFTER_MAX_WAIT_SECONDS: Final[float] = 30.0
RETRY_AFTER_HEADER: Final[str] = "Retry-After"

_EXPONENTIAL_BACKOFF: Final = wait_exponential(
    multiplier=EXPONENTIAL_BACKOFF_MULTIPLIER,
    min=EXPONENTIAL_BACKOFF_MIN_SECONDS,
    max=EXPONENTIAL_BACKOFF_MAX_SECONDS,
)


def _get_last_exception(*, retry_state: RetryCallState) -> BaseException | None:
    """Возвращает исключение последней неудачной попытки.

    Args:
        retry_state: Состояние текущих повторов tenacity.

    Returns:
        Исключение последней попытки или None, если результат не является ошибкой.
    """
    if retry_state.outcome is None:
        return None

    return retry_state.outcome.exception()


def _parse_retry_after(*, raw_value: str) -> float | None:
    """Разбирает значение заголовка Retry-After в количество секунд ожидания.

    Поддерживаются оба формата RFC 9110: число секунд и HTTP-дата.

    Args:
        raw_value: Сырое значение заголовка Retry-After.

    Returns:
        Количество секунд ожидания (не меньше нуля) или None, если значение не распознано.
    """
    stripped_value = raw_value.strip()
    try:
        # Формат 1: количество секунд, например "120".
        seconds = float(stripped_value)
        # Проверяем, что на вход пришло конечное число (отсекаем мусорный заголовок)
        if not math.isfinite(seconds):
            return None
    except ValueError:
        # Формат 2: HTTP-дата, например "Wed, 21 Oct 2015 07:28:00 GMT".
        try:
            retry_at = parsedate_to_datetime(stripped_value)
        except (TypeError, ValueError):
            return None

        if retry_at.tzinfo is None:
            # Дата без часового пояса трактуется как UTC, иначе вычитание из aware-datetime упадет.
            retry_at = retry_at.replace(tzinfo=UTC)

        seconds = (retry_at - datetime.now(tz=UTC)).total_seconds()

    # Дата в прошлом или отрицательное значение означают, что ждать не нужно.
    return max(0.0, seconds)


def _extract_retry_after_seconds(*, exc: BaseException | None) -> float | None:
    """Извлекает задержку из заголовка Retry-After ответа HTTP 429.

    Args:
        exc: Исключение последней попытки.

    Returns:
        Задержка в секундах или None, если исключение не 429 или заголовок отсутствует.
    """
    if not isinstance(exc, httpx.HTTPStatusError):
        return None

    # Заголовок учитываем только при 429: для остальных статусов используется обычный backoff.
    if exc.response.status_code != HTTPStatus.TOO_MANY_REQUESTS:
        return None

    raw_value = exc.response.headers.get(RETRY_AFTER_HEADER)
    if raw_value is None:
        return None

    return _parse_retry_after(raw_value=raw_value)


def _wait_retry_after_or_exponential(retry_state: RetryCallState) -> float:
    """Вычисляет паузу перед следующей попыткой.

    Приоритет у заголовка Retry-After (с верхней границей), иначе используется экспоненциальный backoff.

    Args:
        retry_state: Состояние текущих повторов tenacity.

    Returns:
        Длительность паузы в секундах.
    """
    # Извлекаем время ожидания попытки из заголовка
    retry_after_seconds = _extract_retry_after_seconds(exc=_get_last_exception(retry_state=retry_state))

    # Ждем указанное в заголовке время, но не больше максимального
    if retry_after_seconds is not None:
        return min(retry_after_seconds, RETRY_AFTER_MAX_WAIT_SECONDS)

    # Считаем на стандартную экспоненту
    return _EXPONENTIAL_BACKOFF(retry_state)


def _log_retry_attempt(retry_state: RetryCallState) -> None:
    """Логирует неудачную попытку перед ожиданием следующей.

    Args:
        retry_state: Состояние текущих повторов tenacity.
    """
    logger.error(
        "External API call failed: attempt={attempt}, max_attempts={max_attempts}, error={error}. Retrying",
        attempt=retry_state.attempt_number,
        max_attempts=RETRY_MAX_ATTEMPTS,
        error=repr(_get_last_exception(retry_state=retry_state)),
    )


def external_api_retry[WrappedFn: Callable[..., Awaitable[Any]]](
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> Callable[[WrappedFn], WrappedFn]:
    """Создает декоратор retry-политики для асинхронных вызовов внешних API.

    Политика: до 3 попыток, экспоненциальный backoff, приоритет заголовка Retry-After при HTTP 429.
    Повторяются только ошибки ``httpx.HTTPError`` (статусы и сетевые сбои); остальные исключения пробрасываются сразу.
    После исчерпания попыток пробрасывается последнее исходное исключение.

    Args:
        sleep: Функция ожидания между попытками. Подменяется в тестах, чтобы не ждать реальное время.

    Returns:
        Декоратор, применимый к асинхронной функции.
    """
    # retry() из tenacity типизирован как Any, поэтому тип декоратора задан явно.
    return retry(
        stop=stop_after_attempt(max_attempt_number=RETRY_MAX_ATTEMPTS),
        wait=_wait_retry_after_or_exponential,
        retry=retry_if_exception_type(exception_types=httpx.HTTPError),
        before_sleep=_log_retry_attempt,
        reraise=True,
        sleep=sleep,
    )
