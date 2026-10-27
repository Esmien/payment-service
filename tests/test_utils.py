"""Тесты retry-политики для вызовов внешних API (core.utils)."""

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from core.utils import (
    EXPONENTIAL_BACKOFF_MAX_SECONDS,
    EXPONENTIAL_BACKOFF_MIN_SECONDS,
    RETRY_AFTER_MAX_WAIT_SECONDS,
    RETRY_MAX_ATTEMPTS,
    external_api_retry,
)

TEST_WEBHOOK_URL = "https://hooks.example.com/webhook"
RETRY_AFTER_HEADER_NAME = "Retry-After"


class _SleepRecorder:
    """Заглушка для asyncio.sleep: запоминает паузы вместо реального ожидания."""

    def __init__(self) -> None:
        """Инициализирует пустой список пауз."""
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        """Запоминает длительность паузы.

        Args:
            delay: Запрошенная длительность паузы в секундах.
        """
        self.delays.append(delay)


def _build_status_error(*, status_code: int, headers: dict[str, str] | None = None) -> httpx.HTTPStatusError:
    """Создает HTTPStatusError с заданным статусом и заголовками.

    Args:
        status_code: HTTP-статус ответа.
        headers: Заголовки ответа.

    Returns:
        Исключение httpx с привязанными запросом и ответом.
    """
    request = httpx.Request(method="POST", url=TEST_WEBHOOK_URL)
    response = httpx.Response(status_code=status_code, headers=headers, request=request)
    return httpx.HTTPStatusError(message=f"HTTP {status_code}", request=request, response=response)


def _build_endpoint(*, failures: list[BaseException]) -> tuple[Callable[[], Awaitable[str]], list[int]]:
    """Создает асинхронный вызов, который по очереди выбрасывает заданные ошибки и затем возвращает результат.

    Args:
        failures: Ошибки, которые будут выброшены на первых попытках.

    Returns:
        Кортеж из вызова и списка с номерами выполненных попыток.
    """
    pending_failures = list(failures)
    calls: list[int] = []

    async def endpoint() -> str:
        """Имитирует внешний вызов.

        Returns:
            Строка "ok" после исчерпания заданных ошибок.
        """
        calls.append(len(calls) + 1)
        if pending_failures:
            raise pending_failures.pop(0)
        return "ok"

    return endpoint, calls


async def test_returns_result_without_retry_when_first_attempt_succeeds() -> None:
    """Успешная первая попытка не вызывает повторов и пауз."""
    recorder = _SleepRecorder()
    endpoint, calls = _build_endpoint(failures=[])

    result = await external_api_retry(sleep=recorder)(endpoint)()

    assert result == "ok"
    assert len(calls) == 1
    assert recorder.delays == []


async def test_retries_until_success_on_transient_server_errors() -> None:
    """Временные 5xx ошибки повторяются, и результат возвращается после успешной попытки."""
    recorder = _SleepRecorder()
    endpoint, calls = _build_endpoint(
        failures=[
            _build_status_error(status_code=500),
            _build_status_error(status_code=503),
        ],
    )

    result = await external_api_retry(sleep=recorder)(endpoint)()

    assert result == "ok"
    assert len(calls) == RETRY_MAX_ATTEMPTS
    assert len(recorder.delays) == RETRY_MAX_ATTEMPTS - 1


async def test_gives_up_after_max_attempts_and_reraises_original_error() -> None:
    """После исчерпания попыток пробрасывается исходная HTTP-ошибка, а задержки лежат в границах backoff."""
    recorder = _SleepRecorder()
    endpoint, calls = _build_endpoint(
        failures=[_build_status_error(status_code=500) for _ in range(RETRY_MAX_ATTEMPTS)],
    )

    with pytest.raises(httpx.HTTPStatusError):
        await external_api_retry(sleep=recorder)(endpoint)()

    assert len(calls) == RETRY_MAX_ATTEMPTS
    assert len(recorder.delays) == RETRY_MAX_ATTEMPTS - 1
    for delay in recorder.delays:
        assert EXPONENTIAL_BACKOFF_MIN_SECONDS <= delay <= EXPONENTIAL_BACKOFF_MAX_SECONDS


async def test_retry_after_header_overrides_exponential_backoff_on_429() -> None:
    """Заголовок Retry-After при 429 имеет приоритет над экспоненциальным backoff."""
    recorder = _SleepRecorder()
    endpoint, _ = _build_endpoint(
        failures=[_build_status_error(status_code=429, headers={RETRY_AFTER_HEADER_NAME: "7"})],
    )

    result = await external_api_retry(sleep=recorder)(endpoint)()

    assert result == "ok"
    assert recorder.delays == [7.0]


async def test_retry_after_is_capped_by_maximum_wait() -> None:
    """Слишком большой Retry-After ограничивается максимальным временем ожидания."""
    recorder = _SleepRecorder()
    endpoint, _ = _build_endpoint(
        failures=[_build_status_error(status_code=429, headers={RETRY_AFTER_HEADER_NAME: "3600"})],
    )

    await external_api_retry(sleep=recorder)(endpoint)()

    assert recorder.delays == [RETRY_AFTER_MAX_WAIT_SECONDS]


async def test_retry_after_supports_http_date_format() -> None:
    """Заголовок Retry-After в формате HTTP-даты распознается и переводится в паузу."""
    recorder = _SleepRecorder()
    retry_at = datetime.now(tz=UTC) + timedelta(seconds=5)
    endpoint, _ = _build_endpoint(
        failures=[
            _build_status_error(
                status_code=429,
                headers={RETRY_AFTER_HEADER_NAME: format_datetime(dt=retry_at, usegmt=True)},
            ),
        ],
    )

    await external_api_retry(sleep=recorder)(endpoint)()

    # Допуск учитывает округление HTTP-даты до секунд и время выполнения теста.
    assert recorder.delays[0] == pytest.approx(5.0, abs=1.5)


async def test_retry_after_in_past_http_date_means_no_wait() -> None:
    """HTTP-дата в прошлом означает, что ждать не нужно: пауза равна нулю."""
    recorder = _SleepRecorder()
    retry_at = datetime.now(tz=UTC) - timedelta(minutes=1)
    endpoint, _ = _build_endpoint(
        failures=[
            _build_status_error(
                status_code=429,
                headers={RETRY_AFTER_HEADER_NAME: format_datetime(dt=retry_at, usegmt=True)},
            ),
        ],
    )

    await external_api_retry(sleep=recorder)(endpoint)()

    assert recorder.delays == [0.0]


async def test_unparseable_retry_after_falls_back_to_exponential_backoff() -> None:
    """Нераспознанный заголовок Retry-After игнорируется, используется экспоненциальный backoff."""
    recorder = _SleepRecorder()
    endpoint, _ = _build_endpoint(
        failures=[_build_status_error(status_code=429, headers={RETRY_AFTER_HEADER_NAME: "soon"})],
    )

    await external_api_retry(sleep=recorder)(endpoint)()

    assert recorder.delays == [EXPONENTIAL_BACKOFF_MIN_SECONDS]


async def test_retry_after_is_ignored_for_non_429_statuses() -> None:
    """Заголовок Retry-After учитывается только при 429; для 503 действует обычный backoff."""
    recorder = _SleepRecorder()
    endpoint, _ = _build_endpoint(
        failures=[_build_status_error(status_code=503, headers={RETRY_AFTER_HEADER_NAME: "7"})],
    )

    await external_api_retry(sleep=recorder)(endpoint)()

    assert recorder.delays == [EXPONENTIAL_BACKOFF_MIN_SECONDS]


async def test_transport_errors_are_retried() -> None:
    """Сетевые ошибки соединения повторяются так же, как HTTP-ошибки."""
    recorder = _SleepRecorder()
    endpoint, calls = _build_endpoint(
        failures=[
            httpx.ConnectError(message="connection refused"),
            httpx.ConnectError(message="connection refused"),
        ],
    )

    result = await external_api_retry(sleep=recorder)(endpoint)()

    assert result == "ok"
    assert len(calls) == RETRY_MAX_ATTEMPTS


async def test_unrelated_exceptions_are_not_retried() -> None:
    """Исключения, не относящиеся к httpx, пробрасываются сразу без повторов."""
    recorder = _SleepRecorder()
    endpoint, calls = _build_endpoint(failures=[ValueError("bad payload")])

    with pytest.raises(ValueError, match="bad payload"):
        await external_api_retry(sleep=recorder)(endpoint)()

    assert len(calls) == 1
    assert recorder.delays == []


@pytest.mark.parametrize("bad_value", ["nan", "inf", "-inf", "Infinity", "1e999"])
async def test_non_finite_retry_after_falls_back_to_exponential_backoff(bad_value: str) -> None:
    recorder = _SleepRecorder()
    endpoint, _ = _build_endpoint(
        failures=[_build_status_error(status_code=429, headers={RETRY_AFTER_HEADER_NAME: bad_value})],
    )

    await external_api_retry(sleep=recorder)(endpoint)()

    assert recorder.delays == [EXPONENTIAL_BACKOFF_MIN_SECONDS]
