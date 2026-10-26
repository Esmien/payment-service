"""Тесты эмулятора платежного шлюза (services.gateway)."""

import pytest

from core.constants import (
    GATEWAY_MAX_DELAY_SECONDS,
    GATEWAY_MIN_DELAY_SECONDS,
    GATEWAY_SUCCESS_PROBABILITY,
    PaymentStatus,
)
from services.gateway import PaymentGateway


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


class _FakeRandom:
    """Детерминированный источник случайности для тестов."""

    def __init__(self, *, delay: float, value: float) -> None:
        """Инициализирует предсказуемые значения.

        Args:
            delay: Значение, которое вернет ``uniform``.
            value: Значение, которое вернет ``random``.
        """
        self._delay = delay
        self._value = value
        self.bounds: tuple[float, float] | None = None

    def uniform(self, a: float, b: float) -> float:
        """Возвращает заданную задержку и запоминает границы диапазона.

        Args:
            a: Нижняя граница диапазона.
            b: Верхняя граница диапазона.

        Returns:
            Заданная задержка.
        """
        self.bounds = (a, b)
        return self._delay

    def random(self) -> float:
        """Возвращает заданное случайное значение.

        Returns:
            Значение из диапазона [0, 1).
        """
        return self._value


def _build_gateway(*, sleep: _SleepRecorder, fake_random: _FakeRandom) -> PaymentGateway:
    """Создает шлюз с подмененными зависимостями.

    Args:
        sleep: Заглушка паузы.
        fake_random: Детерминированный источник случайности.

    Returns:
        Эмулятор шлюза.
    """
    return PaymentGateway(
        sleep=sleep,
        uniform=fake_random.uniform,
        random_value=fake_random.random,
    )


async def test_process_payment_uses_configured_delay_bounds() -> None:
    """Шлюз запрашивает задержку в диапазоне из констант и ждет ее."""
    sleep = _SleepRecorder()
    fake_random = _FakeRandom(delay=3.5, value=0.0)
    gateway = _build_gateway(sleep=sleep, fake_random=fake_random)

    status = await gateway.process_payment()

    assert status == PaymentStatus.SUCCEEDED
    assert fake_random.bounds == (GATEWAY_MIN_DELAY_SECONDS, GATEWAY_MAX_DELAY_SECONDS)
    assert sleep.delays == [3.5]


@pytest.mark.parametrize(
    argnames=("random_value", "expected_status"),
    argvalues=[
        pytest.param(0.0, PaymentStatus.SUCCEEDED, id="zero"),
        pytest.param(GATEWAY_SUCCESS_PROBABILITY - 0.01, PaymentStatus.SUCCEEDED, id="below-threshold"),
        pytest.param(GATEWAY_SUCCESS_PROBABILITY, PaymentStatus.FAILED, id="threshold"),
        pytest.param(0.99, PaymentStatus.FAILED, id="high"),
    ],
)
async def test_process_payment_returns_status_by_probability(
    random_value: float,
    expected_status: PaymentStatus,
) -> None:
    """Порог 0.9 отделяет успешный исход от неуспешного.

    Args:
        random_value: Значение, которое вернет источник случайности.
        expected_status: Ожидаемый статус платежа.
    """
    sleep = _SleepRecorder()
    fake_random = _FakeRandom(delay=2.0, value=random_value)
    gateway = _build_gateway(sleep=sleep, fake_random=fake_random)

    status = await gateway.process_payment()

    assert status == expected_status
    assert sleep.delays == [2.0]
