"""Эмулятор внешнего платежного шлюза."""

import asyncio
import random
from collections.abc import Awaitable, Callable

from core.constants import (
    GATEWAY_MAX_DELAY_SECONDS,
    GATEWAY_MIN_DELAY_SECONDS,
    GATEWAY_SUCCESS_PROBABILITY,
    PaymentStatus,
)


class PaymentGateway:
    """Эмулятор внешнего платежного шлюза.

    Имитирует сетевую задержку 2–5 секунд и возвращает успешный результат
    с вероятностью 90%. Зависимости вынесены в конструктор, чтобы тесты
    не ждали реальное время и не зависели от случайности.

    Attributes:
        _sleep: Функция асинхронной паузы.
        _uniform: Функция равномерного распределения для задержки.
        _random_value: Функция случайного числа для выбора исхода.
    """

    def __init__(
        self,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        uniform: Callable[[float, float], float] = random.uniform,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        """Инициализирует эмулятор шлюза.

        Args:
            sleep: Функция паузы; в тестах подменяется заглушкой.
            uniform: Функция равномерного распределения; в тестах подменяется.
            random_value: Функция случайного числа в диапазоне [0, 1).
        """
        self._sleep = sleep
        self._uniform = uniform
        self._random_value = random_value

    async def process_payment(self) -> PaymentStatus:
        """Эмулирует обработку платежа шлюзом.

        Returns:
            ``PaymentStatus.SUCCEEDED`` с вероятностью 90%,
            иначе ``PaymentStatus.FAILED``.
        """
        delay_seconds = self._uniform(GATEWAY_MIN_DELAY_SECONDS, GATEWAY_MAX_DELAY_SECONDS)
        await self._sleep(delay_seconds)

        # random_value() возвращает число из [0, 1); порог 0.9 дает 90% успеха.
        if self._random_value() < GATEWAY_SUCCESS_PROBABILITY:
            return PaymentStatus.SUCCEEDED

        return PaymentStatus.FAILED
