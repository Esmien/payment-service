"""Настройка глобального логирования через Loguru с JSON-сериализацией."""

import sys

from loguru import logger

from core.constants import DEFAULT_LOG_LEVEL


def configure_logging(*, level: str = DEFAULT_LOG_LEVEL) -> None:
    """Настраивает Loguru на вывод структурированных JSON-логов в stdout.

    Args:
        level: Минимальный уровень логирования (например, ``"INFO"`` или ``"ERROR"``).

    Returns:
        None
    """
    # remove() без аргументов удаляет и стандартный stderr-обработчик Loguru,
    # иначе каждая запись дублировалась бы в неструктурированном виде.
    logger.remove()
    # serialize=True формирует JSON-строку на каждую запись (ожидается сбором логов контейнера).
    logger.add(sink=sys.stdout, level=level, serialize=True)
