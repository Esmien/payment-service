"""Настройка глобального логирования через Loguru с JSON-сериализацией."""

import logging
import sys
from typing import Final

from loguru import logger

from core.constants import DEFAULT_LOG_LEVEL

# Сторонние библиотеки (uvicorn, faststream, sqlalchemy) пишут через стандартный logging.
# Переводим их логгеры на Loguru, чтобы весь вывод сервиса был единым JSON-потоком.
STDLIB_LOGGER_NAMES: Final[tuple[str, ...]] = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "faststream",
    "sqlalchemy",
    "alembic",
)


class InterceptHandler(logging.Handler):
    """Хэндлер, перенаправляющий записи стандартного ``logging`` в Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        """Передает запись stdlib в Loguru с сохранением уровня и источника.

        Args:
            record: Запись стандартного логгера.

        Returns:
            None
        """
        # Кастомные уровни stdlib могут отсутствовать в Loguru — тогда используем числовой код.
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Ищем в стеке первый кадр, не принадлежащий модулю logging,
        # чтобы в записи Loguru оказался реальный модуль-источник, а не сам InterceptHandler.
        frame, depth = logging.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


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

    # force=True снимает обработчики, установленные ранее (например, uvicorn при импорте),
    # иначе часть записей шла бы в stderr мимо нашего JSON-хэндлера.
    logging.basicConfig(handlers=[InterceptHandler()], level=level, force=True)

    # Указанные библиотеки объявляют свои логгеры с propagate=False, поэтому root-хэндлера им мало:
    # явно подменяем их обработчики на перехватчик.
    for logger_name in STDLIB_LOGGER_NAMES:
        intercepted_logger = logging.getLogger(name=logger_name)
        intercepted_logger.handlers = [InterceptHandler()]
        intercepted_logger.propagate = False
