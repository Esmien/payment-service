"""Настройки приложения, загружаемые из переменных окружения и файла .env."""

from functools import lru_cache
from pathlib import Path
from typing import Final

from pydantic import AmqpDsn, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from core.constants import DEFAULT_LOG_LEVEL

# Значения по умолчанию рассчитаны на локальный запуск; в docker-compose они переопределяются через environment.
DEFAULT_DATABASE_URL: Final[str] = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
DEFAULT_RABBITMQ_URL: Final[str] = "amqp://guest:guest@localhost:5672/"
DEFAULT_OUTBOX_POLL_INTERVAL_SECONDS: Final[float] = 1.0
DEFAULT_OUTBOX_BATCH_SIZE: Final[int] = 100
DEFAULT_WEBHOOK_TIMEOUT_SECONDS: Final[float] = 10.0


class Settings(BaseSettings):
    """Конфигурация сервиса, читаемая из окружения с приоритетом над файлом .env.

    Attributes:
        database_url: DSN PostgresDsn с асинхронным драйвером asyncpg.
        rabbitmq_url: DSN брокера RabbitMQ.
        api_key: Статический ключ для заголовка X-API-Key. Обязателен.
        outbox_poll_interval_seconds: Пауза между циклами опроса таблицы outbox.
        outbox_batch_size: Максимальное количество событий outbox за один цикл.
        webhook_timeout_seconds: Таймаут одного HTTP-запроса вебхука.
        log_level: Минимальный уровень логирования Loguru.
    """

    # Имена переменных окружения регистронезависимы: DATABASE_URL заполняет поле database_url.
    # extra="ignore" нужен, потому что в одном .env могут лежать переменные для других сервисов.
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent.parent.parent / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: PostgresDsn = Field(default=PostgresDsn(DEFAULT_DATABASE_URL))
    rabbitmq_url: AmqpDsn = Field(default=AmqpDsn(DEFAULT_RABBITMQ_URL))
    # Field(...) делает поле обязательным и не даёт mypy требовать аргумент в конструкторе.
    api_key: SecretStr = Field(...)
    # gt=0 запрещает нули и отрицательные значения: нулевая пауза опроса перегрузит БД.
    # Нулевой размер пачки или таймаут тоже бессмысленны, поэтому проверяем все три поля.
    outbox_poll_interval_seconds: float = Field(default=DEFAULT_OUTBOX_POLL_INTERVAL_SECONDS, gt=0)
    outbox_batch_size: int = Field(default=DEFAULT_OUTBOX_BATCH_SIZE, gt=0)
    webhook_timeout_seconds: float = Field(default=DEFAULT_WEBHOOK_TIMEOUT_SECONDS, gt=0)
    log_level: str = DEFAULT_LOG_LEVEL


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Возвращает кешированный экземпляр настроек приложения.

    Кеш нужен, чтобы не перечитывать окружение и .env при каждом обращении.
    Для тестов кеш сбрасывается через ``get_settings.cache_clear()``.

    Returns:
        Экземпляр Settings, созданный при первом вызове.

    Raises:
        pydantic.ValidationError: Если обязательные переменные отсутствуют или значения некорректны.
    """
    return Settings()
