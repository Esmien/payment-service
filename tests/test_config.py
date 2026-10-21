"""Тесты загрузки и валидации настроек приложения."""

from collections.abc import Iterator
from typing import Final

import pytest
from pydantic import ValidationError

from core.config import (
    DEFAULT_DATABASE_URL,
    DEFAULT_OUTBOX_BATCH_SIZE,
    DEFAULT_OUTBOX_POLL_INTERVAL_SECONDS,
    DEFAULT_RABBITMQ_URL,
    DEFAULT_WEBHOOK_TIMEOUT_SECONDS,
    Settings,
    get_settings,
)
from core.constants import DEFAULT_LOG_LEVEL

TEST_API_KEY: Final[str] = "test-api-key"

# Все переменные, которые читает Settings. Очищаем их перед каждым тестом,
# чтобы значения из реального окружения разработчика не влияли на результат.
SETTINGS_ENV_VARS: Final[tuple[str, ...]] = (
    "DATABASE_URL",
    "RABBITMQ_URL",
    "API_KEY",
    "OUTBOX_POLL_INTERVAL_SECONDS",
    "OUTBOX_BATCH_SIZE",
    "WEBHOOK_TIMEOUT_SECONDS",
    "LOG_LEVEL",
)


@pytest.fixture(autouse=True)
def _clean_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Удаляет переменные настроек из окружения перед каждым тестом.

    Args:
        monkeypatch: Фикстура для безопасного изменения окружения.
    """
    for env_name in SETTINGS_ENV_VARS:
        monkeypatch.delenv(name=env_name, raising=False)


@pytest.fixture
def _reset_settings_cache() -> Iterator[None]:
    """Сбрасывает кеш get_settings до и после теста.

    Yields:
        None
    """
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_settings_loads_values_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Проверяет, что все поля заполняются из переменных окружения.

    Args:
        monkeypatch: Фикстура для установки переменных окружения.
    """
    monkeypatch.setenv(name="DATABASE_URL", value="postgresql+asyncpg://u:p@db:5432/payments")
    monkeypatch.setenv(name="RABBITMQ_URL", value="amqp://u:p@rabbit:5672/")
    monkeypatch.setenv(name="API_KEY", value=TEST_API_KEY)
    monkeypatch.setenv(name="OUTBOX_POLL_INTERVAL_SECONDS", value="0.5")
    monkeypatch.setenv(name="OUTBOX_BATCH_SIZE", value="50")
    monkeypatch.setenv(name="WEBHOOK_TIMEOUT_SECONDS", value="3")
    monkeypatch.setenv(name="LOG_LEVEL", value="DEBUG")

    # _env_file=None отключает чтение реального .env, чтобы тест был изолированным.
    settings = Settings(_env_file=None)

    assert str(settings.database_url) == "postgresql+asyncpg://u:p@db:5432/payments"
    assert str(settings.rabbitmq_url) == "amqp://u:p@rabbit:5672/"
    assert settings.api_key.get_secret_value() == TEST_API_KEY
    assert settings.outbox_poll_interval_seconds == 0.5
    assert settings.outbox_batch_size == 50
    assert settings.webhook_timeout_seconds == 3.0
    assert settings.log_level == "DEBUG"


def test_settings_uses_defaults_for_optional_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Проверяет значения по умолчанию, если заданы только обязательные поля.

    Args:
        monkeypatch: Фикстура для установки переменных окружения.
    """
    monkeypatch.setenv(name="API_KEY", value=TEST_API_KEY)

    settings = Settings(_env_file=None)

    assert str(settings.database_url) == DEFAULT_DATABASE_URL
    assert str(settings.rabbitmq_url) == DEFAULT_RABBITMQ_URL
    assert settings.outbox_poll_interval_seconds == DEFAULT_OUTBOX_POLL_INTERVAL_SECONDS
    assert settings.outbox_batch_size == DEFAULT_OUTBOX_BATCH_SIZE
    assert settings.webhook_timeout_seconds == DEFAULT_WEBHOOK_TIMEOUT_SECONDS
    assert settings.log_level == DEFAULT_LOG_LEVEL


def test_settings_requires_api_key() -> None:
    """Проверяет, что отсутствие API_KEY приводит к ошибке валидации."""
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize(
    argnames=("env_name", "env_value"),
    argvalues=[
        ("OUTBOX_POLL_INTERVAL_SECONDS", "0"),
        ("OUTBOX_BATCH_SIZE", "-1"),
        ("WEBHOOK_TIMEOUT_SECONDS", "0"),
    ],
)
def test_settings_rejects_non_positive_numeric_values(
    monkeypatch: pytest.MonkeyPatch, env_name: str, env_value: str
) -> None:
    """Проверяет, что нулевые и отрицательные числовые параметры отклоняются.

    Args:
        monkeypatch: Фикстура для установки переменных окружения.
        env_name: Имя переменной окружения.
        env_value: Некорректное значение переменной.
    """
    monkeypatch.setenv(name="API_KEY", value=TEST_API_KEY)
    monkeypatch.setenv(name=env_name, value=env_value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_settings_rejects_non_postgres_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Проверяет, что DATABASE_URL с не-PostgreSQL схемой отклоняется.

    Args:
        monkeypatch: Фикстура для установки переменных окружения.
    """
    monkeypatch.setenv(name="API_KEY", value=TEST_API_KEY)
    monkeypatch.setenv(name="DATABASE_URL", value="mysql://u:p@db:3306/payments")

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_get_settings_returns_cached_instance(monkeypatch: pytest.MonkeyPatch, _reset_settings_cache: None) -> None:
    """Проверяет, что повторные вызовы get_settings возвращают один и тот же объект.

    Args:
        monkeypatch: Фикстура для установки переменных окружения.
        _reset_settings_cache: Фикстура сброса кеша настроек.
    """
    monkeypatch.setenv(name="API_KEY", value=TEST_API_KEY)

    assert get_settings() is get_settings()
