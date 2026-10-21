"""Тесты JSON-логирования Loguru."""

import json
from collections.abc import Iterator

import pytest
from loguru import logger

from core.logging import configure_logging


@pytest.fixture
def _restore_loguru() -> Iterator[None]:
    """Удаляет обработчики Loguru после теста, чтобы не утекать в другие тесты.

    Yields:
        None
    """
    yield
    logger.remove()


def test_configure_logging_emits_json_record(capsys: pytest.CaptureFixture[str], _restore_loguru: None) -> None:
    """Проверяет, что запись уровня INFO выводится как валидный JSON.

    Args:
        capsys: Фикстура перехвата stdout.
        _restore_loguru: Фикстура очистки обработчиков Loguru.
    """
    configure_logging(level="INFO")
    logger.info("test message")

    # Берем последнюю строку, так как у Loguru одна JSON-запись на строку.
    output_line = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(output_line)

    assert record["record"]["message"] == "test message"


def test_configure_logging_filters_records_below_level(
    capsys: pytest.CaptureFixture[str], _restore_loguru: None
) -> None:
    """Проверяет, что записи ниже заданного уровня не попадают в вывод.

    Args:
        capsys: Фикстура перехвата stdout.
        _restore_loguru: Фикстура очистки обработчиков Loguru.
    """
    configure_logging(level="INFO")
    logger.debug("hidden debug message")

    assert "hidden debug message" not in capsys.readouterr().out
