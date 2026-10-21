"""Тесты перечислений и констант домена платежей."""

import pytest

from core.constants import Currency, OutboxStatus, PaymentStatus


def test_currency_contains_only_supported_values() -> None:
    """Проверяет, что набор валют соответствует ТЗ (RUB, USD, EUR)."""
    assert {member.value for member in Currency} == {"RUB", "USD", "EUR"}


def test_currency_rejects_unsupported_value() -> None:
    """Проверяет, что неподдерживаемая валюта не конвертируется в Enum."""
    with pytest.raises(ValueError):
        Currency("GBP")


def test_payment_status_values_match_database_contract() -> None:
    """Проверяет строковые значения статусов платежа, которые хранятся в БД."""
    assert {member.value for member in PaymentStatus} == {"pending", "succeeded", "failed"}


def test_outbox_status_values_match_database_contract() -> None:
    """Проверяет строковые значения статусов outbox, которые хранятся в БД."""
    assert {member.value for member in OutboxStatus} == {"pending", "processed"}
