"""Тесты исключений приложения и глобальных обработчиков ошибок (core.exceptions)."""

from http import HTTPStatus
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from core.constants import IDEMPOTENCY_CONFLICT_MESSAGE, INTERNAL_ERROR_MESSAGE, PAYMENT_NOT_FOUND_MESSAGE
from core.exceptions import (
    AppError,
    IdempotencyConflictError,
    PaymentNotFoundError,
    on_app_error,
    on_error,
)


def _build_app() -> FastAPI:
    """Собирает приложение с маршрутами, которые выбрасывают разные исключения.

    Returns:
        FastAPI-приложение с зарегистрированными обработчиками ошибок.
    """
    app = FastAPI()
    app.add_exception_handler(AppError, on_app_error)
    app.add_exception_handler(Exception, on_error)

    @app.get("/payments/not-found")
    async def raise_not_found() -> None:
        """Выбрасывает PaymentNotFoundError."""
        raise PaymentNotFoundError(payment_id=uuid4())

    @app.get("/payments/conflict")
    async def raise_conflict() -> None:
        """Выбрасывает IdempotencyConflictError."""
        raise IdempotencyConflictError(idempotency_key="key-1")

    @app.get("/crash")
    async def raise_unexpected() -> None:
        """Выбрасывает непредвиденное исключение."""
        raise RuntimeError("unexpected failure")

    return app


@pytest.mark.parametrize(
    argnames=("path", "expected_status", "expected_detail"),
    argvalues=[
        pytest.param("/payments/not-found", HTTPStatus.NOT_FOUND, PAYMENT_NOT_FOUND_MESSAGE, id="not-found"),
        pytest.param("/payments/conflict", HTTPStatus.CONFLICT, IDEMPOTENCY_CONFLICT_MESSAGE, id="conflict"),
    ],
)
async def test_app_error_is_mapped_to_http_status_and_client_message(
    path: str,
    expected_status: HTTPStatus,
    expected_detail: str,
) -> None:
    """Прикладные исключения маппятся на свой HTTP-статус и русскоязычное сообщение.

    Args:
        path: Путь маршрута, который выбрасывает исключение.
        expected_status: Ожидаемый HTTP-статус.
        expected_detail: Ожидаемое сообщение для клиента.
    """
    async with httpx.AsyncClient(transport=ASGITransport(app=_build_app()), base_url="http://test") as client:
        response = await client.get(path)

    assert response.status_code == expected_status
    assert response.json() == {"detail": expected_detail}


async def test_unhandled_exception_returns_generic_500_without_leaking_details() -> None:
    """Необработанная ошибка возвращает обобщенный 500 и не раскрывает текст исключения."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=_build_app(), raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.get("/crash")

    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response.json() == {"detail": INTERNAL_ERROR_MESSAGE}
    assert "unexpected failure" not in response.text


def test_payment_not_found_error_keeps_payment_id() -> None:
    """Исключение сохраняет идентификатор платежа для диагностики."""
    payment_id = uuid4()

    error = PaymentNotFoundError(payment_id=payment_id)

    assert error.payment_id == payment_id
    assert str(payment_id) in str(error)
