"""Тесты проверки заголовка X-API-Key (core.security)."""

from collections.abc import Mapping
from dataclasses import dataclass
from http import HTTPStatus
from typing import cast

import httpx
import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport
from pydantic import SecretStr

from core.config import get_settings
from core.constants import UNAUTHORIZED_MESSAGE
from core.exceptions import AppError, on_app_error
from core.security import API_KEY_HEADER_NAME, verify_api_key

VALID_API_KEY = "test-secret-key"
PROTECTED_PATH = "/protected"


@dataclass(frozen=True)
class _FakeSettings:
    """Минимальная подмена Settings: проверке нужен только api_key."""

    api_key: SecretStr


def _build_app() -> FastAPI:
    """Собирает минимальное приложение с одним защищенным эндпоинтом.

    Returns:
        FastAPI-приложение с подмененными настройками.
    """
    app = FastAPI()
    app.add_exception_handler(AppError, on_app_error)

    @app.get(PROTECTED_PATH, dependencies=[Depends(verify_api_key)])
    async def protected() -> dict[str, str]:
        """Защищенный эндпоинт для проверки зависимости.

        Returns:
            Статус успешного доступа.
        """
        return {"status": "ok"}

    app.dependency_overrides[get_settings] = lambda: _FakeSettings(api_key=SecretStr(VALID_API_KEY))
    return app


@pytest.mark.parametrize(
    argnames=("headers", "expected_status"),
    argvalues=[
        pytest.param({}, HTTPStatus.UNAUTHORIZED, id="missing-key"),
        pytest.param({API_KEY_HEADER_NAME: ""}, HTTPStatus.UNAUTHORIZED, id="empty-key"),
        pytest.param({API_KEY_HEADER_NAME: "wrong-key"}, HTTPStatus.UNAUTHORIZED, id="wrong-key"),
        # Не-ASCII значение передаем байтами, т.к. httpx кодирует строковые заголовки в ASCII.
        pytest.param({API_KEY_HEADER_NAME: "ключ".encode()}, HTTPStatus.UNAUTHORIZED, id="non-ascii-key"),
        pytest.param({API_KEY_HEADER_NAME: VALID_API_KEY}, HTTPStatus.OK, id="valid-key"),
    ],
)
async def test_verify_api_key_checks_header_value(
    headers: dict[str, str | bytes],
    expected_status: HTTPStatus,
) -> None:
    """Защищенный эндпоинт пускает только при совпадающем X-API-Key.

    Args:
        headers: Заголовки запроса.
        expected_status: Ожидаемый HTTP-статус ответа.
    """
    async with httpx.AsyncClient(transport=ASGITransport(app=_build_app()), base_url="http://test") as client:
        # httpx принимает байтовые значения заголовков в рантайме, но в типах объявлен только str;
        # bytes нужны, чтобы передать не-ASCII ключ без падения кодировщика клиента.
        response = await client.get(PROTECTED_PATH, headers=cast("Mapping[str, str]", headers))

    assert response.status_code == expected_status


async def test_unauthorized_response_contains_client_message() -> None:
    """Ответ 401 содержит безопасное сообщение на русском языке без внутренних деталей."""
    async with httpx.AsyncClient(transport=ASGITransport(app=_build_app()), base_url="http://test") as client:
        response = await client.get(PROTECTED_PATH)

    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert response.json() == {"detail": UNAUTHORIZED_MESSAGE}
