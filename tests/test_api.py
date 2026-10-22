"""Интеграционные тесты REST API платежей (TASK-3.2)."""

from typing import Any, Final

from httpx import AsyncClient

from core.constants import (
    EVENT_TYPE_PAYMENT_CREATED,
    IDEMPOTENCY_CONFLICT_MESSAGE,
    PAYMENT_NOT_FOUND_MESSAGE,
    UNAUTHORIZED_MESSAGE,
    Currency,
    OutboxStatus,
    PaymentStatus,
)
from core.security import API_KEY_HEADER_NAME
from schemas import CreatePaymentRequest
from tests.conftest import TEST_API_KEY, InMemoryPaymentRepository, InMemoryUnitOfWork, PaymentFactory

PAYMENTS_URL: Final[str] = "/api/v1/payments"
IDEMPOTENCY_KEY_HEADER_NAME: Final[str] = "Idempotency-Key"


def _auth_headers(*, idempotency_key: str | None = None, api_key: str | None = TEST_API_KEY) -> dict[str, str]:
    """Собирает заголовки запроса, опуская переданные как None.

    Args:
        idempotency_key: Значение заголовка Idempotency-Key или None, чтобы не отправлять его.
        api_key: Значение заголовка X-API-Key или None, чтобы не отправлять его.

    Returns:
        Словарь HTTP-заголовков.
    """
    headers: dict[str, str] = {}
    if api_key is not None:
        headers[API_KEY_HEADER_NAME] = api_key
    if idempotency_key is not None:
        headers[IDEMPOTENCY_KEY_HEADER_NAME] = idempotency_key
    return headers


def _payment_payload(**overrides: Any) -> dict[str, Any]:
    """Формирует валидное тело запроса на создание платежа.

    Args:
        **overrides: Поля, которые нужно переопределить.

    Returns:
        Словарь с телом запроса.
    """
    payload: dict[str, Any] = {
        "amount": "1500.00",
        "currency": Currency.RUB.value,
        "description": "Оплата заказа №42",
        "metadata": {"order_id": "42"},
        "webhook_url": "https://example.com/webhooks/payments",
    }
    payload.update(overrides)
    return payload


async def test_create_payment_without_api_key_returns_401(api_client: AsyncClient) -> None:
    """Запрос без заголовка X-API-Key отклоняется со статусом 401.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(idempotency_key="key-1", api_key=None),
    )

    assert response.status_code == 401
    assert response.json() == {"detail": UNAUTHORIZED_MESSAGE}


async def test_create_payment_with_invalid_api_key_returns_401(api_client: AsyncClient) -> None:
    """Запрос с неверным X-API-Key отклоняется со статусом 401.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(idempotency_key="key-1", api_key="wrong-key"),
    )

    assert response.status_code == 401
    assert response.json() == {"detail": UNAUTHORIZED_MESSAGE}


async def test_create_payment_without_idempotency_key_returns_422(api_client: AsyncClient) -> None:
    """Запрос без обязательного Idempotency-Key отклоняется валидацией FastAPI.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(),
    )

    assert response.status_code == 422


async def test_create_payment_returns_202_and_writes_outbox(
    api_client: AsyncClient,
    payment_repository: InMemoryPaymentRepository,
    uow: InMemoryUnitOfWork,
) -> None:
    """Успешное создание платежа возвращает 202 и пишет событие в outbox.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
        payment_repository: Репозиторий платежей в памяти.
        uow: Unit of Work в памяти.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(idempotency_key="key-1"),
    )

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == PaymentStatus.PENDING.value
    assert uow.commit_calls == 1

    assert len(payment_repository.payments) == 1
    assert len(payment_repository.outbox_events) == 1

    outbox_event = payment_repository.outbox_events[0]
    assert outbox_event.event_type == EVENT_TYPE_PAYMENT_CREATED
    assert outbox_event.status == OutboxStatus.PENDING.value
    assert outbox_event.payload == {"payment_id": body["payment_id"]}


async def test_repeated_request_with_same_body_returns_same_payment(
    api_client: AsyncClient,
    payment_repository: InMemoryPaymentRepository,
    uow: InMemoryUnitOfWork,
) -> None:
    """Повторный запрос с тем же ключом и телом возвращает ранее созданный платеж.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
        payment_repository: Репозиторий платежей в памяти.
        uow: Unit of Work в памяти.
    """
    payload = _payment_payload()
    headers = _auth_headers(idempotency_key="key-1")

    first_response = await api_client.post(url=PAYMENTS_URL, json=payload, headers=headers)
    second_response = await api_client.post(url=PAYMENTS_URL, json=payload, headers=headers)

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert second_response.json()["payment_id"] == first_response.json()["payment_id"]

    # Второй запрос не должен создавать ни нового платежа, ни нового события outbox.
    assert len(payment_repository.payments) == 1
    assert len(payment_repository.outbox_events) == 1
    assert uow.commit_calls == 1


async def test_repeated_request_with_changed_body_returns_409(
    api_client: AsyncClient,
    payment_repository: InMemoryPaymentRepository,
) -> None:
    """Повторный запрос с тем же ключом, но другим телом возвращает 409 Conflict.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
        payment_repository: Репозиторий платежей в памяти.
    """
    headers = _auth_headers(idempotency_key="key-1")

    await api_client.post(url=PAYMENTS_URL, json=_payment_payload(), headers=headers)
    conflict_response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(amount="999.99"),
        headers=headers,
    )

    assert conflict_response.status_code == 409
    assert conflict_response.json() == {"detail": IDEMPOTENCY_CONFLICT_MESSAGE}
    assert len(payment_repository.payments) == 1


async def test_concurrent_request_returns_payment_created_by_competitor(
    api_client: AsyncClient,
    payment_repository: InMemoryPaymentRepository,
    uow: InMemoryUnitOfWork,
    make_payment: PaymentFactory,
) -> None:
    """При гонке по Idempotency-Key возвращается платеж, созданный конкурирующим запросом.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
        payment_repository: Репозиторий платежей в памяти.
        uow: Unit of Work в памяти.
        make_payment: Фабрика ORM-моделей платежа.
    """
    payload = _payment_payload()
    idempotency_key = "race-key"
    concurrent_payment = make_payment(
        request=CreatePaymentRequest(**payload),
        idempotency_key=idempotency_key,
    )
    payment_repository.simulate_concurrent_create(payment=concurrent_payment)

    response = await api_client.post(
        url=PAYMENTS_URL,
        json=payload,
        headers=_auth_headers(idempotency_key=idempotency_key),
    )

    assert response.status_code == 202
    assert response.json()["payment_id"] == str(concurrent_payment.id)
    # IntegrityError обработан: транзакция откатана, повторный коммит не выполнялся.
    assert uow.rollback_calls == 1
    assert uow.commit_calls == 0
    assert len(payment_repository.payments) == 1


async def test_get_payment_returns_details(api_client: AsyncClient) -> None:
    """GET по идентификатору возвращает детальную информацию о созданном платеже.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    payload = _payment_payload()
    created_response = await api_client.post(
        url=PAYMENTS_URL,
        json=payload,
        headers=_auth_headers(idempotency_key="key-1"),
    )
    payment_id = created_response.json()["payment_id"]

    response = await api_client.get(url=f"{PAYMENTS_URL}/{payment_id}", headers=_auth_headers())

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == payload["amount"]
    assert body["currency"] == payload["currency"]
    assert body["description"] == payload["description"]
    assert body["metadata"] == payload["metadata"]
    assert body["idempotency_key"] == "key-1"
    assert body["webhook_url"] == payload["webhook_url"]
    assert body["status"] == PaymentStatus.PENDING.value
    assert body["processed_at"] is None


async def test_get_unknown_payment_returns_404(api_client: AsyncClient) -> None:
    """GET по неизвестному идентификатору возвращает 404 с русским сообщением.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.get(
        url=f"{PAYMENTS_URL}/3fa85f64-5717-4562-b3fc-2c963f66afa6",
        headers=_auth_headers(),
    )

    assert response.status_code == 404
    assert response.json() == {"detail": PAYMENT_NOT_FOUND_MESSAGE}


async def test_get_payment_with_invalid_id_returns_422(api_client: AsyncClient) -> None:
    """GET с невалидным UUID отклоняется валидацией пути.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.get(url=f"{PAYMENTS_URL}/not-a-uuid", headers=_auth_headers())

    assert response.status_code == 422


async def test_get_payment_without_api_key_returns_401(api_client: AsyncClient) -> None:
    """GET без заголовка X-API-Key отклоняется со статусом 401.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.get(
        url=f"{PAYMENTS_URL}/3fa85f64-5717-4562-b3fc-2c963f66afa6",
        headers=_auth_headers(api_key=None),
    )

    assert response.status_code == 401
    assert response.json() == {"detail": UNAUTHORIZED_MESSAGE}
