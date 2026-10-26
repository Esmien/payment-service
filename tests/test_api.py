"""Интеграционные тесты REST API платежей (TASK-3.2)."""

from datetime import UTC, datetime
from typing import Any, Final
from uuid import uuid4

from httpx import AsyncClient

from core.constants import (
    EVENT_TYPE_PAYMENT_CREATED,
    IDEMPOTENCY_CONFLICT_MESSAGE,
    IDEMPOTENCY_KEY_MAX_LENGTH,
    PAYMENT_NOT_FOUND_MESSAGE,
    UNAUTHORIZED_MESSAGE,
    WEBHOOK_REPLAY_FAILED_MESSAGE,
    WEBHOOK_REPLAY_NOT_ALLOWED_MESSAGE,
    Currency,
    OutboxStatus,
    PaymentStatus,
    WebhookStatus,
)
from core.exceptions import WebhookDeliveryError
from core.security import API_KEY_HEADER_NAME
from core.utils import RETRY_MAX_ATTEMPTS
from models import Payment
from schemas import CreatePaymentRequest
from tests.conftest import (
    TEST_API_KEY,
    InMemoryPaymentRepository,
    InMemoryUnitOfWork,
    InMemoryWebhookSender,
    PaymentFactory,
)

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


def _mark_webhook_failed(
    *,
    payment: Payment,
    attempts: int = RETRY_MAX_ATTEMPTS,
    error: str = "previous failure",
) -> None:
    """Переводит платеж в состояние «webhook исчерпал попытки».

    Args:
        payment: ORM-модель платежа из in-memory репозитория.
        attempts: Накопленное количество попыток.
        error: Текст последней ошибки.
    """
    payment.status = PaymentStatus.SUCCEEDED.value
    payment.processed_at = datetime.now(tz=UTC)
    payment.webhook_status = WebhookStatus.FAILED.value
    payment.webhook_attempts = attempts
    payment.webhook_last_error = error


async def _create_payment(*, api_client: AsyncClient, idempotency_key: str) -> None:
    """Создает платеж через API и проверяет ответ 202.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
        idempotency_key: Ключ идемпотентности создаваемого платежа.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(idempotency_key=idempotency_key),
    )
    assert response.status_code == 202


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


async def test_create_payment_with_max_length_idempotency_key_returns_202(api_client: AsyncClient) -> None:
    """Ключ максимальной длины, помещающийся в колонку, принимается.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(idempotency_key="k" * IDEMPOTENCY_KEY_MAX_LENGTH),
    )

    assert response.status_code == 202


async def test_create_payment_with_too_long_idempotency_key_returns_422(api_client: AsyncClient) -> None:
    """Ключ длиннее колонки отклоняется валидацией, а не падает 500 на INSERT.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(idempotency_key="k" * (IDEMPOTENCY_KEY_MAX_LENGTH + 1)),
    )

    assert response.status_code == 422


async def test_create_payment_with_empty_idempotency_key_returns_422(api_client: AsyncClient) -> None:
    """Пустой Idempotency-Key отклоняется валидацией.

    Args:
        api_client: HTTP-клиент поверх ASGI-приложения.
    """
    response = await api_client.post(
        url=PAYMENTS_URL,
        json=_payment_payload(),
        headers=_auth_headers(idempotency_key=""),
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
    assert body["webhook_status"] == WebhookStatus.PENDING.value
    assert body["webhook_attempts"] == 0
    assert body["webhook_last_error"] is None


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


async def test_replay_webhook_delivers_after_failure(
    api_client: AsyncClient,
    payment_repository: InMemoryPaymentRepository,
    webhook_sender: InMemoryWebhookSender,
) -> None:
    """Replay успешно доставляет webhook после DLQ и обновляет состояние платежа."""
    await _create_payment(api_client=api_client, idempotency_key="replay-success")
    payment = payment_repository.payments["replay-success"]
    _mark_webhook_failed(payment=payment)
    webhook_sender.attempts = 2

    response = await api_client.post(
        url=f"{PAYMENTS_URL}/{payment.id}/webhook/replay",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["webhook_status"] == WebhookStatus.DELIVERED.value
    assert body["webhook_attempts"] == RETRY_MAX_ATTEMPTS + 2
    assert body["webhook_last_error"] is None

    assert len(webhook_sender.calls) == 1
    url, payload = webhook_sender.calls[0]
    assert url == payment.webhook_url
    assert payload.payment_id == payment.id
    assert payload.status == PaymentStatus.SUCCEEDED


async def test_replay_webhook_failure_returns_502_and_persists_failed(
    api_client: AsyncClient,
    payment_repository: InMemoryPaymentRepository,
    webhook_sender: InMemoryWebhookSender,
) -> None:
    """Неудачный replay возвращает 502 и сохраняет failed/attempts/last_error."""
    await _create_payment(api_client=api_client, idempotency_key="replay-failure")
    payment = payment_repository.payments["replay-failure"]
    _mark_webhook_failed(payment=payment)
    webhook_sender.error = WebhookDeliveryError(
        internal_message="webhook replay failed",
        url=payment.webhook_url,
        attempts=RETRY_MAX_ATTEMPTS,
    )

    response = await api_client.post(
        url=f"{PAYMENTS_URL}/{payment.id}/webhook/replay",
        headers=_auth_headers(),
    )

    assert response.status_code == 502
    assert response.json() == {"detail": WEBHOOK_REPLAY_FAILED_MESSAGE}
    assert payment.webhook_status == WebhookStatus.FAILED.value
    assert payment.webhook_attempts == RETRY_MAX_ATTEMPTS * 2
    assert payment.webhook_last_error == "webhook replay failed"
    assert len(webhook_sender.calls) == 1


async def test_replay_webhook_not_allowed_returns_409(
    api_client: AsyncClient,
    payment_repository: InMemoryPaymentRepository,
    webhook_sender: InMemoryWebhookSender,
) -> None:
    """Replay для webhook в статусе pending отклоняется без отправки."""
    await _create_payment(api_client=api_client, idempotency_key="replay-not-allowed")
    payment = payment_repository.payments["replay-not-allowed"]

    response = await api_client.post(
        url=f"{PAYMENTS_URL}/{payment.id}/webhook/replay",
        headers=_auth_headers(),
    )

    assert response.status_code == 409
    assert response.json() == {"detail": WEBHOOK_REPLAY_NOT_ALLOWED_MESSAGE}
    assert webhook_sender.calls == []


async def test_replay_webhook_unknown_payment_returns_404(api_client: AsyncClient) -> None:
    """Replay для неизвестного платежа возвращает 404."""
    response = await api_client.post(
        url=f"{PAYMENTS_URL}/{uuid4()}/webhook/replay",
        headers=_auth_headers(),
    )

    assert response.status_code == 404
    assert response.json() == {"detail": PAYMENT_NOT_FOUND_MESSAGE}


async def test_replay_webhook_without_api_key_returns_401(api_client: AsyncClient) -> None:
    """Replay без X-API-Key отклоняется до вызова сервиса."""
    response = await api_client.post(
        url=f"{PAYMENTS_URL}/{uuid4()}/webhook/replay",
        headers=_auth_headers(api_key=None),
    )

    assert response.status_code == 401
    assert response.json() == {"detail": UNAUTHORIZED_MESSAGE}
