"""REST-эндпоинты создания и получения платежей (префикс /api/v1/payments)."""

from typing import Annotated, Final
from uuid import UUID

from fastapi import APIRouter, Depends, Header, status

from api.deps import PaymentServiceDepends, WebhookReplayServiceDepends
from core.constants import IDEMPOTENCY_KEY_MAX_LENGTH
from core.security import verify_api_key
from schemas import CreatePaymentRequest, PaymentCreatedResponse, PaymentDetail

IDEMPOTENCY_KEY_HEADER_NAME: Final[str] = "Idempotency-Key"

# Заголовок объявлен без значения по умолчанию, поэтому он обязателен:
# при его отсутствии FastAPI сам вернет 422 Unprocessable Entity.
# max_length совпадает с payments.idempotency_key (String(255)): длинный ключ
# должен отсекаться валидацией, а не падать 500 на INSERT.
IdempotencyKeyHeader = Annotated[
    str,
    Header(
        alias=IDEMPOTENCY_KEY_HEADER_NAME,
        description="Ключ идемпотентности",
        min_length=1,
        max_length=IDEMPOTENCY_KEY_MAX_LENGTH,
    ),
]

router = APIRouter(
    prefix="/payments",
    tags=["Payment"],
    # Проверка X-API-Key применяется ко всем эндпоинтам роутера.
    dependencies=[Depends(dependency=verify_api_key)],
)


@router.post(
    path="",
    response_model=PaymentCreatedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Создание платежа",
)
async def create_payment(
    *,
    service: PaymentServiceDepends,
    body: CreatePaymentRequest,
    idempotency_key: IdempotencyKeyHeader,
) -> PaymentCreatedResponse:
    """Создает платеж либо возвращает ранее созданный при повторном запросе с тем же ключом.

    Args:
        service: Сервис платежей из DI-контейнера FastAPI.
        body: Валидированное тело запроса на создание платежа.
        idempotency_key: Значение обязательного заголовка `Idempotency-Key`.

    Returns:
        Краткая информация о платеже для ответа 202 Accepted.

    Raises:
        IdempotencyConflictError: Если ключ уже использован с другими параметрами тела.
    """
    payment_detail = await service.create_payment(idempotency_key=idempotency_key, request=body)
    return PaymentCreatedResponse.model_validate(obj=payment_detail)


@router.get(
    path="/{payment_id}",
    response_model=PaymentDetail,
    status_code=status.HTTP_200_OK,
    summary="Информация о платеже",
)
async def get_payment(*, payment_id: UUID, service: PaymentServiceDepends) -> PaymentDetail:
    """Возвращает детальную информацию о платеже по его идентификатору.

    Args:
        payment_id: Идентификатор платежа из пути запроса.
        service: Сервис платежей из DI-контейнера FastAPI.

    Returns:
        Детальная информация о платеже.

    Raises:
        PaymentNotFoundError: Если платеж с указанным идентификатором не найден.
    """
    return await service.get_payment_by_id(payment_id=payment_id)


@router.post(
    path="/{payment_id}/webhook/replay",
    response_model=PaymentDetail,
    status_code=status.HTTP_200_OK,
    summary="Повторная отправка webhook",
)
async def replay_webhook(*, payment_id: UUID, service: WebhookReplayServiceDepends) -> PaymentDetail:
    """Повторно отправляет webhook для платежа с неудачной доставкой.

    Args:
        payment_id: Идентификатор платежа из пути запроса.
        service: Сервис повторной доставки webhook из DI-контейнера FastAPI.

    Returns:
        Детальная информация о платеже с обновленным статусом доставки webhook.

    Raises:
        PaymentNotFoundError: Если платеж с указанным идентификатором не найден.
        WebhookReplayNotAllowedError: Если webhook не находится в статусе failed.
        WebhookReplayFailedError: Если повторная доставка не удалась после всех попыток.
    """
    return await service.replay(payment_id=payment_id)
