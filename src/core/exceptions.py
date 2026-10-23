"""Кастомные исключения приложения и глобальные обработчики ошибок FastAPI.

Каждое прикладное исключение декларативно задает ``http_status_code`` и ``client_message``,
по которым глобальный обработчик формирует безопасный HTTP-ответ клиенту.
"""

from http import HTTPStatus
from typing import Any, ClassVar, Final
from uuid import UUID

from fastapi import Request
from fastapi.responses import JSONResponse
from loguru import logger

from core.constants import (
    IDEMPOTENCY_CONFLICT_MESSAGE,
    INTERNAL_ERROR_MESSAGE,
    PAYMENT_NOT_FOUND_MESSAGE,
    UNAUTHORIZED_MESSAGE,
)

# Ключ тела ответа с сообщением для клиента; совпадает с форматом FastAPI HTTPException.
RESPONSE_DETAIL_KEY: Final[str] = "detail"


class AppError(Exception):
    """Базовое прикладное исключение, от которого наследуются все доменные ошибки.

    Attributes:
        http_status_code: HTTP-статус, в который исключение маппится на уровне API.
        client_message: Безопасное для клиента сообщение на русском языке.
        internal_message: Техническое описание ошибки с контекстом (только для логов).
    """

    http_status_code: ClassVar[int] = HTTPStatus.INTERNAL_SERVER_ERROR
    client_message: ClassVar[str] = INTERNAL_ERROR_MESSAGE

    def __init__(self, internal_message: str | None = None, **context: Any) -> None:
        """Создает исключение, сохраняя переданный контекст в атрибуты и техническое сообщение.

        Args:
            internal_message: Явное техническое описание ошибки для логов.
            **context: Произвольные именованные детали ошибки (например, payment_id, idempotency_key).
        """
        for key, value in context.items():
            setattr(self, key, value)

        if internal_message is None:
            details = ", ".join(f"{key}={value}" for key, value in context.items())
            internal_message = f"{type(self).__name__}: {details}" if details else type(self).__name__

        super().__init__(internal_message)
        self.internal_message = internal_message


class PaymentNotFoundError(AppError):
    """Платеж с указанным идентификатором не найден (HTTP 404).

    Attributes:
        payment_id: Идентификатор запрошенного платежа (проставляется базовым __init__).
    """

    http_status_code: ClassVar[int] = HTTPStatus.NOT_FOUND
    client_message: ClassVar[str] = PAYMENT_NOT_FOUND_MESSAGE

    # Аннотация без значения: атрибут создается в AppError.__init__ через setattr(**context).
    # Без объявления mypy не видит поле у экземпляра и ругается в тестах.
    payment_id: UUID


class IdempotencyConflictError(AppError):
    """Повторный запрос с тем же Idempotency-Key, но другими параметрами тела (HTTP 409).

    Attributes:
        idempotency_key: Ключ идемпотентности конфликтующего запроса (проставляется базовым __init__).
    """

    http_status_code: ClassVar[int] = HTTPStatus.CONFLICT
    client_message: ClassVar[str] = IDEMPOTENCY_CONFLICT_MESSAGE

    # Аналогично PaymentNotFoundError: поле приходит через setattr(**context) в AppError.__init__.
    idempotency_key: str


class UnauthorizedError(AppError):
    """Заголовок X-API-Key отсутствует или не совпадает с настроенным ключом (HTTP 401)."""

    http_status_code: ClassVar[int] = HTTPStatus.UNAUTHORIZED
    client_message: ClassVar[str] = UNAUTHORIZED_MESSAGE


class WebhookDeliveryError(AppError):
    """Webhook не удалось доставить после исчерпания всех попыток retry (уход в DLQ на уровне worker)."""


async def on_app_error(request: Request, exc: Exception) -> JSONResponse:
    """Преобразует прикладное исключение в JSON-ответ с соответствующим HTTP-статусом.

    Args:
        request: Входящий HTTP-запрос, в котором возникла ошибка.
        exc: Исключение. Не-AppError передаются в ``on_error``.

    Returns:
        JSON-ответ вида ``{"detail": "<сообщение на русском языке>"}``.
    """
    if not isinstance(exc, AppError):
        # Защитная ветка: Starlette вызывает обработчик только для AppError, но тип аргумента шире.
        return await on_error(request=request, exc=exc)

    # Бизнес-ошибки клиента логируются как INFO: это не сбой на стороне сервиса.
    logger.info(
        "Business error handled: error_type={error_type}, method={method}, path={path}, message={message}",
        error_type=type(exc).__name__,
        method=request.method,
        path=request.url.path,
        message=exc.internal_message,
    )
    return JSONResponse(
        status_code=exc.http_status_code,
        content={RESPONSE_DETAIL_KEY: exc.client_message},
    )


async def on_error(request: Request, exc: Exception) -> JSONResponse:
    """Глобальный обработчик всех необработанных исключений.

    Полный стек трейс пишется в лог, клиенту отдается обезличенное сообщение без внутренних деталей.

    Args:
        request: Входящий HTTP-запрос, в котором возникла ошибка.
        exc: Необработанное исключение.

    Returns:
        JSON-ответ со статусом 500 и обобщенным сообщением.
    """
    # opt(exception=...) сохраняет полный traceback в JSON-записи лога.
    logger.opt(exception=exc).error(
        "Unhandled exception: method={method}, path={path}",
        method=request.method,
        path=request.url.path,
    )
    return JSONResponse(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        content={RESPONSE_DETAIL_KEY: INTERNAL_ERROR_MESSAGE},
    )
