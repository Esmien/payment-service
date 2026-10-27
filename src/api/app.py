"""Фабрика FastAPI-приложения: роутеры, логирование и глобальные обработчики исключений."""

from typing import Final

from fastapi import FastAPI

from api.routers import router as payments_router
from core.config import get_settings
from core.exceptions import AppError, on_app_error, on_error
from core.logging import configure_logging

API_PREFIX: Final[str] = "/api/v1"


def create_app() -> FastAPI:
    """Собирает приложение FastAPI с роутерами и обработчиками ошибок.

    Returns:
        Сконфигурированный экземпляр FastAPI.
    """
    settings = get_settings()
    configure_logging(level=settings.log_level)

    app = FastAPI(
        title="Payment Gateway Service",
        version="0.1.0",
    )

    app.include_router(router=payments_router, prefix=API_PREFIX)

    # Starlette ищет обработчик по MRO класса исключения, поэтому AppError покрывает всех наследников
    # (PaymentNotFoundError -> 404, IdempotencyConflictError -> 409, UnauthorizedError -> 401).
    app.add_exception_handler(exc_class_or_status_code=AppError, handler=on_app_error)
    # Все необработанные исключения уходят в глобальный on_error (HTTP 500).
    app.add_exception_handler(exc_class_or_status_code=Exception, handler=on_error)

    return app


# Экземпляр создается при импорте, чтобы работал запуск `uvicorn api.app:app`.
app = create_app()


if __name__ == "__main__":
    import uvicorn

    # Для reload=True uvicorn требует строковый путь до приложения/фабрики.
    uvicorn.run(app="api.app:app", host="127.0.0.1", port=8002, reload=True)
