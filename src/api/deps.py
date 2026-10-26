from collections.abc import AsyncGenerator
from typing import Annotated

import httpx
from fastapi import Depends

from core.config import get_settings
from core.uow import IUnitOfWork, UnitOfWork
from services.payment_service import PaymentService
from services.webhook import WebhookClient, WebhookSender
from services.webhook_replay import WebhookReplayService


async def get_uow() -> AsyncGenerator[IUnitOfWork]:
    """Провайдер UnitOfWork для инъекции зависимостей"""
    uow = UnitOfWork()
    async with uow:
        yield uow


UowDepends = Annotated[IUnitOfWork, Depends(get_uow)]


async def get_payment_service(uow: UowDepends) -> PaymentService:
    service = PaymentService(uow=uow)
    return service


PaymentServiceDepends = Annotated[PaymentService, Depends(get_payment_service)]


async def get_webhook_client() -> AsyncGenerator[WebhookSender]:
    """Создает HTTP-клиент для ручной отправки webhook на время запроса.

    Yields:
        Клиент доставки webhook; httpx-клиент закрывается после ответа.
    """
    settings = get_settings()
    async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
        yield WebhookClient(client=client)


WebhookClientDepends = Annotated[WebhookSender, Depends(get_webhook_client)]


async def get_webhook_replay_service(
    uow: UowDepends,
    webhook_client: WebhookClientDepends,
) -> WebhookReplayService:
    """Собирает сервис ручной повторной доставки webhook.

    Args:
        uow: Unit of Work текущего запроса.
        webhook_client: Клиент доставки webhook.

    Returns:
        Сервис повторной доставки.
    """
    return WebhookReplayService(uow=uow, webhook_client=webhook_client)


WebhookReplayServiceDepends = Annotated[WebhookReplayService, Depends(get_webhook_replay_service)]
