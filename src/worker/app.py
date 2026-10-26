"""Точка входа FastStream-воркера: consumer и фоновый Outbox Relay."""

import asyncio
from typing import Final

import httpx
from faststream import FastStream

from core.config import get_settings
from core.engine import async_session_factory
from core.logging import configure_logging
from schemas import PaymentEventPayload
from services.gateway import PaymentGateway
from services.outbox import OutboxRelay
from services.webhook import WebhookClient
from worker.broker import create_broker, declare_topology, payments_new_queue
from worker.consumer import PaymentConsumer


def create_worker_app() -> FastStream:
    """Собирает FastStream-приложение воркера.

    Returns:
        Приложение с подпиской на ``payments.new`` и фоновым Outbox Relay.
    """
    settings = get_settings()
    configure_logging(level=settings.log_level)

    broker = create_broker()
    app = FastStream(broker)

    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_seconds)
    consumer = PaymentConsumer(
        gateway=PaymentGateway(),
        webhook_client=WebhookClient(client=http_client),
    )
    relay = OutboxRelay(
        publisher=broker,
        session_factory=async_session_factory,
        queue=payments_new_queue,
        poll_interval_seconds=settings.outbox_poll_interval_seconds,
        batch_size=settings.outbox_batch_size,
    )
    relay_task: asyncio.Task[None] | None = None

    @broker.subscriber(queue=payments_new_queue)
    async def handle_payment_event(payload: PaymentEventPayload) -> None:
        """Передает событие платежа в consumer."""
        await consumer.handle(payload=payload)

    @app.after_startup
    async def start_outbox_relay() -> None:
        """Объявляет топологию и запускает фоновый Outbox Relay."""
        nonlocal relay_task
        # after_startup, а не on_startup: broker уже подключен, declare_* не упадет.
        await declare_topology(broker_instance=broker)
        relay_task = asyncio.create_task(relay.run())

    @app.on_shutdown
    async def stop_outbox_relay() -> None:
        """Останавливает relay и закрывает HTTP-клиент до остановки broker."""
        relay.stop()
        if relay_task is not None:
            await relay_task
        await http_client.aclose()

    return app


app: Final[FastStream] = create_worker_app()
