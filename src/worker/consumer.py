"""Consumer очереди payments.new: обработка платежа и доставка webhook."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from faststream.exceptions import NackMessage
from loguru import logger

from core.constants import Currency, PaymentStatus
from core.exceptions import PaymentNotFoundError, WebhookDeliveryError
from core.uow import IUnitOfWork, UnitOfWork
from schemas import PaymentEventPayload, WebhookPayload


class PaymentProcessor(Protocol):
    """Контракт эмулятора платежного шлюза."""

    async def process_payment(self) -> PaymentStatus:
        """Возвращает итоговый статус обработки платежа.

        Returns:
            ``PaymentStatus.SUCCEEDED`` или ``PaymentStatus.FAILED``.
        """
        ...


class WebhookSender(Protocol):
    """Контракт отправки webhook-уведомления."""

    async def send(self, *, url: str, payload: WebhookPayload) -> None:
        """Отправляет уведомление на URL клиента.

        Args:
            url: URL клиента для получения уведомления.
            payload: Тело уведомления.

        Raises:
            WebhookDeliveryError: Если доставить уведомление не удалось.
        """
        ...


class PaymentConsumer:
    """Обработчик сообщений очереди ``payments.new``.

    Идемпотентен по статусу платежа: если платеж уже ``succeeded`` или ``failed``,
    эмулятор шлюза повторно не запускается — отправляется только webhook.

    Attributes:
        _gateway: Эмулятор платежного шлюза.
        _webhook_client: Клиент доставки webhook.
        _uow_factory: Фабрика Unit of Work.
    """

    def __init__(
        self,
        *,
        gateway: PaymentProcessor,
        webhook_client: WebhookSender,
        uow_factory: Callable[[], IUnitOfWork] = UnitOfWork,
    ) -> None:
        """Инициализирует consumer.

        Args:
            gateway: Эмулятор платежного шлюза.
            webhook_client: Клиент доставки webhook.
            uow_factory: Фабрика Unit of Work; в тестах подменяется.
        """
        self._gateway = gateway
        self._webhook_client = webhook_client
        self._uow_factory = uow_factory

    async def handle(self, *, payload: PaymentEventPayload) -> None:
        """Обрабатывает событие платежа из очереди ``payments.new``.

        Args:
            payload: Тело сообщения с идентификатором платежа.

        Raises:
            NackMessage: С ``requeue=False``, если платеж не найден или webhook
                не удалось доставить после всех попыток. Сообщение уйдет в DLQ.
        """
        try:
            webhook_url, webhook_payload = await self._process_payment(payload=payload)
        except PaymentNotFoundError as exc:
            logger.error("Payment from event not found, sending to DLQ: payment_id={}", payload.payment_id)
            raise NackMessage(requeue=False) from exc

        try:
            await self._webhook_client.send(url=webhook_url, payload=webhook_payload)
        except WebhookDeliveryError as exc:
            logger.error(
                "Webhook delivery failed, sending to DLQ: payment_id={}, error={}",
                payload.payment_id,
                exc.internal_message,
            )
            raise NackMessage(requeue=False) from exc

        logger.info("Payment processed and webhook delivered: payment_id={}", payload.payment_id)

    async def _process_payment(self, *, payload: PaymentEventPayload) -> tuple[str, WebhookPayload]:
        """Загружает платеж, при необходимости запускает шлюз и собирает webhook.

        Args:
            payload: Тело сообщения с идентификатором платежа.

        Returns:
            URL webhook и тело уведомления с итоговым статусом.

        Raises:
            PaymentNotFoundError: Если платеж с указанным идентификатором отсутствует.
            RuntimeError: Если у финального платежа отсутствует ``processed_at``.
        """
        async with self._uow_factory() as uow:
            payment = await uow.payment.get_by_id(payment_id=payload.payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id=payload.payment_id)

            if payment.status == PaymentStatus.PENDING.value:
                status = await self._gateway.process_payment()
                await uow.payment.update_processing_result(
                    payment=payment,
                    status=status,
                    processed_at=datetime.now(tz=UTC),
                )
                # Коммит до webhook: повторная доставка сообщения не запустит шлюз заново.
                await uow.commit()
                logger.info("Payment processed by gateway: payment_id={}, status={}", payment.id, status.value)
            else:
                logger.info(
                    "Payment already processed, skipping gateway: payment_id={}, status={}",
                    payment.id,
                    payment.status,
                )

            processed_at = payment.processed_at
            if processed_at is None:
                # Инвариант: финальный статус всегда сохраняется вместе с processed_at.
                raise RuntimeError(f"Payment {payment.id} has final status without processed_at")

            return payment.webhook_url, WebhookPayload(
                payment_id=payment.id,
                status=PaymentStatus(payment.status),
                amount=payment.amount,
                currency=Currency(payment.currency),
                metadata=payment.metadata_,
                processed_at=processed_at,
            )
