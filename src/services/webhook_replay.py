"""Ручная повторная доставка webhook после исчерпания автоматических попыток."""

from uuid import UUID

from loguru import logger

from core.constants import WebhookStatus
from core.exceptions import (
    PaymentNotFoundError,
    WebhookDeliveryError,
    WebhookReplayFailedError,
    WebhookReplayNotAllowedError,
)
from core.uow import IUnitOfWork
from schemas import PaymentDetail
from services.webhook import WebhookSender, build_webhook_payload


class WebhookReplayService:
    """Повторно отправляет webhook для платежа с неудачной доставкой.

    HTTP-вызов выполняется вне транзакции: платёж читается и проверяется
    в короткой транзакции, затем транзакция коммитится и соединение
    возвращается в пул. Результат доставки записывается отдельной короткой
    транзакцией.

    Доставка webhook — at-least-once. Два параллельных replay могут отправить
    уведомление дважды; это допустимо, клиент должен дедуплицировать
    уведомления по ``payment_id``.
    """

    def __init__(self, *, uow: IUnitOfWork, webhook_client: WebhookSender) -> None:
        """Инициализирует сервис.

        Args:
            uow: Unit of Work с сессией для коротких транзакций.
            webhook_client: Клиент доставки webhook.
        """
        self._uow = uow
        self._webhook_client = webhook_client

    async def replay(self, *, payment_id: UUID) -> PaymentDetail:
        """Повторно отправляет webhook и сохраняет результат доставки.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Обновленное детальное представление платежа.

        Raises:
            PaymentNotFoundError: Если платеж не найден.
            WebhookReplayNotAllowedError: Если webhook уже доставлен, еще не
                отправлялся или платеж не завершен.
            WebhookReplayFailedError: Если повторная доставка не удалась после
                всех HTTP-попыток; статус failed при этом уже сохранен.
        """
        payment = await self._uow.payment.get_by_id(payment_id=payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id=payment_id)

        # Повторять имеет смысл только уведомление, которое уже исчерпало
        # автоматические попытки. Pending может доставляться consumer'ом,
        # delivered повторно отправлять нельзя.
        if payment.webhook_status != WebhookStatus.FAILED.value or payment.processed_at is None:
            raise WebhookReplayNotAllowedError(
                payment_id=payment_id,
                webhook_status=payment.webhook_status,
            )

        # URL и payload собираем до commit: после него ORM-атрибуты могут быть
        # expired при другой настройке sessionmaker.
        webhook_url = payment.webhook_url
        payload = build_webhook_payload(payment=payment)

        # Закрываем транзакцию чтения: HTTP не должен удерживать соединение
        # из пула и row lock.
        await self._uow.commit()

        try:
            attempts = await self._webhook_client.send(url=webhook_url, payload=payload)
        except WebhookDeliveryError as exc:
            await self._uow.payment.update_webhook_result(
                payment=payment,
                status=WebhookStatus.FAILED,
                attempts=exc.attempts,
                last_error=exc.internal_message,
            )
            await self._uow.commit()
            logger.warning(
                "Webhook replay failed: payment_id={}, attempts={}",
                payment_id,
                exc.attempts,
            )
            raise WebhookReplayFailedError(payment_id=payment_id, attempts=exc.attempts) from exc

        await self._uow.payment.update_webhook_result(
            payment=payment,
            status=WebhookStatus.DELIVERED,
            attempts=attempts,
            last_error=None,
        )
        # DTO собираем до commit: после него ORM-атрибуты могут быть expired.
        payment_detail = PaymentDetail.model_validate(payment)
        await self._uow.commit()
        logger.info("Webhook replay delivered: payment_id={}, attempts={}", payment_id, attempts)

        return payment_detail
