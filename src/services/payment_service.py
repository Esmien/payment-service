"""Сервис создания и получения платежей с транзакционным Outbox и защитой от дублей."""

from uuid import UUID

from loguru import logger
from sqlalchemy.exc import IntegrityError

from core.exceptions import IdempotencyConflictError, PaymentNotFoundError
from core.uow import IUnitOfWork

# Компромисс, чтобы не городить Protocol, нужен только для type hint
from models import Payment
from repositories.payment_repo import is_idempotency_key_violation
from schemas import CreatePaymentRequest, PaymentDetail


class PaymentService:
    """Прикладной сервис создания и получения платежей."""

    def __init__(self, *, uow: IUnitOfWork) -> None:
        """Инициализирует сервис.

        Args:
            uow: Unit of Work, управляющий транзакцией и репозиториями.
        """
        self._uow = uow

    async def create_payment(
        self,
        *,
        idempotency_key: str,
        request: CreatePaymentRequest,
    ) -> PaymentDetail:
        """Создает платеж с записью события в outbox в одной транзакции.

        Args:
            idempotency_key: Значение заголовка `Idempotency-Key`.
            request: Валидированное тело запроса на создание платежа.

        Returns:
            Детальная информация о созданном либо ранее созданном платеже.

        Raises:
            IdempotencyConflictError: Если ключ уже использован с другими параметрами.
        """
        # Платеж с таким ключом уже существует
        existing_payment = await self._uow.payment.get_by_idempotency_key(idempotency_key=idempotency_key)
        if existing_payment is not None:
            self._ensure_same_request(payment=existing_payment, request=request)
            return PaymentDetail.model_validate(existing_payment)

        try:
            # Оба INSERT (payments + outbox) выполняются в одной транзакции.
            # flush внутри метода может поднять IntegrityError, поэтому он входит в try.
            payment = await self._uow.payment.create_payment_with_outbox(
                request=request,
                idempotency_key=idempotency_key,
            )
            # Снимок DTO строим до commit: после него ORM-атрибуты могут быть expired.
            payment_detail = PaymentDetail.model_validate(payment)
            await self._uow.commit()

        except IntegrityError as error:
            if not is_idempotency_key_violation(error=error):
                # Нарушено другое ограничение (например, ck_payments_amount_positive) - не маскируем.
                raise

            # Параллельный запрос с тем же ключом успел закоммититься раньше нас.
            await self._uow.rollback()

            # Переиспользуем существующий платеж
            concurrent_payment = await self._uow.payment.get_by_idempotency_key(idempotency_key=idempotency_key)

            if concurrent_payment is None:
                # Ошибка вызвана другим, пробрасываем дальше.
                raise

            # Guard на запись платежа с тем же ключом, но другим телом.
            self._ensure_same_request(payment=concurrent_payment, request=request)
            return PaymentDetail.model_validate(concurrent_payment)

        logger.info(
            "Payment created: payment_id={payment_id}, idempotency_key={idempotency_key}",
            payment_id=str(payment_detail.payment_id),
            idempotency_key=idempotency_key,
        )

        return payment_detail

    async def get_payment_by_id(self, *, payment_id: UUID) -> PaymentDetail:
        """Возвращает платеж по идентификатору.

        Args:
            payment_id: Идентификатор платежа.

        Returns:
            Детальная информация о платеже.

        Raises:
            PaymentNotFoundError: Если платеж с указанным идентификатором не найден.
        """
        payment = await self._uow.payment.get_by_id(payment_id=payment_id)

        if payment is None:
            raise PaymentNotFoundError(payment_id=payment_id)

        return PaymentDetail.model_validate(payment)

    @staticmethod
    def _ensure_same_request(*, payment: Payment, request: CreatePaymentRequest) -> None:
        """Проверяет совпадение параметров повторного запроса с сохраненным платежом.

        Args:
            payment: Ранее сохраненный платеж.
            request: Тело повторного запроса.

        Raises:
            IdempotencyConflictError: Если параметры запроса отличаются от исходных.
        """
        # currency хранится в БД строкой, поэтому сравниваем с `.value` enum'а.
        # webhook_url хранится уже нормализованным Pydantic (str от AnyHttpUrl).
        is_same = (
            payment.amount == request.amount
            and payment.currency == request.currency.value
            and payment.description == request.description
            and payment.metadata_ == request.metadata
            and payment.webhook_url == str(request.webhook_url)
        )

        if not is_same:
            raise IdempotencyConflictError(idempotency_key=payment.idempotency_key)
