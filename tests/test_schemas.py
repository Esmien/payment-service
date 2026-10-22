"""Тесты валидации Pydantic-схем запросов, ответов и сообщений."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from core.constants import Currency, PaymentStatus
from schemas import (
    CreatePaymentRequest,
    PaymentCreatedResponse,
    PaymentDetail,
    PaymentEventPayload,
    WebhookPayload,
)

CREATED_AT = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


def _build_request_data(**overrides: object) -> dict[str, object]:
    """Формирует корректное тело запроса на создание платежа с переопределениями.

    Args:
        **overrides: Поля, которые нужно заменить в базовом теле запроса.

    Returns:
        Новый словарь с телом запроса.
    """
    data: dict[str, object] = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Оплата заказа",
        "webhook_url": "https://example.com/webhook",
    }
    data.update(overrides)
    return data


class TestCreatePaymentRequest:
    """Тесты схемы CreatePaymentRequest."""

    def test_valid_request_is_accepted(self) -> None:
        """Корректное тело запроса проходит валидацию с ожидаемыми типами полей."""
        request = CreatePaymentRequest.model_validate(_build_request_data())

        assert request.amount == Decimal("100.50")
        assert request.currency is Currency.RUB
        assert request.metadata == {}

    def test_metadata_default_is_not_shared_between_instances(self) -> None:
        """Каждый экземпляр получает собственный словарь metadata по умолчанию."""
        first = CreatePaymentRequest.model_validate(_build_request_data())
        second = CreatePaymentRequest.model_validate(_build_request_data())

        first.metadata["key"] = "value"

        assert second.metadata == {}

    def test_smallest_valid_amount_is_accepted(self) -> None:
        """Минимально допустимая сумма 0.01 проходит валидацию."""
        request = CreatePaymentRequest.model_validate(_build_request_data(amount="0.01"))

        assert request.amount == Decimal("0.01")

    @pytest.mark.parametrize("amount", ["0", "0.00", "-1.00"])
    def test_non_positive_amount_is_rejected(self, amount: str) -> None:
        """Нулевая и отрицательная сумма отклоняются.

        Args:
            amount: Значение суммы, которое должно быть отклонено.
        """
        with pytest.raises(ValidationError):
            CreatePaymentRequest.model_validate(_build_request_data(amount=amount))

    @pytest.mark.parametrize("amount", ["10.005", "0.001"])
    def test_amount_with_more_than_two_decimal_places_is_rejected(self, amount: str) -> None:
        """Сумма с более чем двумя знаками после запятой отклоняется.

        Args:
            amount: Значение суммы с лишней точностью.
        """
        with pytest.raises(ValidationError):
            CreatePaymentRequest.model_validate(_build_request_data(amount=amount))

    def test_amount_exceeding_max_digits_is_rejected(self) -> None:
        """Сумма, превышающая 12 значащих цифр (Numeric(12, 2)), отклоняется."""
        with pytest.raises(ValidationError):
            CreatePaymentRequest.model_validate(_build_request_data(amount="10000000000.00"))

    @pytest.mark.parametrize("currency", [member.value for member in Currency])
    def test_supported_currencies_are_accepted(self, currency: str) -> None:
        """Все валюты из перечисления Currency принимаются.

        Args:
            currency: Код поддерживаемой валюты.
        """
        request = CreatePaymentRequest.model_validate(_build_request_data(currency=currency))

        assert request.currency == currency

    def test_unsupported_currency_is_rejected(self) -> None:
        """Неподдерживаемая валюта отклоняется."""
        with pytest.raises(ValidationError):
            CreatePaymentRequest.model_validate(_build_request_data(currency="GBP"))

    @pytest.mark.parametrize("webhook_url", ["not-a-url", "ftp://example.com/hook"])
    def test_invalid_webhook_url_is_rejected(self, webhook_url: str) -> None:
        """Невалидный URL и URL с неподдерживаемой схемой отклоняются.

        Args:
            webhook_url: Некорректное значение webhook_url.
        """
        with pytest.raises(ValidationError):
            CreatePaymentRequest.model_validate(_build_request_data(webhook_url=webhook_url))

    def test_missing_required_field_is_rejected(self) -> None:
        """Отсутствие обязательного поля description приводит к ошибке валидации."""
        data = _build_request_data()
        del data["description"]

        with pytest.raises(ValidationError):
            CreatePaymentRequest.model_validate(data)


class TestPaymentCreatedResponse:
    """Тесты схемы PaymentCreatedResponse."""

    def test_maps_payment_detail_attributes_to_response(self) -> None:
        """Поля PaymentDetail корректно переносятся в ответ на создание платежа."""
        payment_id = uuid4()
        # Роутер собирает ответ из PaymentDetail, где поле уже называется payment_id.
        payment_detail = SimpleNamespace(payment_id=payment_id, status="pending", created_at=CREATED_AT)

        response = PaymentCreatedResponse.model_validate(payment_detail)

        assert response.payment_id == payment_id
        assert response.status is PaymentStatus.PENDING
        assert response.created_at == CREATED_AT

    def test_unknown_status_is_rejected(self) -> None:
        """Статус вне перечисления PaymentStatus отклоняется."""
        payment_detail = SimpleNamespace(payment_id=uuid4(), status="unknown", created_at=CREATED_AT)

        with pytest.raises(ValidationError):
            PaymentCreatedResponse.model_validate(payment_detail)


class TestPaymentDetailResponse:
    """Тесты схемы PaymentDetailResponse."""

    def _build_orm_payment(self, processed_at: datetime | None) -> SimpleNamespace:
        """Создает объект, имитирующий ORM-модель Payment.

        Args:
            processed_at: Значение времени обработки шлюзом.

        Returns:
            Объект с атрибутами модели Payment, включая metadata_.
        """
        return SimpleNamespace(
            id=uuid4(),
            amount=Decimal("250.00"),
            currency="USD",
            description="Подписка",
            metadata_={"order": 1},
            status="succeeded",
            idempotency_key="key-123",
            webhook_url="https://example.com/webhook",
            created_at=CREATED_AT,
            processed_at=processed_at,
        )

    def test_maps_all_fields_from_orm_object(self) -> None:
        """Все поля ORM-объекта переносятся в ответ, metadata_ читается в metadata."""
        orm_payment = self._build_orm_payment(processed_at=None)

        response = PaymentDetail.model_validate(orm_payment)

        assert response.payment_id == orm_payment.id
        assert response.amount == Decimal("250.00")
        assert response.currency is Currency.USD
        assert response.metadata == {"order": 1}
        assert response.status is PaymentStatus.SUCCEEDED
        assert response.processed_at is None

    def test_processed_at_is_accepted_when_set(self) -> None:
        """Заполненное время обработки сохраняется в ответе."""
        processed_at = datetime(2026, 10, 21, 12, 0, 5, tzinfo=UTC)
        orm_payment = self._build_orm_payment(processed_at=processed_at)

        response = PaymentDetail.model_validate(orm_payment)

        assert response.processed_at == processed_at


class TestPaymentEventPayload:
    """Тесты схемы PaymentEventPayload для очереди payments.new."""

    def test_valid_payload_is_parsed_to_uuid(self) -> None:
        """Строковый payment_id приводится к UUID."""
        payment_id = uuid4()

        payload = PaymentEventPayload.model_validate({"payment_id": str(payment_id)})

        assert payload.payment_id == payment_id
        assert isinstance(payload.payment_id, UUID)

    def test_invalid_uuid_is_rejected(self) -> None:
        """Некорректный идентификатор платежа отклоняется."""
        with pytest.raises(ValidationError):
            PaymentEventPayload.model_validate({"payment_id": "not-a-uuid"})


class TestWebhookPayload:
    """Тесты схемы WebhookPayload для исходящего вебхука."""

    def _build_data(self, **overrides: object) -> dict[str, object]:
        """Формирует корректное тело вебхука с переопределениями.

        Args:
            **overrides: Поля, которые нужно заменить в базовом теле.

        Returns:
            Новый словарь с телом вебхука.
        """
        data: dict[str, object] = {
            "payment_id": str(uuid4()),
            "status": "succeeded",
            "amount": "100.50",
            "currency": "EUR",
            "metadata": {"order": 1},
            "processed_at": CREATED_AT.isoformat(),
        }
        data.update(overrides)
        return data

    def test_valid_payload_is_accepted(self) -> None:
        """Корректное тело вебхука проходит валидацию."""
        payload = WebhookPayload.model_validate(self._build_data())

        assert payload.status is PaymentStatus.SUCCEEDED
        assert payload.currency is Currency.EUR
        assert payload.amount == Decimal("100.50")

    def test_unsupported_currency_is_rejected(self) -> None:
        """Неподдерживаемая валюта в вебхуке отклоняется."""
        with pytest.raises(ValidationError):
            WebhookPayload.model_validate(self._build_data(currency="GBP"))

    def test_missing_processed_at_is_rejected(self) -> None:
        """Отсутствие времени обработки в вебхуке приводит к ошибке валидации."""
        data = self._build_data()
        del data["processed_at"]

        with pytest.raises(ValidationError):
            WebhookPayload.model_validate(data)
