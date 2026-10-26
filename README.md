# Payment Processing Service

Асинхронный сервис процессинга платежей: FastAPI, PostgreSQL, RabbitMQ, FastStream и transactional outbox.

## Возможности

- `POST /api/v1/payments` — создание платежа, `202 Accepted`.
- `GET /api/v1/payments/{payment_id}` — статус и детали платежа.
- Статический API-ключ в заголовке `X-API-Key`.
- Идемпотентность через обязательный `Idempotency-Key`.
- Транзакционный Outbox: платёж и событие пишутся в одной транзакции.
- Outbox Relay с `FOR UPDATE SKIP LOCKED` и публикацией в `payments.new`.
- Consumer: эмуляция шлюза (2–5 сек, 90% успех / 10% ошибка), обновление статуса, webhook.
- Retry webhook: 3 попытки, экспоненциальный backoff, поддержка `Retry-After`.
- Dead Letter Queue `payments.dlq` после исчерпания попыток.
- Docker-окружение из четырёх сервисов.

## Архитектура

```text
Клиент
  │ POST /api/v1/payments (X-API-Key, Idempotency-Key)
  ▼
FastAPI ──► PostgreSQL: INSERT payments + INSERT outbox (одна транзакция)
  │
  │ Outbox Relay: SELECT ... FOR UPDATE SKIP LOCKED
  ▼
RabbitMQ: payments.new
  │
  ▼
Consumer
  ├── эмулятор шлюза: 2–5 сек, succeeded/failed
  ├── UPDATE payments: status, processed_at
  ├── POST webhook_url: WebhookPayload
  └── при ошибке webhook после 3 попыток ──► payments.dlx ──► payments.dlq
```

Ключевые решения:

- **Idempotency-Key** защищает от повторного создания платежа: тот же ключ и тело возвращают существующий платёж, другое тело — `409 Conflict`.
- **Outbox** гарантирует, что событие не потеряется, если RabbitMQ временно недоступен.
- **Consumer идемпотентен по статусу:** если платёж уже `succeeded`/`failed`, шлюз повторно не вызывается, отправляется только webhook.
- **Commit до webhook:** недоставленный webhook не откатывает платёж.
- **DLQ** хранит сообщения, которые не удалось доставить после всех попыток.

## Быстрый старт в Docker

```bash
cp .env.example .env
# отредактируйте API_KEY на длинную случайную строку
docker compose up --build
```

Сервисы:

| Сервис | Адрес | Примечание |
|---|---|---|
| API | `http://localhost:8004` | порт задаётся `API_PORT` |
| Swagger | `http://localhost:8004/docs` | |
| RabbitMQ UI | `http://localhost:15672` | `payments` / `payments` |
| PostgreSQL | `localhost:5432` | `payments` / `payments` |

Если `8004` занят, поменяйте `API_PORT` в `.env`.

Полезные команды:

```bash
docker compose ps
docker compose logs -f api
docker compose logs -f consumer
docker compose down        # остановить
docker compose down -v     # остановить и удалить данные
```

## Переменные окружения

| Переменная | Назначение | Значение по умолчанию |
|---|---|---|
| `DATABASE_URL` | DSN PostgreSQL для локального запуска | `postgresql+asyncpg://payments:payments@localhost:5432/payments` |
| `RABBITMQ_URL` | DSN RabbitMQ для локального запуска | `amqp://guest:guest@localhost:5672/` |
| `API_KEY` | Ключ для заголовка `X-API-Key` | обязателен |
| `API_PORT` | Порт API на хосте для docker-compose | `8004` |
| `RABBITMQ_USER` | Пользователь RabbitMQ для docker-compose | `payments` |
| `RABBITMQ_PASSWORD` | Пароль RabbitMQ для docker-compose | `payments` |
| `OUTBOX_POLL_INTERVAL_SECONDS` | Пауза между циклами Outbox Relay | `1.0` |
| `OUTBOX_BATCH_SIZE` | Размер пачки outbox за цикл | `100` |
| `WEBHOOK_TIMEOUT_SECONDS` | Таймаут одного запроса webhook | `10.0` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |

В Docker переменные `DATABASE_URL` и `RABBITMQ_URL` подставляются на хосты `postgres` и `rabbitmq`; значения из `.env` нужны для локального запуска.

Дополнительно можно переопределить порты и учётные данные RabbitMQ для compose: `POSTGRES_PORT`, `RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`.

## API

### Создание платежа

```bash
export API_KEY=$(grep -E '^API_KEY=' .env | cut -d= -f2-)
export API_PORT=$(grep -E '^API_PORT=' .env | cut -d= -f2-)
export BASE_URL="http://localhost:${API_PORT:-8000}"

curl -s -X POST "$BASE_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "100.00",
    "currency": "RUB",
    "description": "Оплата заказа №42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://httpbin.org/post"
  }' | jq
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "a398b42e-47d2-4444-b423-6d2a8a569891",
  "status": "pending",
  "created_at": "2026-10-26T14:23:00.000000Z"
}
```

### Идемпотентность

Повторный запрос с тем же `Idempotency-Key` и тем же телом вернёт тот же `payment_id`.

Тот же ключ с другим телом:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST "$BASE_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "999.00",
    "currency": "RUB",
    "description": "Оплата заказа №42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://httpbin.org/post"
  }'
```

Ожидаем `409 Conflict`.

### Получение платежа

```bash
curl -s "$BASE_URL/api/v1/payments/$PAYMENT_ID" \
  -H "X-API-Key: $API_KEY" | jq
```

Через 2–5 секунд после создания `status` станет `succeeded` или `failed`, а `processed_at` — заполненным.

### Коды ошибок

| Код | Причина |
|---|---|
| `401` | нет или неверный `X-API-Key` |
| `404` | платёж не найден |
| `409` | тот же `Idempotency-Key`, но другое тело |
| `422` | невалидное тело, отсутствует `Idempotency-Key`, невалидный UUID |

## Webhook

После обработки платежа consumer отправляет `POST` на `webhook_url`, указанный при создании.

Тело:

```json
{
  "payment_id": "a398b42e-47d2-4444-b423-6d2a8a569891",
  "status": "succeeded",
  "amount": "100.00",
  "currency": "RUB",
  "metadata": {"order_id": "42"},
  "processed_at": "2026-10-26T14:23:18.066230Z"
}
```

Политика доставки:

- успех — любой `2xx`;
- ошибка — `3xx`, `4xx`, `5xx`, таймаут или сетевая ошибка;
- до 3 попыток с экспоненциальным backoff;
- при `429` приоритет у заголовка `Retry-After`;
- после 3 неудач — `NackMessage(requeue=False)` → `payments.dlx` → `payments.dlq`.

Доставка **at-least-once**: webhook может прийти повторно. Клиент должен быть идемпотентным по `payment_id`.

## Dead Letter Queue

Посмотреть очереди:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged
```

Посмотреть DLQ в Management UI: `http://localhost:15672` → Queues → `payments.dlq`.

Почистить:

```bash
docker compose exec rabbitmq rabbitmqctl purge_queue payments.dlq
```

## Локальный запуск

```bash
uv sync
docker compose up -d postgres rabbitmq
uv run alembic upgrade head

# API (API_PORT влияет только на docker-compose, локальный uvicorn слушает 8000)
uv run uvicorn api.app:create_app --factory --reload --port 8000

# в другом терминале — consumer + Outbox Relay
uv run faststream run worker.app:app
```

Для локального запуска используются `DATABASE_URL` и `RABBITMQ_URL` из `.env` (хосты `localhost`).

## Тесты и проверки

```bash
# тестам test_payment.py нужен PostgreSQL
docker compose up -d postgres

# безопаснее — отдельная база, чтобы не затронуть данные приложения
docker compose exec postgres createdb -U payments payments_test
DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test uv run pytest

uv run ruff check
uv run mypy src tests
```

Важно: тесты `tests/test_payment.py` пересоздают таблицы в базе из `DATABASE_URL`. Не запускайте их на боевой базе.

## Структура

```text
src/
├── api/                 # FastAPI: приложение, роутеры, DI
├── core/                # конфиг, БД, UoW, исключения, retry, логирование
├── repositories/        # SQLAlchemy-репозитории
├── services/            # платежи, outbox relay, gateway, webhook
├── worker/              # FastStream-приложение и consumer
├── models.py            # Payment, OutboxEvent
└── schemas.py           # Pydantic-схемы
migrations/              # Alembic
tests/                   # pytest
Dockerfile
docker-compose.yml
```

## Ограничения

- Платёжный шлюз **эмулируется**: реальные деньги не списываются, карточные данные не используются.
- Webhook не подписывается; в продакшене нужны HMAC-подпись, timestamp и защита от replay.
- Аутентификация статическая (`X-API-Key`); в продакшене — OAuth/mTLS/секреты в Vault.
- Доставка at-least-once; клиент обязан быть идемпотентным.
- Нет возвратов, chargeback, сверки и antifraud — это рамки тестового задания.
