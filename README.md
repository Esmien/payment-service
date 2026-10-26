# Payment Processing Service

Асинхронный процессинг платежей: FastAPI + PostgreSQL + RabbitMQ/FastStream, transactional outbox, webhook retry и DLQ.

## Суть

`POST /api/v1/payments` возвращает `202` и создаёт платёж и outbox-событие в одной транзакции. Outbox Relay публикует событие в `payments.new`, consumer эмулирует шлюз (2–5 сек, 90/10), сохраняет статус и отправляет webhook. После 3 неудач webhook сообщение уходит в DLQ.

```text
Client → API → PostgreSQL(payments + outbox) → Relay → payments.new → Consumer → webhook
                                                                      └→ DLQ
```

- `POST /api/v1/payments` — `X-API-Key`, `Idempotency-Key` → `202`.
- `GET /api/v1/payments/{id}` → `200` / `404`.

## Запуск

```bash
cp .env.example .env
# заполнить: API_KEY, POSTGRES_USER, POSTGRES_PASSWORD, RABBITMQ_USER, RABBITMQ_PASSWORD
docker compose up --build
```

- API: `http://localhost:8000` (Swagger: `/docs`)
- RabbitMQ UI: `http://127.0.0.1:15672`
- Без обязательных переменных `docker compose` падает: дефолтных кредов нет.
- Postgres и RabbitMQ слушают только `127.0.0.1`.
- Остановка: `docker compose down` / `docker compose down -v`.

## Проверка

```bash
export API_KEY=$(grep '^API_KEY=' .env | cut -d= -f2-)
export API_PORT=$(grep '^API_PORT=' .env | cut -d= -f2-)
export BASE_URL="http://localhost:${API_PORT:-8000}"

# 202 + payment_id
curl -s -X POST "$BASE_URL/api/v1/payments" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{"amount":"100.00","currency":"RUB","description":"Order 42","metadata":{"order_id":"42"},"webhook_url":"https://httpbin.org/post"}' | jq

# статус: pending → succeeded/failed через 2–5 сек
curl -s "$BASE_URL/api/v1/payments/<payment_id>" -H "X-API-Key: $API_KEY" | jq

# повтор с тем же ключом/телом → тот же payment_id; другое тело → 409
```

### Webhook / DLQ

```bash
create_payment() {
  curl -s -X POST "$BASE_URL/api/v1/payments" \
    -H "X-API-Key: $API_KEY" \
    -H "Idempotency-Key: $1" \
    -H "Content-Type: application/json" \
    -d "{\"amount\":\"10.00\",\"currency\":\"RUB\",\"description\":\"Webhook test\",\"metadata\":{},\"webhook_url\":\"$2\"}" | jq
}

# падение: example.com отвечает 405 → 3 попытки → payments.dlq
create_payment dlq-fail-1 https://example.com/webhook

# успех: httpbin.org/post отвечает 200 → DLQ не растёт
create_payment dlq-ok-1 https://httpbin.org/post

docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready
docker compose exec rabbitmq rabbitmqctl purge_queue payments.dlq
```

## Ключевые решения

| Решение | Зачем |
|---|---|
| `Idempotency-Key` + `UNIQUE` + обработка `IntegrityError` | повтор не создаёт дубль; гонка → чтение платежа победителя |
| Outbox + `FOR UPDATE SKIP LOCKED` + один `commit` на пачку | событие не теряется; блокировки держатся до конца пачки |
| At-least-once + идемпотентность consumer по статусу | дубли безопасны; финальный статус не запускает шлюз повторно |
| Webhook: 3 попытки, backoff, `Retry-After`, затем `Nack(requeue=False)` | недоставленный webhook не откатывает платёж и уходит в DLQ |
| Quorum-очереди + DLX/DLQ | переживают потерю узла; «ядовитые» сообщения не теряются |
| UoW для request-scoped сервисов, session factory для Outbox Relay | разные жизненные циклы и границы транзакций |
| Postgres/RabbitMQ на `127.0.0.1`, наружу — только API | инфраструктура не торчит в сеть |

<details>
<summary><b>Окружение, локальный запуск и тесты</b></summary>

### Сервисы

| Сервис | Порт                         | Роль |
|---|------------------------------|---|
| `api` | `8000`                       | FastAPI, миграции Alembic |
| `consumer` | —                            | FastStream, gateway, webhook, Outbox Relay |
| `postgres` | `127.0.0.1:5432`             | `payments` + `outbox` |
| `rabbitmq` | `127.0.0.1:5672`, UI `15672` | `payments.new`, DLX/DLQ |

### Переменные

| Переменная | Значение |
|---|---|
| `API_KEY`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | **обязательны, без дефолтов** |
| `POSTGRES_DB` | `payments` |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `postgres` / `5432` (внутри Docker) |
| `POSTGRES_HOST_PORT` | `5432` (на хосте) |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `rabbitmq` / `5672` (внутри Docker) |
| `RABBITMQ_HOST_PORT` / `RABBITMQ_MANAGEMENT_PORT` | `5672` / `15672` (на хосте) |
| `API_PORT` | `8004` (на хосте) |
| `DATABASE_URL` / `RABBITMQ_URL` | только для локального запуска |
| `OUTBOX_POLL_INTERVAL_SECONDS` / `OUTBOX_BATCH_SIZE` | `1.0` / `100` |
| `WEBHOOK_TIMEOUT_SECONDS` / `LOG_LEVEL` | `10.0` / `INFO` |

Если меняете креды на существующих volume, нужен `docker compose down -v` или ручное создание пользователей. Пароли со спецсимволами (`@`, `:`, `/`) нужно URL-кодировать в URL.

### Локально

```bash
set -a; source .env; set +a
uv sync
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn api.app:create_app --factory --reload --port 8000
uv run faststream run worker.app:app   # другой терминал
```

### Тесты

```bash
docker compose up -d postgres   # если ещё не запущен
uv run pytest                   # conftest создаст payments_test и очистит таблицы
uv run ruff check
uv run mypy src tests
```

- Тесты используют отдельную БД `payments_test`: `conftest` создаёт её, пересоздаёт схему и делает `TRUNCATE` до/после каждого теста.
- Если PostgreSQL недоступен, тесты с БД пропускаются; остальные выполняются.
- Переопределить URL тестовой БД: `TEST_DATABASE_URL=postgresql+asyncpg://...`.

</details>

## Ограничения

- Шлюз эмулируется: реальные деньги, карты и PCI DSS не задействованы.
- Webhook не подписан; в продакшене нужны HMAC, timestamp и replay protection.
- Доставка at-least-once: клиент должен быть идемпотентным.
- Аутентификация статическая: `X-API-Key`.
