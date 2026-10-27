# Payment Processing Service

Сервис асинхронной обработки платежей: FastAPI + PostgreSQL + RabbitMQ/FastStream.
Платёж и событие outbox создаются в одной транзакции, consumer эмулирует шлюз и отправляет webhook с retry и DLQ.

## Архитектура

```text
Client → API → PostgreSQL (payments + outbox) → Outbox Relay → payments.new → Consumer → webhook
                                                                                    └→ payments.dlq
```

- API и consumer — отдельные контейнеры; Outbox Relay работает внутри consumer (осознанно, ради 4 контейнеров из ТЗ).
- Платёж и событие outbox создаются атомарно.
- Consumer идемпотентен по статусу платежа; webhook повторяется 3 раза, затем сообщение уходит в DLQ.

## Запуск

```bash
cp .env.example .env
# заполнить API_KEY, POSTGRES_USER, POSTGRES_PASSWORD, RABBITMQ_USER, RABBITMQ_PASSWORD
docker compose up --build
```

- API: `http://localhost:<API_PORT>` (по умолчанию `8000`), Swagger: `http://localhost:<API_PORT>/docs`
- RabbitMQ UI: `http://127.0.0.1:15672`
- API-контейнер сам применяет `alembic upgrade head`.
- Postgres и RabbitMQ слушают только `127.0.0.1`; наружу опубликован только API.
- Остановка: `docker compose down`; полный сброс данных: `docker compose down -v`.

## API

Все эндпоинты требуют `X-API-Key`; значение — `API_KEY` из `.env`.

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/api/v1/payments` | Создать платёж. Нужен `Idempotency-Key`. Ответ `202`, `409` при конфликте тела, `422` при ошибке валидации. |
| `GET` | `/api/v1/payments/{id}` | Получить платёж и состояние доставки webhook. Ответ `200` / `404`. |
| `POST` | `/api/v1/payments/{id}/webhook/replay` | Повторно отправить webhook после `failed`. Ответ `200`, `404` для неизвестного платежа, `409` если replay недоступен, `502` при неудачной доставке. |

### Примеры curl

Выполняйте из корня проекта; `API_KEY` и `API_PORT` возьмите из `.env`.
Примеры используют `httpbin.org`; если он недоступен, замените `webhook_url` на свой тестовый URL.

```bash
export API_KEY="$(grep -E '^API_KEY=' .env | cut -d= -f2- | tr -d '\"')"
export API_PORT="$(grep -E '^API_PORT=' .env | cut -d= -f2- | tr -d '\"')"

# 1. Создание платежа. Ключ идемпотентности сохраняем для повтора.
IDEMPOTENCY_KEY="demo-$(date +%s)"
PAYMENT_ID=$(curl -sS -X POST "http://localhost:${API_PORT}/api/v1/payments" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Idempotency-Key: ${IDEMPOTENCY_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Оплата заказа №42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://httpbin.org/post"
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["payment_id"])')

echo "PAYMENT_ID=${PAYMENT_ID}"

# 2. Идемпотентный повтор: тот же ключ и тело → тот же payment_id, 202.
curl -sS -X POST "http://localhost:${API_PORT}/api/v1/payments" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Idempotency-Key: ${IDEMPOTENCY_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Оплата заказа №42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://httpbin.org/post"
  }'

# 3. Подождите 10 секунд: gateway 2–5 с + отправка webhook.
sleep 10
curl -sS "http://localhost:${API_PORT}/api/v1/payments/${PAYMENT_ID}" \
  -H "X-API-Key: ${API_KEY}"
```

- `webhook_url=https://httpbin.org/post` — успешная доставка; статус платежа меняется `pending → succeeded/failed` за 2–5 секунд, `webhook_status` становится `delivered`.
- Повтор с тем же `Idempotency-Key` и телом возвращает тот же `payment_id`; другое тело — `409`.

### Сценарий DLQ и replay

```bash
export API_KEY="$(grep -E '^API_KEY=' .env | cut -d= -f2- | tr -d '\"')"
export API_PORT="$(grep -E '^API_PORT=' .env | cut -d= -f2- | tr -d '\"')"

# 1. Платёж с URL, который стабильно отвечает 500.
DLQ_KEY="dlq-$(date +%s)"
DLQ_PAYMENT_ID=$(curl -sS -X POST "http://localhost:${API_PORT}/api/v1/payments" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Idempotency-Key: ${DLQ_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "777.00",
    "currency": "RUB",
    "description": "DLQ replay demo",
    "metadata": {"order_id": "777"},
    "webhook_url": "https://httpbin.org/status/500"
  }' | python3 -c 'import json,sys; print(json.load(sys.stdin)["payment_id"])')

# 2. Подождите 15–20 секунд: gateway 2–5 с + 3 попытки webhook.
sleep 15
curl -sS "http://localhost:${API_PORT}/api/v1/payments/${DLQ_PAYMENT_ID}" \
  -H "X-API-Key: ${API_KEY}"
# Ожидаемо: "webhook_status": "failed", "webhook_attempts": 3

# 3. Replay, пока URL отвечает 500, вернёт 502.
curl -sS -i -X POST "http://localhost:${API_PORT}/api/v1/payments/${DLQ_PAYMENT_ID}/webhook/replay" \
  -H "X-API-Key: ${API_KEY}"

# 4. Чтобы проверить успешный replay, направьте webhook_url на рабочий URL
#    (в тестовом стеке — через SQL; в реальном сценарии URL задаётся при создании).
set -a; source .env; set +a
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "UPDATE payments SET webhook_url='https://httpbin.org/post' WHERE id='${DLQ_PAYMENT_ID}';"

# 5. Теперь replay вернёт 200, webhook_status=delivered, attempts=7
#    (3 автоматические + 3 неудачного replay + 1 успешный).
curl -sS -X POST "http://localhost:${API_PORT}/api/v1/payments/${DLQ_PAYMENT_ID}/webhook/replay" \
  -H "X-API-Key: ${API_KEY}"

# 6. Повторный replay для delivered вернёт 409.
curl -sS -i -X POST "http://localhost:${API_PORT}/api/v1/payments/${DLQ_PAYMENT_ID}/webhook/replay" \
  -H "X-API-Key: ${API_KEY}"
```

## Ключевые решения

| Решение | Зачем |
|---|---|
| `Idempotency-Key` + `UNIQUE` + обработка `IntegrityError` | повтор не создаёт дубль; при гонке возвращается платёж победителя |
| Outbox + `FOR UPDATE SKIP LOCKED` + один `commit` на пачку | событие не теряется; блокировки держатся до конца пачки |
| Consumer: `SELECT FOR UPDATE` на платеже | два consumer'а не обрабатывают один `pending` параллельно; webhook при этом at-least-once |
| Webhook: 3 попытки, backoff, `Retry-After`, затем `Nack(requeue=False)` | недоставленный webhook не откатывает платёж и уходит в DLQ |
| `webhook_status` / `webhook_attempts` / `webhook_last_error` + replay | состояние доставки видно в API; уведомление можно доставить после DLQ |
| Replay: HTTP вне транзакции | соединение из пула не занято на время HTTP; повторная доставка допустима (at-least-once) |
| Quorum-очереди + DLX/DLQ | переживают потерю узла; «ядовитые» сообщения не теряются |
| UoW для request-scoped сервисов, session factory для Outbox Relay | разные жизненные циклы и границы транзакций |
| Outbox Relay внутри consumer | осознанный компромисс: отдельный контейнер был бы чище по связанности, но нарушил бы требование ТЗ «ровно 4 контейнера»; при необходимости relay выносится отдельно |
| Outbox retention + индекс `(status, created_at)` | processed-события не растут бесконечно; выборка pending не деградирует |
| Postgres/RabbitMQ на `127.0.0.1`, наружу — только API | инфраструктура не торчит в сеть |

<details>
<summary><b>Окружение, локальный запуск и тесты</b></summary>

### Сервисы

| Сервис | Порт | Роль |
|---|---|---|
| `api` | `8000` | FastAPI, миграции Alembic |
| `consumer` | — | FastStream, gateway, webhook, Outbox Relay |
| `postgres` | `127.0.0.1:5432` | `payments` + `outbox` |
| `rabbitmq` | `127.0.0.1:5672`, UI `15672` | `payments.new`, DLX/DLQ |

### Переменные

| Переменная | Значение |
|---|---|
| `API_KEY`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | обязательны, без дефолтов |
| `POSTGRES_DB` | `payments` |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `postgres` / `5432` внутри Docker |
| `POSTGRES_HOST_PORT` | `5432` на хосте |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `rabbitmq` / `5672` внутри Docker |
| `RABBITMQ_HOST_PORT` / `RABBITMQ_MANAGEMENT_PORT` | `5672` / `15672` на хосте |
| `API_PORT` | `8000` на хосте |
| `DATABASE_URL` / `RABBITMQ_URL` | только для локального запуска |
| `OUTBOX_POLL_INTERVAL_SECONDS` / `OUTBOX_BATCH_SIZE` | `1.0` / `100` |
| `OUTBOX_RETENTION_DAYS` / `OUTBOX_RETENTION_INTERVAL_SECONDS` | `7` / `3600` |
| `WEBHOOK_TIMEOUT_SECONDS` | `10.0` |
| `LOG_LEVEL` | `INFO` |

Если меняете креды на существующих volume, нужен `docker compose down -v` или ручное создание пользователей. Пароли со спецсимволами (`@`, `:`, `/`) нужно URL-кодировать в `DATABASE_URL`/`RABBITMQ_URL`; для Docker проще использовать пароль без этих символов.

### Локальный запуск

```bash
set -a; source .env; set +a
uv sync
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn api.app:create_app --factory --reload --port "${API_PORT:-8000}"
uv run faststream run worker.app:app   # другой терминал
```

### Тесты

```bash
docker compose up -d postgres
uv run pytest
uv run ruff check
uv run ruff format --check src tests
uv run mypy src tests
```

- Тесты используют отдельную БД `payments_test`: `conftest` создаёт её, пересоздаёт схему и очищает таблицы до/после каждого теста.
- RabbitMQ для тестов не нужен.
- Если PostgreSQL недоступен, тесты с БД пропускаются; остальные выполняются.
- Переопределить URL тестовой БД: `TEST_DATABASE_URL=postgresql+asyncpg://...`.
- CI: `.github/workflows/ci.yml` запускает ruff, format-check, mypy и pytest с PostgreSQL.

</details>

## Ограничения

- Шлюз эмулируется: реальные деньги не задействованы.
- Webhook не подписан; в продакшене нужны HMAC, timestamp и replay protection.
- `webhook_url` задаётся клиентом и может указывать на внутренние адреса (SSRF): consumer и replay выполняют POST из своей сети. В продакшене нужны валидация/allowlist, запрет private/loopback/link-local и защита от DNS rebinding; сейчас такой проверки нет.
- Доставка at-least-once: клиент должен быть идемпотентным.
- Аутентификация статическая: `X-API-Key`.
- После 3 неудач webhook остаётся в DLQ; автоматического планировщика повторной отправки нет — только ручной replay.
- Replay выполняет HTTP вне транзакции и не удерживает row lock. Два параллельных replay могут доставить webhook дважды; это допустимо, клиент должен дедуплицировать уведомления по `payment_id`. При гонке итоговый `webhook_status` — последний записавший.
- Consumer удерживает `SELECT FOR UPDATE` на строке платежа на время эмуляции шлюза (2–5 с), чтобы два consumer'а не вызвали шлюз дважды. Это осознанный компромисс: без idempotency key провайдера вынос вызова из-под лока допускает двойное списание.
- Падение между вызовом шлюза и commit требует idempotency key на стороне провайдера; `SELECT FOR UPDATE` закрывает только параллельных consumer'ов.
- Если запись результата webhook в БД упадёт после успешной доставки, `webhook_status` может остаться `pending`; replay для `pending` недоступен. Ретрая записи нет — в продакшене нужен reconciliation/outbox для результата доставки.
- Retention outbox считает возраст по `created_at` и удаляет `processed` старше `OUTBOX_RETENTION_DAYS` одним `DELETE` без батчей. Для больших объёмов нужен батчевый `DELETE`; старый backlog, опубликованный только сейчас, удаляется при ближайшей очистке — это безопасно, так как событие уже опубликовано.
- Outbox Relay живёт внутри consumer: при падении consumer публикация событий останавливается до рестарта.
