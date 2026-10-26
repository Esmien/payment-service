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

- API: `http://localhost:8000`, Swagger: `http://localhost:8000/docs`
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

Ручная проверка — через Swagger `/docs`.

Сценарии для проверки:
- `webhook_url=https://httpbin.org/post` — успешная доставка; статус платежа меняется `pending → succeeded/failed` за 2–5 секунд, `webhook_status` становится `delivered`.
- `webhook_url=https://example.com/webhook` — ответ не 2xx; после 3 попыток сообщение уходит в `payments.dlq`, `webhook_status=failed`; повторная отправка — через replay-эндпоинт.
- Повтор с тем же `Idempotency-Key` и телом возвращает тот же `payment_id`; другое тело — `409`.

## Ключевые решения

| Решение | Зачем |
|---|---|
| `Idempotency-Key` + `UNIQUE` + обработка `IntegrityError` | повтор не создаёт дубль; при гонке возвращается платёж победителя |
| Outbox + `FOR UPDATE SKIP LOCKED` + один `commit` на пачку | событие не теряется; блокировки держатся до конца пачки |
| At-least-once + `SELECT FOR UPDATE` на платеже | дубли безопасны; два consumer'а не обрабатывают один `pending` параллельно |
| Webhook: 3 попытки, backoff, `Retry-After`, затем `Nack(requeue=False)` | недоставленный webhook не откатывает платёж и уходит в DLQ |
| `webhook_status` / `webhook_attempts` / `webhook_last_error` + replay | состояние доставки видно в API; уведомление можно доставить после DLQ |
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
| `WEBHOOK_TIMEOUT_SECONDS` / `LOG_LEVEL` | `10.0` / `INFO` |

Если меняете креды на существующих volume, нужен `docker compose down -v` или ручное создание пользователей. Пароли со спецсимволами (`@`, `:`, `/`) нужно URL-кодировать в `DATABASE_URL`/`RABBITMQ_URL`; для Docker проще использовать пароль без этих символов.

### Локальный запуск

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
docker compose up -d postgres
uv run pytest
uv run ruff check
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
- Доставка at-least-once: клиент должен быть идемпотентным.
- Аутентификация статическая: `X-API-Key`.
- После 3 неудач webhook остаётся в DLQ; автоматического планировщика повторной отправки нет — только ручной replay.
- Падение между вызовом шлюза и commit требует idempotency key на стороне провайдера; `SELECT FOR UPDATE` закрывает только параллельных consumer'ов.
- Outbox Relay живёт внутри consumer: при падении consumer публикация событий останавливается до рестарта.
