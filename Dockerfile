# syntax=docker/dockerfile:1

FROM python:3.12-slim AS builder

# Копируем uv из официального образа: не тащим pip-зависимости в runtime.
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Сначала только манифесты: слой с зависимостями кешируется отдельно от исходников.
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Исходники и миграции нужны в runtime-образе.
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./

FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Не работаем от root: контейнеру достаточно системного пользователя.
RUN groupadd --system app && useradd --system --gid app --create-home app

# .venv уже собран в builder-стадии; исходники и миграции копируются как есть.
COPY --from=builder --chown=app:app /app /app

USER app

EXPOSE 8000

# Дефолтная команда для api; consumer переопределяет её в docker-compose.
CMD ["uvicorn", "api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
