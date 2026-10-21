import asyncio
from logging.config import fileConfig

from sqlalchemy import Connection

from alembic import context
from core.config import get_settings
from core.engine import Base, engine

# Импорт регистрирует таблицы моделей в Base.metadata для autogenerate.
import models  # noqa: F401

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# DSN берется из core.config, а не из alembic.ini, чтобы не дублировать настройки.
# % экранируется, т.к. ConfigParser трактует его как спецсимвол интерполяции.
config.set_main_option("sqlalchemy.url", str(get_settings().database_url).replace("%", "%%"))
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    """Выполняет миграции поверх синхронного адаптера переданного соединения.

    Args:
        connection: Соединение SQLAlchemy, полученное через ``AsyncConnection.run_sync``.
    """
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Выполняет миграции в online-режиме через асинхронный движок.

    Использует общий ``engine`` из ``core.engine``, чтобы параметры подключения совпадали с приложением.
    """
    async with engine.connect() as connection:
        await connection.run_sync(_do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
