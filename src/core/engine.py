"""Асинхронный движок SQLAlchemy, фабрика сессий и базовый класс моделей."""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from core.config import get_settings


class Base(DeclarativeBase):
    """Базовый класс для всех декларативных моделей SQLAlchemy."""


engine: AsyncEngine = create_async_engine(url=str(get_settings().database_url), pool_pre_ping=True)
async_session_factory = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Выдает асинхронную сессию БД на время обработки запроса.

    Yields:
        Асинхронная сессия SQLAlchemy, которая закрывается после использования.
    """
    async with async_session_factory() as session:
        yield session
