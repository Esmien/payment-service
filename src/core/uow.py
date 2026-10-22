"""Unit of Work: единая транзакция и репозитории для прикладного слоя."""

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession

from core.engine import async_session_factory
from repositories.payment_repo import PaymentRepository


class IUnitOfWork(ABC):
    """Абстрактный контракт Unit of Work.

    Attributes:
        session: Асинхронная сессия SQLAlchemy — владелец транзакции.
        payment: Репозиторий платежей.
    """

    session: AsyncSession
    payment: PaymentRepository

    @abstractmethod
    async def __aenter__(self) -> Self:
        """Входит в контекст Unit of Work.

        Returns:
            Экземпляр Unit of Work.
        """
        ...

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Выходит из контекста, откатывая транзакцию при ошибке и закрывая сессию.

        Args:
            exc_type: Тип исключения, если контекст завершился ошибкой.
            exc_val: Экземпляр исключения.
            exc_tb: Трейсбек исключения.
        """
        ...

    @abstractmethod
    async def commit(self) -> None:
        """Фиксирует текущую транзакцию."""
        ...

    @abstractmethod
    async def rollback(self) -> None:
        """Откатывает текущую транзакцию."""
        ...


class UnitOfWork(IUnitOfWork):
    """Конкретная реализация Unit of Work поверх SQLAlchemy `AsyncSession`.

    Транзакция фиксируется только явным вызовом `commit()`. Если контекст
    завершился исключением, изменения откатываются автоматически.
    """

    def __init__(self, session: AsyncSession | None = None) -> None:
        """Инициализирует Unit of Work.

        Args:
            session: Готовая сессия (например, из тестовой фикстуры).
                Если не передана, создается новая через `async_session_factory`.
        """
        self.session = session if session is not None else async_session_factory()
        self.payment = PaymentRepository(session=self.session)

    async def __aenter__(self) -> Self:
        """Входит в контекст Unit of Work.

        Returns:
            Экземпляр Unit of Work.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Выходит из контекста: откатывает транзакцию при ошибке и закрывает сессию.

        Args:
            exc_type: Тип исключения, если контекст завершился ошибкой.
            exc_val: Экземпляр исключения.
            exc_tb: Трейсбек исключения.
        """
        if exc_type is not None:
            await self.rollback()

        await self.session.close()

    async def commit(self) -> None:
        """Фиксирует текущую транзакцию."""
        await self.session.commit()

    async def rollback(self) -> None:
        """Откатывает текущую транзакцию."""
        await self.session.rollback()
