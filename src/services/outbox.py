"""Фоновая публикация событий transactional outbox в RabbitMQ."""

import asyncio
import contextlib
import time
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from faststream.rabbit import RabbitQueue
from loguru import logger
from sqlalchemy import delete, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from core.constants import OutboxStatus
from models import OutboxEvent

# Фабрика сессий: в приложении — async_sessionmaker, в тестах — совместимый async-контекст.
SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]


class EventPublisher(Protocol):
    """Минимальный контракт публикации сообщения, достаточный для Outbox Relay.

    Протокол позволяет подменять RabbitBroker в тестах без реального соединения с RabbitMQ.
    """

    async def publish(
        self,
        *,
        message: dict[str, Any],
        queue: RabbitQueue,
        message_id: str,
        persist: bool = True,
    ) -> object:
        """Публикует сообщение в указанную очередь.

        Args:
            message: Тело события, сериализуемое в JSON.
            queue: Очередь, в которую публикуется событие.
            message_id: Идентификатор сообщения для дедупликации на стороне брокера.
            persist: Требовать сохранения сообщения на диск брокера.

        Returns:
            Результат публикации брокера. Relay его не использует, поэтому контракт
            ограничен ``object``: ``RabbitBroker.publish`` возвращает confirmation
            frame (``Ack``/``Nack``/``Reject``/``None``).
        """
        ...


async def fetch_pending_events(*, session: AsyncSession, limit: int) -> Sequence[OutboxEvent]:
    """Выбирает пачку неопубликованных событий outbox с блокировкой строк.

    Блокировка ``FOR UPDATE SKIP LOCKED`` не даёт двум экземплярам воркера обработать
    одно и то же событие и при этом не блокирует соседние строки.

    Args:
        session: Асинхронная сессия SQLAlchemy.
        limit: Максимальное количество событий в пачке.

    Returns:
        События в порядке возрастания ``created_at``.
    """
    statement = (
        select(OutboxEvent)
        .where(OutboxEvent.status == OutboxStatus.PENDING.value)
        .order_by(OutboxEvent.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(statement)
    return result.scalars().all()


async def publish_pending_events(
    *,
    session: AsyncSession,
    publisher: EventPublisher,
    queue: RabbitQueue,
    limit: int,
) -> int:
    """Публикует пачку событий outbox и помечает их как processed одним коммитом.

    Блокировки ``FOR UPDATE SKIP LOCKED`` удерживаются до конца пачки: коммит
    внутри цикла снял бы их с оставшихся событий, и второй экземпляр relay
    смог бы опубликовать их повторно. Плата за это — at-least-once: если
    публикация упадёт на середине пачки, уже отправленные события останутся
    ``pending`` и будут опубликованы повторно. Consumer идемпотентен по
    статусу платежа, поэтому дубли безопасны.

    Args:
        session: Асинхронная сессия SQLAlchemy.
        publisher: Публикатор сообщений (RabbitBroker или подмена в тестах).
        queue: Очередь, в которую публикуются события.
        limit: Максимальное количество событий за один цикл.

    Returns:
        Количество успешно опубликованных событий.

    Raises:
        Exception: Любая ошибка брокера или БД; транзакция откатывается,
            все события пачки остаются ``pending``.
    """
    events = await fetch_pending_events(session=session, limit=limit)
    if not events:
        return 0

    published_count = 0

    try:
        for event in events:
            # persist=True: сообщение должно пережить рестарт брокера. Для quorum-очередей
            # это критично для dead-lettered сообщений: они сохраняют исходный delivery mode.
            await publisher.publish(
                message=event.payload,
                queue=queue,
                message_id=str(event.id),
                persist=True,
            )

            event.status = OutboxStatus.PROCESSED.value
            event.processed_at = datetime.now(tz=UTC)
            published_count += 1

        await session.commit()
    except Exception:
        # Откат обязателен: он снимает блокировки FOR UPDATE SKIP LOCKED
        # и возвращает пачку в pending для следующего цикла.
        await session.rollback()
        raise

    return published_count


async def cleanup_processed_events(*, session: AsyncSession, older_than: datetime) -> int:
    """Удаляет опубликованные события outbox, созданные раньше cutoff.

    Политика хранения опирается на ``created_at``, а не на ``processed_at``: так DELETE
    использует композитный индекс ``(status, created_at)``. Для outbox разница
    между созданием и публикацией мала, поэтому возраст события — достаточный
    критерий хранения. Pending-события не удаляются ни при каких условиях.

    Args:
        session: Асинхронная сессия SQLAlchemy.
        older_than: Граница хранения; события с ``created_at < older_than`` удаляются.

    Returns:
        Количество удалённых событий.

    Raises:
        Exception: Любая ошибка БД; транзакция откатывается.
    """
    statement = delete(OutboxEvent).where(
        OutboxEvent.status == OutboxStatus.PROCESSED.value,
        OutboxEvent.created_at < older_than,
    )

    try:
        # AsyncSession.execute типизирован как Result, но для DELETE фактический
        # объект — CursorResult, у которого есть rowcount.
        result = cast(CursorResult[Any], await session.execute(statement))
        await session.commit()
    except Exception:
        # Откат обязателен: иначе сессия останется в failed-транзакции
        # и следующий цикл relay не сможет продолжить работу.
        await session.rollback()
        raise

    return result.rowcount


class OutboxRelay:
    """Фоновый цикл публикации событий outbox в очередь ``payments.new``.

    Attributes:
        _publisher: Публикатор сообщений в брокер.
        _session_factory: Фабрика сессий БД.
        _queue: Очередь, в которую публикуются события.
        _poll_interval_seconds: Пауза между циклами опроса outbox.
        _batch_size: Максимальное количество событий за один цикл.
        _retention_days: Сколько дней хранить опубликованные события.
        _retention_interval_seconds: Пауза между запусками очистки outbox.
        _stop_event: Событие, сигнализирующее о необходимости остановить цикл.
    """

    def __init__(
        self,
        *,
        publisher: EventPublisher,
        session_factory: SessionFactory,
        queue: RabbitQueue,
        poll_interval_seconds: float,
        batch_size: int,
        retention_days: int,
        retention_interval_seconds: float,
    ) -> None:
        """Инициализирует релей.

        Args:
            publisher: Публикатор сообщений в брокер.
            session_factory: Фабрика асинхронных сессий БД.
            queue: Очередь, в которую публикуются события.
            poll_interval_seconds: Пауза между циклами опроса outbox.
            batch_size: Максимальное количество событий за один цикл.
            retention_days: Сколько дней хранить опубликованные события.
            retention_interval_seconds: Пауза между запусками очистки outbox.
        """
        self._publisher = publisher
        self._session_factory = session_factory
        self._queue = queue
        self._poll_interval_seconds = poll_interval_seconds
        self._batch_size = batch_size
        self._retention_days = retention_days
        self._retention_interval_seconds = retention_interval_seconds
        self._stop_event = asyncio.Event()

    async def run_once(self) -> int:
        """Выполняет один цикл: выбирает пачку событий и публикует их.

        Returns:
            Количество успешно опубликованных событий.

        Raises:
            Exception: Любая ошибка БД или брокера; сессия закрывается с откатом.
        """
        async with self._session_factory() as session:
            return await publish_pending_events(
                session=session,
                publisher=self._publisher,
                queue=self._queue,
                limit=self._batch_size,
            )

    async def run_cleanup(self) -> int:
        """Удаляет processed-события старше настроенного срока хранения.

        Returns:
            Количество удалённых событий.

        Raises:
            Exception: Ошибка БД; вызывающий цикл логирует её и продолжает работу.
        """
        cutoff = datetime.now(tz=UTC) - timedelta(days=self._retention_days)
        async with self._session_factory() as session:
            deleted_count = await cleanup_processed_events(session=session, older_than=cutoff)

        if deleted_count:
            logger.info("Outbox retention deleted {} events older than {}", deleted_count, cutoff)

        return deleted_count

    async def run(self) -> None:
        """Крутит цикл публикации и периодической очистки outbox до :meth:`stop`.

        Временные сбои БД или брокера не прерывают работу релея: ошибка логируется,
        и следующая итерация выполняется после паузы.

        Returns:
            None
        """
        logger.info(
            "Outbox relay started: poll_interval={}s, batch_size={}, retention_days={}, retention_interval={}s",
            self._poll_interval_seconds,
            self._batch_size,
            self._retention_days,
            self._retention_interval_seconds,
        )

        next_cleanup_at = time.monotonic()

        while not self._stop_event.is_set():
            # Сначала публикуем pending-события: очистка не должна задерживать платежи.
            try:
                published_count = await self.run_once()
            except Exception:
                # Фоновый цикл не должен умирать от временного сбоя: логируем traceback и пробуем снова.
                logger.exception("Outbox relay iteration failed")
            else:
                if published_count:
                    logger.info("Outbox relay published {} events", published_count)

            if time.monotonic() >= next_cleanup_at:
                try:
                    await self.run_cleanup()
                except Exception:
                    # Очистка не должна ронять relay: таблица может быть временно недоступна.
                    logger.exception("Outbox retention cleanup failed")
                finally:
                    # Даже после ошибки не долбим БД очисткой на каждой итерации.
                    next_cleanup_at = time.monotonic() + self._retention_interval_seconds

            # Ожидание либо сигнала остановки, либо истечения интервала поллинга.
            # contextlib.suppress вместо pass: таймаут — штатное завершение ожидания.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval_seconds)

        logger.info("Outbox relay stopped")

    def stop(self) -> None:
        """Сигнализирует циклу о необходимости завершиться.

        Returns:
            None
        """
        self._stop_event.set()
