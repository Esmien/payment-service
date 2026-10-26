"""Тесты Outbox Relay: блокировка строк, публикация событий и сбои брокера."""

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from faststream.rabbit import RabbitQueue
from sqlalchemy import Select
from sqlalchemy.dialects import postgresql

from core.constants import EVENT_TYPE_PAYMENT_CREATED, PAYMENTS_NEW_QUEUE, OutboxStatus
from models import OutboxEvent
from services.outbox import OutboxRelay, publish_pending_events


class FakeScalarResult:
    """Результат выборки, повторяющий контракт ``ScalarResult``."""

    def __init__(self, *, events: list[OutboxEvent]) -> None:
        """Сохраняет события выборки.

        Args:
            events: События, которые вернёт выборка.
        """
        self._events = events

    def all(self) -> list[OutboxEvent]:
        """Возвращает все выбранные события.

        Returns:
            Список событий.
        """
        return list(self._events)


class FakeResult:
    """Результат выполнения запроса, отдающий события через ``scalars()``."""

    def __init__(self, *, events: list[OutboxEvent]) -> None:
        """Сохраняет события выборки.

        Args:
            events: События, которые вернёт выборка.
        """
        self._events = events

    def scalars(self) -> FakeScalarResult:
        """Возвращает обёртку над скалярной выборкой.

        Returns:
            Объект с методом ``all()``.
        """
        return FakeScalarResult(events=self._events)


class FakeSession:
    """Сессия-заглушка: фиксирует SQL, отдаёт pending-события и эмулирует commit/rollback.

    ``rollback`` возвращает события к последнему закоммиченному состоянию, как это
    сделала бы реальная БД. Без этого тест на сбой брокера видел бы изменения
    ORM-атрибутов, которые в PostgreSQL были бы отменены.
    """

    def __init__(self, *, events: list[OutboxEvent]) -> None:
        """Инициализирует сессию с заданным набором событий.

        Args:
            events: События, хранящиеся «в БД».
        """
        self._events = events
        self._committed_state: dict[UUID, tuple[str, datetime | None]] = {
            event.id: (event.status, event.processed_at) for event in events
        }
        self.statements: list[Select[Any]] = []
        self.commits = 0
        self.rollbacks = 0

    async def execute(self, statement: Select[Any]) -> FakeResult:
        """Эмулирует выборку pending-событий.

        Args:
            statement: SQL-запрос, который нужно запомнить для проверок.

        Returns:
            Результат с событиями в статусе ``pending``.
        """
        self.statements.append(statement)
        pending_events = [event for event in self._events if event.status == OutboxStatus.PENDING.value]
        return FakeResult(events=pending_events)

    async def commit(self) -> None:
        """Фиксирует транзакцию и запоминает новое состояние событий.

        Returns:
            None
        """
        self.commits += 1
        self._committed_state = {event.id: (event.status, event.processed_at) for event in self._events}

    async def rollback(self) -> None:
        """Откатывает транзакцию, возвращая события к последнему commit.

        Returns:
            None
        """
        self.rollbacks += 1
        for event in self._events:
            event.status, event.processed_at = self._committed_state[event.id]


class FakeSessionFactory:
    """Фабрика, отдающая одну и ту же сессию-заглушку как async-контекст."""

    def __init__(self, *, session: FakeSession) -> None:
        """Сохраняет сессию.

        Args:
            session: Сессия-заглушка.
        """
        self._session = session

    def __call__(self) -> "FakeSessionFactory":
        """Возвращает сам себя как контекстный менеджер.

        Returns:
            Текущий экземпляр фабрики.
        """
        return self

    async def __aenter__(self) -> FakeSession:
        """Входит в контекст и отдаёт сессию.

        Returns:
            Сессия-заглушка.
        """
        return self._session

    async def __aexit__(self, *exc_info: object) -> None:
        """Выходит из контекста.

        Args:
            *exc_info: Информация об исключении, если оно возникло.

        Returns:
            None
        """
        return None


class FakePublisher:
    """Публикатор-заглушка: записывает вызовы и умеет падать на заданной попытке."""

    def __init__(self, *, fail_on_call: int | None = None) -> None:
        """Инициализирует заглушку.

        Args:
            fail_on_call: Номер вызова ``publish`` (с единицы), на котором нужно упасть.
        """
        self._fail_on_call = fail_on_call
        self.attempts = 0
        self.calls: list[tuple[dict[str, Any], RabbitQueue, str]] = []
        self.persist_flags: list[bool] = []

    async def publish(
        self,
        *,
        message: dict[str, Any],
        queue: RabbitQueue,
        message_id: str,
        persist: bool = True,
    ) -> None:
        """Публикует сообщение или имитирует недоступность брокера.

        Args:
            message: Тело события.
            queue: Очередь публикации.
            message_id: Идентификатор сообщения.
            persist: Требование сохранить сообщение на диск брокера.

        Raises:
            ConnectionError: Если номер вызова совпал с ``fail_on_call``.
        """
        self.attempts += 1
        if self._fail_on_call is not None and self.attempts == self._fail_on_call:
            raise ConnectionError("broker is unavailable")

        self.calls.append((message, queue, message_id))
        self.persist_flags.append(persist)


def make_outbox_event(*, status: OutboxStatus = OutboxStatus.PENDING) -> OutboxEvent:
    """Создаёт transient-модель события outbox.

    Args:
        status: Статус события.

    Returns:
        Несохранённая ORM-модель события.
    """
    return OutboxEvent(
        id=uuid4(),
        event_type=EVENT_TYPE_PAYMENT_CREATED,
        payload={"payment_id": str(uuid4())},
        status=status.value,
        created_at=datetime.now(tz=UTC),
        processed_at=None,
    )


def make_queue() -> RabbitQueue:
    """Создаёт описание основной очереди для передачи в релей.

    Returns:
        Описание очереди ``payments.new``.
    """
    return RabbitQueue(name=PAYMENTS_NEW_QUEUE, durable=True)


async def test_publish_pending_events_marks_events_processed() -> None:
    """Успешная публикация переводит события в processed и проставляет processed_at одним коммитом."""
    events = [make_outbox_event(), make_outbox_event()]
    session = FakeSession(events=events)
    publisher = FakePublisher()
    queue = make_queue()

    published_count = await publish_pending_events(
        session=session,  # type: ignore[arg-type]
        publisher=publisher,
        queue=queue,
        limit=10,
    )

    assert published_count == 2
    assert [event.status for event in events] == [OutboxStatus.PROCESSED.value] * 2
    assert all(event.processed_at is not None for event in events)
    # Один коммит на всю пачку: иначе блокировки FOR UPDATE SKIP LOCKED снимаются между событиями.
    assert session.commits == 1
    assert session.rollbacks == 0
    assert [message_id for _, _, message_id in publisher.calls] == [str(event.id) for event in events]
    assert publisher.calls[0][0] is events[0].payload
    assert all(call_queue is queue for _, call_queue, _ in publisher.calls)
    assert publisher.persist_flags == [True, True]


async def test_pending_events_query_uses_skip_locked() -> None:
    """Запрос выборки блокирует строки через FOR UPDATE SKIP LOCKED и сортирует по created_at."""
    session = FakeSession(events=[])

    await publish_pending_events(
        session=session,  # type: ignore[arg-type]
        publisher=FakePublisher(),
        queue=make_queue(),
        limit=5,
    )

    compiled_sql = str(
        session.statements[0].compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "FOR UPDATE SKIP LOCKED" in compiled_sql
    assert "ORDER BY outbox.created_at" in compiled_sql


async def test_publish_pending_events_returns_zero_for_empty_batch() -> None:
    """Пустая пачка не приводит ни к публикации, ни к коммиту."""
    session = FakeSession(events=[])
    publisher = FakePublisher()

    published_count = await publish_pending_events(
        session=session,  # type: ignore[arg-type]
        publisher=publisher,
        queue=make_queue(),
        limit=10,
    )

    assert published_count == 0
    assert publisher.calls == []
    assert session.commits == 0


async def test_broker_failure_keeps_event_pending() -> None:
    """Сбой брокера откатывает транзакцию и оставляет событие в pending."""
    event = make_outbox_event()
    session = FakeSession(events=[event])

    with pytest.raises(ConnectionError):
        await publish_pending_events(
            session=session,  # type: ignore[arg-type]
            publisher=FakePublisher(fail_on_call=1),
            queue=make_queue(),
            limit=10,
        )

    assert event.status == OutboxStatus.PENDING.value
    assert event.processed_at is None
    assert session.commits == 0
    assert session.rollbacks == 1


async def test_broker_failure_mid_batch_rolls_back_whole_batch() -> None:
    """Сбой на втором событии откатывает всю пачку: оба события остаются pending."""
    events = [make_outbox_event(), make_outbox_event()]
    session = FakeSession(events=events)
    publisher = FakePublisher(fail_on_call=2)

    with pytest.raises(ConnectionError):
        await publish_pending_events(
            session=session,  # type: ignore[arg-type]
            publisher=publisher,
            queue=make_queue(),
            limit=10,
        )

    assert events[0].status == OutboxStatus.PENDING.value
    assert events[1].status == OutboxStatus.PENDING.value
    assert session.commits == 0
    assert session.rollbacks == 1
    # Первое событие уже ушло в брокер, но без commit оно будет опубликовано повторно.
    assert len(publisher.calls) == 1
    assert publisher.attempts == 2


async def test_relay_continues_after_broker_error() -> None:
    """Релей переживает сбой брокера и публикует событие на следующей итерации."""
    event = make_outbox_event()
    session = FakeSession(events=[event])
    publisher = FakePublisher(fail_on_call=1)
    relay = OutboxRelay(
        publisher=publisher,
        session_factory=FakeSessionFactory(session=session),  # type: ignore[arg-type]
        queue=make_queue(),
        poll_interval_seconds=0.01,
        batch_size=10,
    )

    task = asyncio.create_task(relay.run())
    try:
        # Ждём, пока цикл переживёт первую ошибку и опубликует событие со второй попытки.
        for _ in range(200):
            if event.status == OutboxStatus.PROCESSED.value:
                break
            await asyncio.sleep(0.01)
    finally:
        relay.stop()
        await asyncio.wait_for(task, timeout=2.0)

    assert event.status == OutboxStatus.PROCESSED.value
    assert publisher.attempts >= 2


async def test_relay_stops_by_event() -> None:
    """Вызов stop() корректно завершает фоновый цикл релея."""
    session = FakeSession(events=[])
    relay = OutboxRelay(
        publisher=FakePublisher(),
        session_factory=FakeSessionFactory(session=session),  # type: ignore[arg-type]
        queue=make_queue(),
        poll_interval_seconds=0.01,
        batch_size=10,
    )

    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.02)
    relay.stop()

    await asyncio.wait_for(task, timeout=2.0)

    assert task.done()
    assert not task.cancelled()
