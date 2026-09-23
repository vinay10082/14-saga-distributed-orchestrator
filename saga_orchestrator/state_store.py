from __future__ import annotations

import json
import logging
import threading
from abc import ABC, abstractmethod
from typing import Optional

from .enums import SagaStatus, StepStatus
from .models import SagaContext

logger = logging.getLogger(__name__)


class StateStore(ABC):
    """Persistent state store for saga instances.

    Every mutation is synchronous and durable *before* the orchestrator acts on
    its result, so a crash between two steps can always be recovered from by
    replaying sagas that are not in a terminal status.
    """

    @abstractmethod
    def save(self, context: SagaContext) -> None: ...

    @abstractmethod
    def load(self, saga_id: str) -> Optional[SagaContext]:
        ...

    @abstractmethod
    def list_recoverable(self) -> list[SagaContext]:
        """Return sagas left in a non-terminal status by a previous crash."""

    @abstractmethod
    def set_step_status(self, saga_id: str, step_name: str, status: StepStatus) -> None: ...


class InMemoryStateStore(StateStore):
    """Volatile store, useful for local demos and development."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sagas: dict[str, SagaContext] = {}

    def save(self, context: SagaContext) -> None:
        with self._lock:
            self._sagas[context.saga_id] = context

    def load(self, saga_id: str) -> Optional[SagaContext]:
        with self._lock:
            return self._sagas.get(saga_id)

    def list_recoverable(self) -> list[SagaContext]:
        terminal = {SagaStatus.COMPLETED, SagaStatus.COMPENSATED, SagaStatus.FAILED}
        with self._lock:
            return [ctx for ctx in self._sagas.values() if ctx.status not in terminal]

    def set_step_status(self, saga_id: str, step_name: str, status: StepStatus) -> None:
        with self._lock:
            ctx = self._sagas.get(saga_id)
            if ctx:
                ctx.step_states[step_name] = status
                ctx.touch()


class PostgresStateStore(StateStore):
    """Durable store backed by PostgreSQL.

    Requires `psycopg2` and a reachable `DATABASE_URL`. The schema is created
    automatically on first use.
    """

    def __init__(self, database_url: str) -> None:
        import psycopg2
        from psycopg2.extras import Json

        self._psycopg2 = psycopg2
        self._Json = Json
        self._database_url = database_url
        self._conn = psycopg2.connect(database_url)
        self._conn.autocommit = True
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS saga_instances (
                    saga_id       TEXT PRIMARY KEY,
                    saga_name     TEXT NOT NULL,
                    status        TEXT NOT NULL,
                    data          JSONB NOT NULL,
                    step_states   JSONB NOT NULL,
                    error         TEXT,
                    created_at    TIMESTAMPTZ NOT NULL,
                    updated_at    TIMESTAMPTZ NOT NULL
                );
                """
            )

    def save(self, context: SagaContext) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO saga_instances
                    (saga_id, saga_name, status, data, step_states, error, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (saga_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    data = EXCLUDED.data,
                    step_states = EXCLUDED.step_states,
                    error = EXCLUDED.error,
                    updated_at = EXCLUDED.updated_at;
                """,
                (
                    context.saga_id,
                    context.saga_name,
                    context.status.value,
                    self._Json(context.data),
                    self._Json({k: v.value for k, v in context.step_states.items()}),
                    context.error,
                    context.created_at,
                    context.updated_at,
                ),
            )

    def load(self, saga_id: str) -> Optional[SagaContext]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT saga_id, saga_name, status, data, step_states, error, created_at, updated_at "
                "FROM saga_instances WHERE saga_id = %s",
                (saga_id,),
            )
            row = cur.fetchone()
            return self._row_to_context(row) if row else None

    def list_recoverable(self) -> list[SagaContext]:
        terminal = (SagaStatus.COMPLETED.value, SagaStatus.COMPENSATED.value, SagaStatus.FAILED.value)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT saga_id, saga_name, status, data, step_states, error, created_at, updated_at "
                "FROM saga_instances WHERE status != ALL(%s)",
                (list(terminal),),
            )
            rows = cur.fetchall()
            return [self._row_to_context(row) for row in rows]

    def set_step_status(self, saga_id: str, step_name: str, status: StepStatus) -> None:
        context = self.load(saga_id)
        if context is None:
            return
        context.step_states[step_name] = status
        context.touch()
        self.save(context)

    def _row_to_context(self, row) -> SagaContext:
        saga_id, saga_name, status, data, step_states, error, created_at, updated_at = row
        return SagaContext(
            saga_id=saga_id,
            saga_name=saga_name,
            status=SagaStatus(status),
            data=data or {},
            step_states={k: StepStatus(v) for k, v in (step_states or {}).items()},
            error=error,
            created_at=created_at,
            updated_at=updated_at,
        )


def build_state_store(store_type: str, database_url: str) -> StateStore:
    if store_type == "postgres":
        logger.info("Using PostgresStateStore (%s)", database_url)
        return PostgresStateStore(database_url)
    if store_type == "memory":
        logger.info("Using InMemoryStateStore (non-durable, for local demo use)")
        return InMemoryStateStore()
    raise ValueError(f"Unknown STATE_STORE_TYPE: {store_type!r}. Expected 'postgres' or 'memory'.")
