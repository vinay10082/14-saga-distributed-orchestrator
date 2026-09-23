from __future__ import annotations

import logging
import time
from typing import Any, Optional

from .broker import MessageBroker
from .enums import SagaStatus, StepStatus
from .models import Saga, SagaContext, SagaStep
from .state_store import StateStore

logger = logging.getLogger(__name__)


class SagaOrchestrator:
    """Executes registered sagas step by step, persisting state after every
    transition so an in-flight saga can always be recovered after a crash.

    On step failure, previously succeeded steps are compensated in reverse
    order. Each compensation is retried with linear backoff up to
    `max_compensation_retries`; if it still fails, the saga is marked FAILED
    and a message describing the stuck step is sent to the dead-letter queue
    for manual/asynchronous remediation.
    """

    def __init__(
        self,
        state_store: StateStore,
        broker: MessageBroker,
        dlq_topic: str = "saga.dead_letter",
        max_compensation_retries: int = 3,
        step_retry_backoff_ms: int = 500,
    ) -> None:
        self._state_store = state_store
        self._broker = broker
        self._dlq_topic = dlq_topic
        self._max_retries = max_compensation_retries
        self._backoff_ms = step_retry_backoff_ms
        self._sagas: dict[str, Saga] = {}

    def register(self, saga: Saga) -> None:
        self._sagas[saga.name] = saga
        logger.info("Registered saga '%s' with %d step(s)", saga.name, len(saga.steps))

    def run(self, saga_name: str, initial_data: Optional[dict[str, Any]] = None) -> SagaContext:
        """Start a brand new saga instance and drive it to completion."""
        saga = self._require_saga(saga_name)
        context = SagaContext(saga_name=saga_name, data=dict(initial_data or {}))
        return self._execute(saga, context)

    def recover(self) -> list[SagaContext]:
        """Resume every saga instance left non-terminal by a previous crash.

        Safe to call on every process startup: forward actions and
        compensations are only re-invoked for steps that had not yet reached
        a terminal (SUCCEEDED/COMPENSATED) status, and handlers are expected
        to be idempotent regardless.
        """
        recovered = []
        for context in self._state_store.list_recoverable():
            saga = self._sagas.get(context.saga_name)
            if saga is None:
                logger.warning(
                    "Cannot recover saga_id=%s: unknown saga '%s' (not registered in this process)",
                    context.saga_id,
                    context.saga_name,
                )
                continue
            logger.info("Recovering saga_id=%s name=%s status=%s", context.saga_id, context.saga_name, context.status.value)
            if context.status == SagaStatus.COMPENSATING:
                recovered.append(self._compensate(saga, context))
            else:
                recovered.append(self._execute(saga, context))
        return recovered

    # -- internals ---------------------------------------------------------

    def _require_saga(self, saga_name: str) -> Saga:
        saga = self._sagas.get(saga_name)
        if saga is None:
            raise ValueError(f"Saga '{saga_name}' is not registered")
        return saga

    def _execute(self, saga: Saga, context: SagaContext) -> SagaContext:
        context.status = SagaStatus.RUNNING
        self._state_store.save(context)

        completed_steps: list[SagaStep] = []
        failure: Optional[Exception] = None

        for step in saga.steps:
            state = context.step_states.get(step.name)
            if state == StepStatus.SUCCEEDED:
                completed_steps.append(step)
                continue

            logger.info("saga_id=%s step='%s' executing forward action", context.saga_id, step.name)
            context.step_states[step.name] = StepStatus.RUNNING
            self._state_store.save(context)
            try:
                result = step.action(context)
                if result:
                    context.data.update(result)
                context.step_states[step.name] = StepStatus.SUCCEEDED
                context.touch()
                self._state_store.save(context)
                completed_steps.append(step)
            except Exception as exc:  # noqa: BLE001 - saga steps are arbitrary user code
                logger.exception("saga_id=%s step='%s' failed", context.saga_id, step.name)
                context.step_states[step.name] = StepStatus.FAILED
                context.error = f"step '{step.name}' failed: {exc}"
                context.touch()
                self._state_store.save(context)
                failure = exc
                break

        if failure is None:
            context.status = SagaStatus.COMPLETED
            context.touch()
            self._state_store.save(context)
            logger.info("saga_id=%s name=%s COMPLETED", context.saga_id, context.saga_name)
            return context

        return self._compensate(saga, context, already_completed=completed_steps)

    def _compensate(
        self,
        saga: Saga,
        context: SagaContext,
        already_completed: Optional[list[SagaStep]] = None,
    ) -> SagaContext:
        context.status = SagaStatus.COMPENSATING
        context.touch()
        self._state_store.save(context)

        if already_completed is not None:
            steps_to_consider = already_completed
        else:
            # Recovery path: any step not still PENDING was at least attempted.
            steps_to_consider = [s for s in saga.steps if context.step_states.get(s.name) is not None]

        compensation_failed = False
        for step in reversed(steps_to_consider):
            current_state = context.step_states.get(step.name)
            if current_state in (StepStatus.COMPENSATED, StepStatus.PENDING, StepStatus.FAILED, None):
                # COMPENSATED: already undone. PENDING/None: never attempted.
                # FAILED: the forward action itself failed and never took
                # effect, so there's nothing to undo.
                continue
            if step.compensation is None:
                context.step_states[step.name] = StepStatus.COMPENSATED
                self._state_store.save(context)
                continue
            if not self._compensate_step_with_retry(context, step):
                compensation_failed = True

        context.status = SagaStatus.FAILED if compensation_failed else SagaStatus.COMPENSATED
        context.touch()
        self._state_store.save(context)
        logger.info("saga_id=%s name=%s %s", context.saga_id, context.saga_name, context.status.value)
        return context

    def _compensate_step_with_retry(self, context: SagaContext, step: SagaStep) -> bool:
        context.step_states[step.name] = StepStatus.COMPENSATING
        self._state_store.save(context)

        attempt = 0
        last_error: Optional[Exception] = None
        while attempt <= self._max_retries:
            try:
                logger.info(
                    "saga_id=%s step='%s' compensating (attempt %d/%d)",
                    context.saga_id,
                    step.name,
                    attempt + 1,
                    self._max_retries + 1,
                )
                step.compensation(context)
                context.step_states[step.name] = StepStatus.COMPENSATED
                context.touch()
                self._state_store.save(context)
                return True
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                attempt += 1
                if attempt <= self._max_retries:
                    backoff_seconds = (self._backoff_ms / 1000) * attempt
                    logger.warning(
                        "saga_id=%s step='%s' compensation attempt %d failed: %s (retrying in %.2fs)",
                        context.saga_id,
                        step.name,
                        attempt,
                        exc,
                        backoff_seconds,
                    )
                    time.sleep(backoff_seconds)

        context.step_states[step.name] = StepStatus.COMPENSATION_FAILED
        context.error = f"compensation for step '{step.name}' failed after {attempt} attempt(s): {last_error}"
        context.touch()
        self._state_store.save(context)

        self._broker.publish_dead_letter(
            self._dlq_topic,
            {
                "saga_id": context.saga_id,
                "saga_name": context.saga_name,
                "step": step.name,
                "error": str(last_error),
                "data": context.data,
                "attempts": attempt,
            },
        )
        return False
