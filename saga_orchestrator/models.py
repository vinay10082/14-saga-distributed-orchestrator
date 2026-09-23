from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from .enums import SagaStatus, StepStatus

# A step action/compensation receives the shared SagaContext and returns a dict
# of values to merge into context.data (or None). Handlers MUST be idempotent:
# they may be invoked more than once for the same saga_id/step during recovery.
StepHandler = Callable[["SagaContext"], Optional[dict]]


@dataclass
class SagaStep:
    name: str
    action: StepHandler
    compensation: Optional[StepHandler] = None
    retryable: bool = True


@dataclass
class Saga:
    name: str
    steps: list[SagaStep] = field(default_factory=list)

    def step(self, name: str, compensation: Optional[StepHandler] = None, retryable: bool = True):
        """Decorator to register a forward action as a saga step."""

        def decorator(func: StepHandler) -> StepHandler:
            self.steps.append(SagaStep(name=name, action=func, compensation=compensation, retryable=retryable))
            return func

        return decorator

    def add_step(self, step: SagaStep) -> None:
        self.steps.append(step)


@dataclass
class SagaContext:
    saga_name: str
    saga_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    data: dict[str, Any] = field(default_factory=dict)
    status: SagaStatus = SagaStatus.PENDING
    step_states: dict[str, StepStatus] = field(default_factory=dict)
    error: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def idempotency_key(self, step_name: str) -> str:
        return f"{self.saga_id}:{step_name}"

    def touch(self) -> None:
        self.updated_at = datetime.now(timezone.utc)
