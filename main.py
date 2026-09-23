#!/usr/bin/env python3
"""Entry point for the Saga Distributed Orchestrator.

Usage:
    python main.py run [--fail-at STEP]   Run one order saga instance.
    python main.py recover                Resume any sagas left in-flight by a
                                           previous crash (reads persisted state).
    python main.py status SAGA_ID         Print the persisted state of a saga.

Configuration is read from environment variables / `.env` (see saga_orchestrator/config.py).
"""

from __future__ import annotations

import argparse
import logging
import sys

from saga_orchestrator.broker import build_broker
from saga_orchestrator.config import config
from saga_orchestrator.orchestrator import SagaOrchestrator
from saga_orchestrator.state_store import build_state_store
from sagas import build_order_saga


def configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def build_orchestrator() -> SagaOrchestrator:
    state_store = build_state_store(config.state_store_type, config.database_url)
    broker = build_broker(config.broker_type, config.broker_connection_url)
    orchestrator = SagaOrchestrator(
        state_store=state_store,
        broker=broker,
        dlq_topic=config.dlq_topic,
        max_compensation_retries=config.max_compensation_retries,
        step_retry_backoff_ms=config.step_retry_backoff_ms,
    )
    orchestrator.register(build_order_saga())
    return orchestrator


def cmd_run(args: argparse.Namespace) -> int:
    orchestrator = build_orchestrator()
    order_payload = {
        "customer_id": "cust-1001",
        "sku": "WIDGET-42",
        "quantity": 2,
        "amount": 59.98,
        "address": "123 Main St, Springfield",
    }
    if args.fail_at:
        order_payload["simulate_failure"] = args.fail_at

    context = orchestrator.run("order_processing", initial_data=order_payload)

    print(f"\nsaga_id   : {context.saga_id}")
    print(f"status    : {context.status.value}")
    print(f"error     : {context.error}")
    print("step states:")
    for step_name, step_status in context.step_states.items():
        print(f"  - {step_name}: {step_status.value}")

    return 0 if context.status.value in ("COMPLETED",) else 1


def cmd_recover(_: argparse.Namespace) -> int:
    orchestrator = build_orchestrator()
    recovered = orchestrator.recover()
    if not recovered:
        print("No in-flight sagas found to recover.")
        return 0
    for context in recovered:
        print(f"saga_id={context.saga_id} name={context.saga_name} -> {context.status.value}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    state_store = build_state_store(config.state_store_type, config.database_url)
    context = state_store.load(args.saga_id)
    if context is None:
        print(f"No saga found with id {args.saga_id}")
        return 1
    print(f"saga_id   : {context.saga_id}")
    print(f"saga_name : {context.saga_name}")
    print(f"status    : {context.status.value}")
    print(f"error     : {context.error}")
    print(f"data      : {context.data}")
    print("step states:")
    for step_name, step_status in context.step_states.items():
        print(f"  - {step_name}: {step_status.value}")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Saga Distributed Orchestrator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a new order saga instance")
    run_parser.add_argument(
        "--fail-at",
        choices=["reserve_inventory", "charge_payment", "ship_order"],
        default=None,
        help="Force the named step to fail, to demonstrate compensation.",
    )
    run_parser.set_defaults(func=cmd_run)

    recover_parser = subparsers.add_parser("recover", help="Resume in-flight sagas after a crash")
    recover_parser.set_defaults(func=cmd_recover)

    status_parser = subparsers.add_parser("status", help="Show the persisted state of a saga")
    status_parser.add_argument("saga_id")
    status_parser.set_defaults(func=cmd_status)

    return parser


def main() -> int:
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
