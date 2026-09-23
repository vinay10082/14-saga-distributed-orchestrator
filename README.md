# Stage Failure Data Cleanup

## Description
A highly resilient distributed transaction orchestrator implementing the Saga pattern for fault-tolerant microservice data cleanup.

## Architecture Overview
State-persisted Saga orchestrator utilizing strict idempotent compensating transactions, eventual consistency, and dead-letter queues.

## Prerequisites
* Python 3.11+
* Message Broker (Kafka/RabbitMQ)
* Persistent State Store (PostgreSQL)

## Environment Variables
Configuration lives in `.env` (see that file for the full list with defaults). Key variables:
* `BROKER_TYPE` - `memory` (default, no infra needed) | `kafka` | `rabbitmq`
* `BROKER_CONNECTION_URL` - Kafka bootstrap servers or an AMQP URI, depending on `BROKER_TYPE`
* `DLQ_TOPIC` - topic/queue name failed compensations are published to
* `STATE_STORE_TYPE` - `memory` (default, no infra needed) | `postgres`
* `DATABASE_URL` - PostgreSQL connection string, used when `STATE_STORE_TYPE=postgres`
* `SAGA_TIMEOUT_MS`
* `MAX_COMPENSATION_RETRIES`
* `STEP_RETRY_BACKOFF_MS`

## Install

```bash
pip install -r requirements.txt
```

## Quick Start & Usage
`main.py` is the entry point. It registers an example `order_processing` saga
(`sagas/order_saga.py`) with three steps - `reserve_inventory`,
`charge_payment`, `ship_order` - each paired with an idempotent compensating
action, and drives it through the `SagaOrchestrator`.

```bash
# Run a saga instance to completion
python main.py run

# Force a step to fail, and watch the orchestrator compensate completed
# steps in reverse order
python main.py run --fail-at charge_payment

# Resume any sagas left RUNNING/COMPENSATING by a previous crash
# (requires STATE_STORE_TYPE=postgres for state to survive a restart)
python main.py recover

# Inspect the persisted state of a specific saga instance
python main.py status <saga_id>
```

To define your own workflow, build a `Saga` and register forward actions with
their corresponding idempotent compensating actions:

```python
from saga_orchestrator.models import Saga

saga = Saga(name="my_workflow")

@saga.step(name="do_thing", compensation=undo_thing)
def do_thing(ctx):
    ...  # call the microservice; must be safe to retry
    return {"result": "..."}  # merged into ctx.data

orchestrator.register(saga)
orchestrator.run("my_workflow", initial_data={...})
```

By default the orchestrator runs entirely in-process against in-memory
implementations (`BROKER_TYPE=memory`, `STATE_STORE_TYPE=memory`) so it can be
exercised with zero external setup. Switch to `kafka`/`rabbitmq` and
`postgres` in `.env` to run against real infrastructure with durable,
crash-recoverable state.
