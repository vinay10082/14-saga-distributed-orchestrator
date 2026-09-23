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
* `BROKER_CONNECTION_URL`
* `SAGA_TIMEOUT_MS`
* `MAX_COMPENSATION_RETRIES`

## Quick Start & Usage
Register forward actions and their corresponding idempotent compensating actions within the orchestrator to define the distributed workflow.

## Testing & CI
Simulates chaotic network partitions, forces mid-transaction service crashes, and validates that the orchestrator successfully recovers and executes all compensations.
