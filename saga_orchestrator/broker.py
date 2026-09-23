from __future__ import annotations

import json
import logging
import threading
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class MessageBroker(ABC):
    """Publishes saga events. Used mainly to route failed compensations to a
    dead-letter queue for manual/asynchronous inspection.
    """

    @abstractmethod
    def publish(self, topic: str, message: dict[str, Any]) -> None: ...

    def publish_dead_letter(self, dlq_topic: str, message: dict[str, Any]) -> None:
        logger.error("Publishing to dead-letter queue [%s]: %s", dlq_topic, message)
        self.publish(dlq_topic, message)


class InMemoryBroker(MessageBroker):
    """Volatile broker keeping published messages in memory per topic.

    Useful for local demos and development when no Kafka/RabbitMQ cluster is
    available. Nothing is delivered across process boundaries.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.topics: dict[str, list[dict[str, Any]]] = {}

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        with self._lock:
            self.topics.setdefault(topic, []).append(message)
        logger.info("[memory-broker] -> %s: %s", topic, message)


class KafkaBroker(MessageBroker):
    """Broker backed by Kafka via `kafka-python`.

    Requires `BROKER_CONNECTION_URL` to point at a `bootstrap.servers` list,
    e.g. `localhost:9092` or `broker1:9092,broker2:9092`.
    """

    def __init__(self, bootstrap_servers: str) -> None:
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        future = self._producer.send(topic, message)
        future.get(timeout=10)
        logger.info("[kafka-broker] -> %s: %s", topic, message)


class RabbitMQBroker(MessageBroker):
    """Broker backed by RabbitMQ via `pika`.

    `connection_url` must be a valid AMQP URI, e.g.
    `amqp://guest:guest@localhost:5672/%2F`. Each topic is published to a
    durable direct exchange-less queue of the same name.
    """

    def __init__(self, connection_url: str) -> None:
        import pika

        self._pika = pika
        params = pika.URLParameters(connection_url)
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        self._channel.queue_declare(queue=topic, durable=True)
        self._channel.basic_publish(
            exchange="",
            routing_key=topic,
            body=json.dumps(message).encode("utf-8"),
            properties=self._pika.BasicProperties(delivery_mode=2),
        )
        logger.info("[rabbitmq-broker] -> %s: %s", topic, message)


def build_broker(broker_type: str, connection_url: str) -> MessageBroker:
    if broker_type == "kafka":
        logger.info("Using KafkaBroker (%s)", connection_url)
        return KafkaBroker(connection_url)
    if broker_type == "rabbitmq":
        logger.info("Using RabbitMQBroker (%s)", connection_url)
        return RabbitMQBroker(connection_url)
    if broker_type == "memory":
        logger.info("Using InMemoryBroker (non-durable, for local demo use)")
        return InMemoryBroker()
    raise ValueError(f"Unknown BROKER_TYPE: {broker_type!r}. Expected 'kafka', 'rabbitmq', or 'memory'.")
