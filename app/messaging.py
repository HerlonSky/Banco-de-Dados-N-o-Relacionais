import json
from typing import Protocol

import pika
from kafka import KafkaProducer

from app.events import OrderCreatedEvent


class OrderPublisher(Protocol):
    def publish(self, event: OrderCreatedEvent) -> None: ...

    def close(self) -> None: ...


class RabbitMQOrderPublisher:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        queue: str,
    ) -> None:
        self._parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(user, password),
            heartbeat=30,
            blocked_connection_timeout=30,
        )
        self._queue = queue

    def publish(self, event: OrderCreatedEvent) -> None:
        connection = pika.BlockingConnection(self._parameters)
        try:
            channel = connection.channel()
            channel.queue_declare(queue=self._queue, durable=True)
            channel.basic_publish(
                exchange="",
                routing_key=self._queue,
                body=json.dumps(event.rabbit_payload()).encode("utf-8"),
                properties=pika.BasicProperties(
                    message_id=event.cabecalho.evento_id,
                    content_type="application/json",
                    delivery_mode=2,
                    type=event.cabecalho.tipo,
                    app_id=event.cabecalho.origem,
                ),
            )
        finally:
            connection.close()

    def close(self) -> None:
        # Cada publicação utiliza uma conexão curta, portanto não há estado aberto.
        return None


class KafkaOrderPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8"),
            acks="all",
            retries=5,
        )

    def publish(self, event: OrderCreatedEvent) -> None:
        future = self._producer.send(
            self._topic,
            key=event.pedido.id,
            value=event.kafka_payload(),
            headers=[
                ("evento_id", event.cabecalho.evento_id.encode("utf-8")),
                ("versao", event.cabecalho.versao.encode("utf-8")),
            ],
        )
        future.get(timeout=10)

    def close(self) -> None:
        self._producer.flush(timeout=10)
        self._producer.close(timeout=10)
