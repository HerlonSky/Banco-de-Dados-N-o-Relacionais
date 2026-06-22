from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field

from app.identity import EVENT_SCHEMA_VERSION, EVENT_SOURCE
from app.models import Order


class EventHeader(BaseModel):
    """Cabeçalho próprio para rastrear a mesma ocorrência nos dois brokers."""

    evento_id: str = Field(default_factory=lambda: str(uuid4()))
    tipo: str = "pedido.criado"
    versao: str = EVENT_SCHEMA_VERSION
    origem: str = EVENT_SOURCE
    ocorrido_em: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class OrderCreatedEvent(BaseModel):
    cabecalho: EventHeader = Field(default_factory=EventHeader)
    pedido: Order

    @classmethod
    def from_order(cls, order: Order) -> "OrderCreatedEvent":
        return cls(pedido=order)

    def rabbit_payload(self) -> dict:
        """RabbitMQ recebe a notificação enxuta solicitada no enunciado."""
        return {
            "cabecalho": self.cabecalho.model_dump(mode="json"),
            "pedido": {
                "id": self.pedido.id,
                "status": self.pedido.status.value,
            },
        }

    def kafka_payload(self) -> dict:
        """Kafka mantém o fato completo para integrações e histórico."""
        return self.model_dump(mode="json")
