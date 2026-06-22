from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class OrderStatus(str, Enum):
    PENDENTE = "PENDENTE"


class OrderCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    nome_cliente: str = Field(min_length=1, max_length=150, examples=["Maria Silva"])
    nome_produto: str = Field(min_length=1, max_length=200, examples=["Notebook"])
    quantidade: int = Field(gt=0, examples=[2])


class Order(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    nome_cliente: str
    nome_produto: str
    quantidade: int
    status: OrderStatus = OrderStatus.PENDENTE
    criado_em: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class OrderResponse(Order):
    pass
