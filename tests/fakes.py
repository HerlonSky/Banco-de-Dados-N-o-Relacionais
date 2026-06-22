from copy import deepcopy

from app.events import OrderCreatedEvent
from app.models import Order


class InMemoryOrderRepository:
    def __init__(self) -> None:
        self.orders: list[Order] = []

    def create(self, order: Order) -> Order:
        self.orders.append(deepcopy(order))
        return order

    def list_all(self) -> list[Order]:
        return deepcopy(self.orders)


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[OrderCreatedEvent] = []
        self.closed = False

    def publish(self, event: OrderCreatedEvent) -> None:
        self.published.append(deepcopy(event))

    def close(self) -> None:
        self.closed = True
