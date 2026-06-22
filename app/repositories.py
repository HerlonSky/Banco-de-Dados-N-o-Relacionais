from typing import Protocol

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from app.models import Order


class OrderRepository(Protocol):
    def create(self, order: Order) -> Order: ...

    def list_all(self) -> list[Order]: ...


class MongoOrderRepository:
    def __init__(self, client: MongoClient, database: str, collection: str) -> None:
        self._collection: Collection = client[database][collection]
        self._collection.create_index([("criado_em", ASCENDING)])

    def create(self, order: Order) -> Order:
        document = order.model_dump(mode="python")
        document["_id"] = document.pop("id")
        self._collection.insert_one(document)
        return order

    def list_all(self) -> list[Order]:
        orders: list[Order] = []
        for document in self._collection.find().sort("criado_em", ASCENDING):
            document["id"] = str(document.pop("_id"))
            orders.append(Order.model_validate(document))
        return orders
