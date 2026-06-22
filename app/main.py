import logging
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import uuid4

from anyio import to_thread
from fastapi import Depends, FastAPI, HTTPException, Request, status
from pymongo import MongoClient

from app.config import Settings, get_settings
from app.events import OrderCreatedEvent
from app.identity import PROJECT_IDENTITY
from app.messaging import (
    KafkaOrderPublisher,
    OrderPublisher,
    RabbitMQOrderPublisher,
)
from app.models import Order, OrderCreate, OrderResponse
from app.repositories import MongoOrderRepository, OrderRepository

logger = logging.getLogger(__name__)


def create_app(
    repository: OrderRepository | None = None,
    rabbit_publisher: OrderPublisher | None = None,
    kafka_publisher: OrderPublisher | None = None,
    settings: Settings | None = None,
) -> FastAPI:
    app_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        mongo_client: MongoClient | None = None
        if repository is None:
            mongo_client = MongoClient(
                app_settings.mongodb_url,
                serverSelectionTimeoutMS=5000,
            )
            mongo_client.admin.command("ping")
            app.state.repository = MongoOrderRepository(
                mongo_client,
                app_settings.mongodb_database,
                app_settings.mongodb_collection,
            )
        else:
            app.state.repository = repository

        app.state.rabbit_publisher = rabbit_publisher or RabbitMQOrderPublisher(
            host=app_settings.rabbitmq_host,
            port=app_settings.rabbitmq_port,
            user=app_settings.rabbitmq_user,
            password=app_settings.rabbitmq_password,
            queue=app_settings.rabbitmq_queue,
        )
        app.state.kafka_publisher = kafka_publisher or KafkaOrderPublisher(
            bootstrap_servers=app_settings.kafka_bootstrap_servers,
            topic=app_settings.kafka_topic,
        )

        yield

        app.state.rabbit_publisher.close()
        app.state.kafka_publisher.close()
        if mongo_client is not None:
            mongo_client.close()

    api = FastAPI(
        title=app_settings.app_name,
        description=(
            "Projeto individual de Herlon para cadastro de pedidos com "
            "eventos correlacionados no RabbitMQ e Kafka."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    @api.middleware("http")
    async def add_orderflow_request_id(request: Request, call_next):
        request_id = request.headers.get("X-Correlation-ID", str(uuid4()))
        response = await call_next(request)
        response.headers["X-OrderFlow-Request-ID"] = request_id
        return response

    def get_repository(request: Request) -> OrderRepository:
        return request.app.state.repository

    def get_rabbit_publisher(request: Request) -> OrderPublisher:
        return request.app.state.rabbit_publisher

    def get_kafka_publisher(request: Request) -> OrderPublisher:
        return request.app.state.kafka_publisher

    @api.get("/", tags=["Identidade"])
    def project_identity() -> dict:
        return PROJECT_IDENTITY

    @api.get("/health", tags=["Infraestrutura"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @api.post(
        "/pedidos",
        response_model=OrderResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["Pedidos"],
    )
    async def create_order(
        payload: OrderCreate,
        repo: Annotated[OrderRepository, Depends(get_repository)],
        rabbit: Annotated[OrderPublisher, Depends(get_rabbit_publisher)],
        kafka: Annotated[OrderPublisher, Depends(get_kafka_publisher)],
    ) -> Order:
        order = Order(**payload.model_dump())
        try:
            created = await to_thread.run_sync(repo.create, order)
            event = OrderCreatedEvent.from_order(created)
            # O mesmo evento_id conecta a notificação RabbitMQ ao fato no Kafka.
            await to_thread.run_sync(rabbit.publish, event)
            await to_thread.run_sync(kafka.publish, event)
            return created
        except Exception as exc:
            logger.exception("Falha ao criar ou publicar o pedido %s", order.id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível processar o pedido neste momento.",
            ) from exc

    @api.get(
        "/pedidos",
        response_model=list[OrderResponse],
        tags=["Pedidos"],
    )
    async def list_orders(
        repo: Annotated[OrderRepository, Depends(get_repository)],
    ) -> list[Order]:
        try:
            return await to_thread.run_sync(repo.list_all)
        except Exception as exc:
            logger.exception("Falha ao consultar pedidos")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Não foi possível consultar os pedidos neste momento.",
            ) from exc

    return api


app = create_app()
