from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "HerlON Flow"

    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "pedidos_db"
    mongodb_collection: str = "pedidos"

    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "appuser"
    rabbitmq_password: str = "apppassword"
    rabbitmq_queue: str = "pedidos.criados"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "pedidos-criados"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
