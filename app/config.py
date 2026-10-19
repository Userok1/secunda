from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    API_KEY: str = "secret-api-key"

    DATABASE_URL: str = (
        "postgresql+asyncpg://postgres:postgres@postgres:5432/payments"
    )
    RABBITMQ_URL: str = "amqp://guest:guest@rabbitmq:5672/"

    OUTBOX_POLL_INTERVAL: float = 1.0
    OUTBOX_BATCH_SIZE: int = 50

    PAYMENT_PROCESS_MIN_DELAY: float = 2.0
    PAYMENT_PROCESS_MAX_DELAY: float = 5.0
    PAYMENT_SUCCESS_RATE: float = 0.9

    MESSAGE_MAX_ATTEMPTS: int = 3
    RETRY_BASE_DELAY_SECONDS: float = 1.0

    WEBHOOK_TIMEOUT: float = 10.0
    WEBHOOK_MAX_ATTEMPTS: int = 3
    WEBHOOK_RETRY_BASE_DELAY: float = 2.0

    AUTO_CREATE_TABLES: bool = False
    LOG_LEVEL: str = "INFO"


def get_settings() -> Settings:
    return Settings()


settings = get_settings()
