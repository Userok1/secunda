from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

from app.config import settings

PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_NEW_QUEUE_NAME = "payments.new"

DLX_EXCHANGE_NAME = "payments.dlx"
DLQ_ROUTING_KEY = "payments.new.dead"
DLQ_QUEUE_NAME = "payments.new.dlq"


def get_main_queue_arguments() -> dict:
    return {
        "x-dead-letter-exchange": DLX_EXCHANGE_NAME,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    }


def get_retry_queue_name(attempt: int) -> str:
    return f"{PAYMENTS_NEW_QUEUE_NAME}.retry.{attempt}"


def get_retry_queue_arguments(attempt: int) -> dict:
    delay_ms = int(
        settings.RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)) * 1000
    )
    return {
        "x-message-ttl": delay_ms,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
        "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
    }


PAYMENTS_EXCHANGE = RabbitExchange(
    PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True
)
DLX_EXCHANGE = RabbitExchange(
    DLX_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True
)

# Main work queue. Messages rejected without requeue are routed to the
# dead-letter exchange and parked in the DLQ.
PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW_QUEUE_NAME,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    durable=True,
    arguments=get_main_queue_arguments(),
)


def get_retry_queue(attempt: int) -> RabbitQueue:
    """Per-attempt delay queue.

    Messages sit here for an exponentially growing TTL and are then dead-lettered
    back to the main queue, implementing exponential backoff between attempts.
    """
    return RabbitQueue(
        get_retry_queue_name(attempt),
        durable=True,
        arguments=get_retry_queue_arguments(attempt),
    )


def get_retry_delay_seconds(attempt: int) -> float:
    return settings.RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
