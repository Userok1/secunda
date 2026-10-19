import asyncio
import logging
import random
from datetime import datetime, UTC

import aio_pika
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.config import settings
from app.db import SessionFactory
from app.models.payment import Payment, PaymentStatus
from app.rabbit import (
    DLQ_QUEUE_NAME,
    DLQ_ROUTING_KEY,
    DLX_EXCHANGE_NAME,
    PAYMENTS_EXCHANGE,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_NEW_QUEUE_NAME,
    PAYMENTS_NEW_ROUTING_KEY,
    get_main_queue_arguments,
    get_retry_delay_seconds,
    get_retry_queue,
    get_retry_queue_arguments,
    get_retry_queue_name,
)
from app.schemas.events import PaymentCreatedEvent
from app.services.webhook import send_webhook

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("payments.consumer")

broker = RabbitBroker(settings.RABBITMQ_URL)
app = FastStream(broker)


async def declare_topology() -> None:
    """Declare exchanges, queues and bindings explicitly (idempotent).

    Uses a throwaway aio-pika connection so that every object - including the
    DLQ, which has no consumer - exists and is bound regardless of subscribers.
    """
    connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()

        payments_exchange = await channel.declare_exchange(
            PAYMENTS_EXCHANGE_NAME,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        dlx_exchange = await channel.declare_exchange(
            DLX_EXCHANGE_NAME,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        main_queue = await channel.declare_queue(
            PAYMENTS_NEW_QUEUE_NAME,
            durable=True,
            arguments=get_main_queue_arguments(),
        )
        await main_queue.bind(
            payments_exchange, routing_key=PAYMENTS_NEW_ROUTING_KEY
        )

        # No consumer on the DLQ: exhausted messages stay parked for inspection
        # and manual replay via the RabbitMQ management UI.
        dlq = await channel.declare_queue(DLQ_QUEUE_NAME, durable=True)
        await dlq.bind(dlx_exchange, routing_key=DLQ_ROUTING_KEY)

        for attempt in range(2, settings.MESSAGE_MAX_ATTEMPTS + 1):
            await channel.declare_queue(
                get_retry_queue_name(attempt),
                durable=True,
                arguments=get_retry_queue_arguments(attempt),
            )

    logger.info("RabbitMQ topology declared")


async def emulate_gateway() -> bool:
    """Simulate an external payment gateway: 2-5s latency, 90% success."""
    await asyncio.sleep(
        random.uniform(
            settings.PAYMENT_PROCESS_MIN_DELAY,
            settings.PAYMENT_PROCESS_MAX_DELAY,
        )
    )
    return random.random() < settings.PAYMENT_SUCCESS_RATE


def build_webhook_payload(payment: Payment) -> dict:
    return {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "idempotency_key": payment.idempotency_key,
        "processed_at": payment.processed_at.isoformat()
        if payment.processed_at
        else None,
    }


async def handle_payment(event: PaymentCreatedEvent) -> None:
    """Process a single payment end to end.

    Idempotent: the status transition happens only while the payment is still
    ``pending``, so redeliveries only re-attempt the webhook, never the gateway.
    """
    async with SessionFactory() as session:
        payment = await session.get(
            Payment, event.payment_id, with_for_update=True
        )
        if payment is None:
            logger.error("Payment %s not found, skipping", event.payment_id)
            return

        if payment.status is PaymentStatus.pending:
            succeeded = await emulate_gateway()
            payment.status = (
                PaymentStatus.succeeded if succeeded else PaymentStatus.failed
            )
            payment.processed_at = datetime.now(UTC)
            await session.commit()
            await session.refresh(payment)
            logger.info(
                "Payment %s processed with status=%s",
                payment.id,
                payment.status.value,
            )
        else:
            logger.info(
                "Payment %s already %s, retrying webhook only",
                payment.id,
                payment.status.value,
            )

        payload = build_webhook_payload(payment)

    await send_webhook(payment.webhook_url, payload)


@broker.subscriber(
    queue=PAYMENTS_NEW_QUEUE,
    exchange=PAYMENTS_EXCHANGE,
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def process_payment(
    event: PaymentCreatedEvent, message: RabbitMessage
) -> None:
    attempt = int(message.headers.get("x-attempt", 1) or 1)
    logger.info(
        "Received payment %s (attempt %s/%s)",
        event.payment_id,
        attempt,
        settings.MESSAGE_MAX_ATTEMPTS,
    )

    try:
        await handle_payment(event)
    except Exception as exc:
        if attempt >= settings.MESSAGE_MAX_ATTEMPTS:
            logger.error(
                "Payment %s exhausted %s attempts, parking in %s: %s",
                event.payment_id,
                attempt,
                DLQ_QUEUE_NAME,
                exc,
            )
            raise

        next_attempt = attempt + 1
        logger.warning(
            "Payment %s attempt %s failed (%s); retrying as attempt %s in %ss",
            event.payment_id,
            attempt,
            exc,
            next_attempt,
            get_retry_delay_seconds(next_attempt),
        )
        await broker.publish(
            event,
            queue=get_retry_queue(next_attempt),
            headers={"x-attempt": next_attempt},
            persist=True,
        )


@app.after_startup
async def _on_startup() -> None:
    logger.info(
        "Consumer started: queue=%s exchange=%s max_attempts=%s",
        PAYMENTS_NEW_QUEUE_NAME,
        PAYMENTS_EXCHANGE_NAME,
        settings.MESSAGE_MAX_ATTEMPTS,
    )
    await declare_topology()


if __name__ == "__main__":
    asyncio.run(app.run())
