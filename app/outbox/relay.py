import asyncio
import logging
from datetime import datetime, timezone

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from app.config import settings
from app.db import SessionFactory
from app.models.payment import OutboxMessage
from app.rabbit import PAYMENTS_EXCHANGE

logger = logging.getLogger(__name__)

_BATCH_SIZE = settings.OUTBOX_BATCH_SIZE


async def publish_pending_messages(broker: RabbitBroker) -> int:
    """Publish a batch of unpublished outbox rows.

    Rows are locked with ``FOR UPDATE SKIP LOCKED`` so that concurrent relay
    instances never publish the same message. ``published_at`` is committed
    only after RabbitMQ confirms the publish; if the process dies in between,
    the row is retried on the next tick (at-least-once delivery).
    """
    async with SessionFactory() as session:
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at)
            .limit(_BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        messages = list((await session.scalars(stmt)).all())
        if not messages:
            return 0

        for message in messages:
            await broker.publish(
                message.payload,
                exchange=PAYMENTS_EXCHANGE,
                routing_key=message.event_type,
                message_id=str(message.id),
                headers={
                    "x-attempt": 1,
                    "x-outbox-id": str(message.id),
                },
                persist=True,
            )
            message.published_at = datetime.now(timezone.utc)
            message.attempts += 1

        await session.commit()
        logger.info("Outbox relay published %s message(s)", len(messages))
        return len(messages)


async def relay_loop(broker: RabbitBroker, stop_event: asyncio.Event) -> None:
    """Background task: poll the outbox and publish new events forever."""
    logger.info("Outbox relay started (interval=%ss)", settings.OUTBOX_POLL_INTERVAL)
    while not stop_event.is_set():
        try:
            published = await publish_pending_messages(broker)
        except Exception:
            logger.exception("Outbox relay iteration failed")
            published = 0

        if published == 0:
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=settings.OUTBOX_POLL_INTERVAL
                )
            except asyncio.TimeoutError:
                pass

    logger.info("Outbox relay stopped")
