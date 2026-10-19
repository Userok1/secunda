import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment, PaymentStatus, OutboxMessage
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payment import PaymentCreate

PAYMENT_CREATED_EVENT = "payments.new"


def build_payment_event(payment: Payment) -> dict:
    return PaymentCreatedEvent(
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        webhook_url=payment.webhook_url,
    ).model_dump(mode="json")


async def get_payment_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> Payment | None:
    stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
    return await session.scalar(stmt)


async def get_payment(
    session: AsyncSession, payment_id: uuid.UUID
) -> Payment | None:
    return await session.get(Payment, payment_id)


async def create_payment(
    session: AsyncSession,
    data: PaymentCreate,
    idempotency_key: str,
) -> tuple[Payment, bool]:
    """Create a payment and its outbox event within a single transaction.

    Returns ``(payment, created)`` where ``created`` is ``False`` when an
    existing payment with the same idempotency key was returned.
    """
    existing = await get_payment_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing, False

    payment = Payment(
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        meta=data.metadata,
        status=PaymentStatus.pending,
        idempotency_key=idempotency_key,
        webhook_url=str(data.webhook_url),
    )
    session.add(payment)
    await session.flush()

    # TODO: Разобраться с тем, что такое aggregate
    session.add(
        OutboxMessage(
            aggregate_type="payment",
            aggregate_id=payment.id,
            event_type=PAYMENT_CREATED_EVENT,
            payload=build_payment_event(payment),
        )
    )

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await get_payment_by_idempotency_key(session, idempotency_key)
        if existing is None:
            raise
        return existing, False

    await session.refresh(payment)
    return payment, True
