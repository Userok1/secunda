from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.models.payment import Currency


class PaymentCreatedEvent(BaseModel):
    """Event published to RabbitMQ when a payment is created.

    Serialized into the outbox ``payload`` and consumed by the worker.
    """

    model_config = ConfigDict(extra="ignore")

    payment_id: UUID
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str | None = None
    webhook_url: str
