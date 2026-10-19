from uuid import UUID
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import verify_api_key
from app.db import get_session
from app.schemas.payment import PaymentCreate, PaymentCreated, PaymentRead
from app.services import payments as payments_service
from app.models.payment import Payment

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentCreated)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: Annotated[str, Header(
        ...,
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Unique key protecting against duplicate payments",
    )],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentCreated:
    payment, _created = await payments_service.create_payment(
        session, data, idempotency_key
    )
    return {
        "payment_id": payment.id,
        "status": payment.status,
        "created_at": payment.created_at,
    }


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentRead:
    payment: Payment | None = await payments_service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )
    return PaymentRead.model_validate(payment)
    # return {
    #     "id": payment.id, 
    #     "amount": payment.amount,
    #     "currency": payment.currency,
    #     "description": payment.description,
    #     "metadata": payment.metadata,
    #     "status": payment.status,
    #     "idempotency_key": payment.idempotency_key,
    #     "webhook_url": payment.webhook_url,
    #     "created_at": payment.created_at,
    #     "processed_at": payment.processed_at,
    # }
