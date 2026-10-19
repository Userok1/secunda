import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from faststream.rabbit import RabbitBroker

import app.models
from app.api.v1.payments_router import router
from app.config import settings
from app.db import Base, engine
from app.outbox.relay import relay_loop

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("payments.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if settings.AUTO_CREATE_TABLES:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database schema ensured (AUTO_CREATE_TABLES=true)")

    broker = RabbitBroker(settings.RABBITMQ_URL)
    await broker.start()
    app.state.broker = broker

    stop_event = asyncio.Event()
    relay_task = asyncio.create_task(relay_loop(broker, stop_event))

    try:
        yield
    finally:
        stop_event.set()
        await relay_task
        await broker.stop()
        await engine.dispose()


app = FastAPI(
    title="Payment Processing Service",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
