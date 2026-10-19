import asyncio
import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class WebhookDeliveryError(RuntimeError):
    """Raised when a webhook could not be delivered after all retries."""


async def send_webhook(
    url: str,
    payload: dict,
    *,
    max_attempts: int | None = None,
    base_delay: float | None = None,
    timeout: float | None = None,
) -> None:
    """POST ``payload`` to ``url`` retrying with exponential backoff.

    Raises :class:`WebhookDeliveryError` when every attempt fails.
    """
    max_attempts = max_attempts or settings.WEBHOOK_MAX_ATTEMPTS
    base_delay = base_delay or settings.WEBHOOK_RETRY_BASE_DELAY
    timeout = timeout or settings.WEBHOOK_TIMEOUT

    delay = base_delay
    last_error: Exception | None = None

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                logger.info(
                    "Webhook delivered to %s (attempt %s)", url, attempt
                )
                return
            except Exception as exc: # Retry на любую ошибку
                last_error = exc
                logger.warning(
                    "Webhook attempt %s/%s to %s failed: %s",
                    attempt,
                    max_attempts,
                    url,
                    exc,
                )
                if attempt == max_attempts:
                    break
                await asyncio.sleep(delay)
                delay *= 2

    raise WebhookDeliveryError(
        f"Webhook to {url} failed after {max_attempts} attempts"
    ) from last_error
