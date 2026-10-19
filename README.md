# Payment Processing Service

Асинхронный микросервис процессинга платежей: принимает запросы на оплату,
обрабатывает их через эмулируемый платёжный шлюз и уведомляет клиента через
webhook.

## Стек

- **FastAPI** + **Pydantic v2** — HTTP API
- **SQLAlchemy 2.0** (async) + **PostgreSQL** — хранилище
- **Alembic** — миграции схемы БД
- **RabbitMQ** + **FastStream** — очередь сообщений
- **Outbox pattern** — гарантированная публикация событий
- **Docker / docker-compose** — окружение

### Поток обработки

1. `POST /api/v1/payments` создаёт `payment` (status `pending`) и запись в
   `outbox` **в одной транзакции**.
2. Фоновая задача **outbox relay** (в процессе API) выбирает неопубликованные
   строки `outbox` через `SELECT ... FOR UPDATE SKIP LOCKED`, публикует событие
   `payments.new` в RabbitMQ и проставляет `published_at`. Если процесс упадёт
   после публикации, но до коммита — сообщение будет опубликовано повторно
   (at-least-once).
3. **Consumer** получает сообщение, эмулирует обработку шлюза (2–5 сек, 90%
   успех / 10% ошибка), обновляет статус в БД и отправляет webhook.
4. При ошибке сообщение переотправляется в **retry-очередь** с экспоненциальной
   задержкой (2s, 4s). После 3 неудачных попыток сообщение уходит в
   **Dead Letter Queue** (`payments.new.dlq`).

## Запуск

```bash
cp .env.example .env            # опционально: значения заданы в compose
docker compose up --build
```

Сервисы:

| Сервис            | URL / порт                              |
|-------------------|-----------------------------------------|
| API               | http://localhost:8000                   |
| Swagger UI        | http://localhost:8000/docs              |
| RabbitMQ UI       | http://localhost:15672 (guest / guest)  |
| PostgreSQL        | localhost:5433 (postgres / postgres)    |

### Миграции (Alembic)

Схема БД управляется через Alembic: таблицы `payments` и `outbox` создаются
миграцией `alembic/versions/0001_initial.py` (включая PostgreSQL-enum-типы
`currency` и `payment_status`).

При `docker compose up` отдельный сервис `migrate` сам применяет миграции до
старта API и consumer (стратегия `alembic upgrade head`), поэтому ручных действий
не требуется.

```bash
# применить все миграции
alembic upgrade head

# откатить последнюю / все миграции
alembic downgrade -1
alembic downgrade base

# текущая ревизия / история
alembic current
alembic history --verbose

# создать новую автогенерируемую миграцию по изменениям моделей
alembic revision --autogenerate -m "add something"

# применить миграции внутри работающего окружения
docker compose run --rm migrate alembic upgrade head
```

Для `alembic` из контейнера `migrate` используется `Dockerfile.api`
(`alembic.ini` и каталог `alembic/` копируются в образ).

> Флаг `AUTO_CREATE_TABLES` (`app/config.py`) остаётся как dev-фолбэк: при
> `true` API создаёт таблицы через `Base.metadata.create_all` на старте. По
> умолчанию `false` — источник истины это Alembic.

> Если БД уже содержит таблицы `payments`/`outbox` (например, созданные ранее
> через `AUTO_CREATE_TABLES`) и нужно перейти на Alembic без потери данных —
> один раз пометьте текущую ревизию: `alembic stamp head`.

### Создание платежа

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: 8e0c2f0c-2b1a-4f6e-9a2c-1234567890ab" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа #42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://webhook.site/<your-unique-id>"
      }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "2f7a6c2e-...-9b1e",
  "status": "pending",
  "created_at": "2026-01-01T12:00:00Z"
}
```

Повторный запрос с тем же `Idempotency-Key` вернёт тот же `payment_id`.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/2f7a6c2e-...-9b1e \
  -H "X-API-Key: secret-api-key"
```

```json
{
  "id": "2f7a6c2e-...-9b1e",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Оплата заказа #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "8e0c2f0c-...",
  "webhook_url": "https://webhook.site/...",
  "created_at": "2026-01-01T12:00:00Z",
  "processed_at": "2026-01-01T12:00:04Z"
}
```

### Webhook

После обработки на `webhook_url` отправляется POST:

```json
{
  "payment_id": "2f7a6c2e-...-9b1e",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "idempotency_key": "8e0c2f0c-...",
  "processed_at": "2026-01-01T12:00:04Z"
}
```

Доставка повторяется до 3 раз с экспоненциальной задержкой.

Проверить очереди: http://localhost:15672 (Логин: guest, Пароль: guest) → Queues.

## Проверка сценариев

- **Успешная обработка**: создайте платёж, через ~2–5 сек `GET` покажет
  `succeeded`/`failed`, webhook доставлен.
- **Идемпотентность**: повторите POST с тем же `Idempotency-Key` — вернётся тот
  же `payment_id`, в БД одна запись.
- **DLQ**: укажите недоступный `webhook_url` (например `http://10.255.255.1/x`) —
  после 3 попыток сообщение появится в `payments.new.dlq`.
