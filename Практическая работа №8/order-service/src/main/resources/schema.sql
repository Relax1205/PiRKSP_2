-- Схема orders_db. Выполняется при каждом старте (spring.sql.init.mode=always),
-- поэтому только CREATE ... IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS orders
(
    id               BIGSERIAL PRIMARY KEY,
    customer_name    VARCHAR(200) NOT NULL,
    delivery_address VARCHAR(500) NOT NULL,
    comment          TEXT,
    status           VARCHAR(32)  NOT NULL,
    status_reason    TEXT,
    total            NUMERIC(12, 2),
    delivery_id      BIGINT,
    courier          VARCHAR(200),
    request_id       VARCHAR(64),
    created_at       TIMESTAMP    NOT NULL,
    updated_at       TIMESTAMP    NOT NULL,
    version          BIGINT       NOT NULL DEFAULT 0
);

-- Позиции заказа. Название, поставщик и цена копируются из Supplier Service при подтверждении:
-- цена в заказе не должна меняться, даже если поставщик потом её изменит.
CREATE TABLE IF NOT EXISTS order_items
(
    id            BIGSERIAL PRIMARY KEY,
    order_id      BIGINT  NOT NULL REFERENCES orders (id),
    product_id    BIGINT  NOT NULL,
    quantity      INT     NOT NULL,
    product_name  VARCHAR(200),
    supplier_name VARCHAR(200),
    price         NUMERIC(12, 2)
);

-- История статусов: из неё SSE-поток отдаёт уже случившиеся события.
CREATE TABLE IF NOT EXISTS order_status_history
(
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT      NOT NULL REFERENCES orders (id),
    status     VARCHAR(32) NOT NULL,
    details    TEXT,
    created_at TIMESTAMP   NOT NULL
);

-- Transactional outbox: событие пишется в одной транзакции с заказом, в Kafka его отправляет OutboxRelay.
CREATE TABLE IF NOT EXISTS outbox
(
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID         NOT NULL UNIQUE,
    event_type  VARCHAR(64)  NOT NULL,
    topic       VARCHAR(128) NOT NULL,
    message_key VARCHAR(64)  NOT NULL,
    payload     TEXT         NOT NULL,
    request_id  VARCHAR(64),
    order_id    BIGINT,
    created_at  TIMESTAMP    NOT NULL,
    sent_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_history_order ON order_status_history (order_id);
CREATE INDEX IF NOT EXISTS idx_outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
