CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE dwh.users (
    user_id BIGINT PRIMARY KEY,
    phone TEXT
);

CREATE TABLE dwh.stores (
    store_id BIGINT PRIMARY KEY,
    address TEXT
);

CREATE TABLE dwh.drivers (
    driver_id BIGINT PRIMARY KEY,
    phone TEXT
);

CREATE TABLE dwh.orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT REFERENCES dwh.users(user_id),
    store_id BIGINT REFERENCES dwh.stores(store_id),
    driver_id BIGINT REFERENCES dwh.drivers(driver_id),

    address TEXT,
    created_at TIMESTAMP,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,

    payment_type TEXT,
    delivery_cost NUMERIC,
    order_discount NUMERIC,
    order_cancellation_reason TEXT
);

CREATE TABLE dwh.items (
    item_id BIGINT PRIMARY KEY,
    title TEXT,
    item_category TEXT
);

CREATE TABLE dwh.order_items (
    id SERIAL PRIMARY KEY,
    order_id BIGINT REFERENCES dwh.orders(order_id),
    item_id BIGINT REFERENCES dwh.items(item_id),
    quantity INT,
    price NUMERIC,
    canceled_quantity INT,
    item_replaced_id BIGINT,
    item_discount NUMERIC
);

--- tmp tables ---
CREATE TABLE IF NOT EXISTS dwh.users_tmp (
    user_id BIGINT,
    phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.stores_tmp (
    store_id BIGINT,
    address TEXT
);

CREATE TABLE IF NOT EXISTS dwh.drivers_tmp (
    driver_id BIGINT,
    phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.items_tmp (
    item_id BIGINT,
    title TEXT,
    item_category TEXT
);

CREATE TABLE IF NOT EXISTS dwh.orders_tmp (
    order_id BIGINT,
    user_id BIGINT,
    store_id BIGINT,
    driver_id BIGINT,
    address TEXT,
    created_at TIMESTAMP,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type TEXT,
    delivery_cost NUMERIC,
    order_discount NUMERIC,
    order_cancellation_reason TEXT
);

CREATE TABLE IF NOT EXISTS dwh.order_items_tmp (
    order_id BIGINT,
    item_id BIGINT,
    quantity INT,
    price NUMERIC,
    canceled_quantity INT,
    item_replaced_id BIGINT,
    item_discount NUMERIC
);

--- marts tables (mirrors of dwh) ---

CREATE TABLE IF NOT EXISTS marts.users (
    user_id BIGINT PRIMARY KEY,
    phone TEXT
);

CREATE TABLE IF NOT EXISTS marts.stores (
    store_id BIGINT PRIMARY KEY,
    address TEXT
);

CREATE TABLE IF NOT EXISTS marts.drivers (
    driver_id BIGINT PRIMARY KEY,
    phone TEXT
);

CREATE TABLE IF NOT EXISTS marts.items (
    item_id BIGINT PRIMARY KEY,
    title TEXT,
    item_category TEXT
);

CREATE TABLE IF NOT EXISTS marts.orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    store_id BIGINT,
    driver_id BIGINT,
    address TEXT,
    created_at TIMESTAMP,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type TEXT,
    delivery_cost NUMERIC,
    order_discount NUMERIC,
    order_cancellation_reason TEXT
);

CREATE TABLE IF NOT EXISTS marts.order_items (
    id SERIAL PRIMARY KEY,
    order_id BIGINT,
    item_id BIGINT,
    quantity INT,
    price NUMERIC,
    canceled_quantity INT,
    item_replaced_id BIGINT,
    item_discount NUMERIC
);
