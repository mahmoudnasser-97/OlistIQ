-- OlistIQ Data Warehouse Schema Initialization
-- Runs automatically when postgres-dw container starts for the first time

CREATE SCHEMA IF NOT EXISTS delivery_performance;
CREATE SCHEMA IF NOT EXISTS customer_churn;

-- ── DELIVERY PERFORMANCE ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS delivery_performance.dim_date (
    date_sk        SERIAL PRIMARY KEY,
    full_date      DATE NOT NULL,
    day_number     INT,
    day_name       VARCHAR(20),
    week_number    INT,
    month_number   INT,
    month_name     VARCHAR(20),
    quarter_number INT,
    year_number    INT,
    is_weekend     BOOLEAN,
    is_month_start BOOLEAN,
    is_month_end   BOOLEAN
);

CREATE TABLE IF NOT EXISTS delivery_performance.dim_customer (
    customer_sk          SERIAL PRIMARY KEY,
    customer_id          VARCHAR(50),
    customer_unique_id   VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city        VARCHAR(100),
    customer_state       VARCHAR(10),
    customer_region      VARCHAR(100),
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    created_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS delivery_performance.dim_seller (
    seller_sk            SERIAL PRIMARY KEY,
    seller_id            VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city          VARCHAR(100),
    seller_state         VARCHAR(10),
    seller_region        VARCHAR(100),
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    created_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS delivery_performance.dim_product (
    product_sk                  SERIAL PRIMARY KEY,
    product_id                  VARCHAR(50),
    product_category_name       VARCHAR(100),
    product_category_english    VARCHAR(100),
    product_weight_g            DOUBLE PRECISION,
    product_length_cm           DOUBLE PRECISION,
    product_height_cm           DOUBLE PRECISION,
    product_width_cm            DOUBLE PRECISION,
    product_volume_cm3          DOUBLE PRECISION,
    product_photos_qty          INT,
    product_name_length         INT,
    product_description_length  INT,
    logistics_size_category     VARCHAR(20),
    logistics_weight_category   VARCHAR(20),
    created_at                  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS delivery_performance.fct_order_delivery (
    order_id                    VARCHAR(50),
    customer_sk_fk              INT REFERENCES delivery_performance.dim_customer(customer_sk),
    seller_sk_fk                INT REFERENCES delivery_performance.dim_seller(seller_sk),
    purchase_date_sk            INT REFERENCES delivery_performance.dim_date(date_sk),
    estimated_delivery_date_sk  INT REFERENCES delivery_performance.dim_date(date_sk),
    actual_delivery_date_sk     INT REFERENCES delivery_performance.dim_date(date_sk),
    order_status                VARCHAR(30),
    freight_total_value         DOUBLE PRECISION,
    total_items_count           INT,
    seller_count                INT,
    is_multi_seller_order       BOOLEAN,
    delivery_duration_days      INT,
    delay_days                  INT,
    buffer_days                 INT,
    delivery_status             VARCHAR(20),
    on_time_flag                BOOLEAN,
    distance_km                 DOUBLE PRECISION,
    distance_bucket             VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS delivery_performance.fct_seller_fulfillment (
    order_id                        VARCHAR(50),
    order_item_id                   INT,
    seller_sk_fk                    INT REFERENCES delivery_performance.dim_seller(seller_sk),
    product_sk_fk                   INT REFERENCES delivery_performance.dim_product(product_sk),
    customer_sk_fk                  INT REFERENCES delivery_performance.dim_customer(customer_sk),
    purchase_date_sk                INT REFERENCES delivery_performance.dim_date(date_sk),
    shipping_limit_date_sk          INT REFERENCES delivery_performance.dim_date(date_sk),
    freight_value                   DOUBLE PRECISION,
    item_price                      DOUBLE PRECISION,
    seller_preparation_days         INT,
    shipping_deadline_gap_days      INT,
    freight_ratio                   DOUBLE PRECISION,
    heavy_product_flag              BOOLEAN,
    oversized_product_flag          BOOLEAN,
    seller_to_customer_distance_km  DOUBLE PRECISION
);

-- ── CUSTOMER CHURN ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customer_churn.dim_date (
    date_sk        SERIAL PRIMARY KEY,
    full_date      DATE NOT NULL,
    day_number     INT,
    day_name       VARCHAR(20),
    week_number    INT,
    month_number   INT,
    month_name     VARCHAR(20),
    quarter_number INT,
    year_number    INT,
    is_weekend     BOOLEAN,
    is_month_start BOOLEAN,
    is_month_end   BOOLEAN
);

CREATE TABLE IF NOT EXISTS customer_churn.dim_product (
    product_sk                  SERIAL PRIMARY KEY,
    product_id                  VARCHAR(50),
    product_category_name       VARCHAR(100),
    product_category_english    VARCHAR(100),
    product_weight_g            DOUBLE PRECISION,
    product_length_cm           DOUBLE PRECISION,
    product_height_cm           DOUBLE PRECISION,
    product_width_cm            DOUBLE PRECISION,
    product_volume_cm3          DOUBLE PRECISION,
    product_photos_qty          INT,
    product_name_length         INT,
    product_description_length  INT,
    logistics_size_category     VARCHAR(20),
    logistics_weight_category   VARCHAR(20),
    created_at                  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_churn.dim_customer_profile (
    customer_profile_sk      SERIAL PRIMARY KEY,
    customer_unique_id       VARCHAR(50),
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(10),
    customer_region          VARCHAR(100),
    latitude                 DOUBLE PRECISION,
    longitude                DOUBLE PRECISION,
    first_order_date         TIMESTAMP,
    last_order_date          TIMESTAMP,
    total_orders             INT,
    total_spend_egp          DOUBLE PRECISION,
    avg_order_value_egp      DOUBLE PRECISION,
    days_since_first_order   INT,
    days_since_last_order    INT,
    customer_segment         VARCHAR(20),
    churn_flag               BOOLEAN,
    created_at               TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_churn.fct_customer_orders (
    order_id                    VARCHAR(50),
    customer_profile_sk_fk      INT REFERENCES customer_churn.dim_customer_profile(customer_profile_sk),
    order_date_sk               INT REFERENCES customer_churn.dim_date(date_sk),
    order_purchase_timestamp    TIMESTAMP,
    order_status                VARCHAR(30),
    payment_value_egp           DOUBLE PRECISION,
    items_count                 INT,
    distinct_categories         INT,
    review_score                DOUBLE PRECISION,
    delivery_status             VARCHAR(20),
    order_sequence_number       INT,
    days_since_previous_order   INT
);

CREATE TABLE IF NOT EXISTS customer_churn.fct_churn_summary (
    customer_profile_sk_fk      INT REFERENCES customer_churn.dim_customer_profile(customer_profile_sk),
    first_order_date_sk         INT REFERENCES customer_churn.dim_date(date_sk),
    last_order_date_sk          INT REFERENCES customer_churn.dim_date(date_sk),
    customer_segment            VARCHAR(20),
    churn_flag                  BOOLEAN,
    total_orders                INT,
    total_spend_egp             DOUBLE PRECISION,
    avg_order_value_egp         DOUBLE PRECISION,
    avg_review_score            DOUBLE PRECISION,
    avg_days_between_orders     DOUBLE PRECISION,
    days_since_last_order       INT,
    top_category                VARCHAR(100)
);
