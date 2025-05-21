CREATE TABLE IF NOT EXISTS dim_customer
(
    dim_customer_id      SERIAL PRIMARY KEY,
    source_customer_id   INT,
    customer_first_name  VARCHAR(100),
    customer_last_name   VARCHAR(100),
    customer_age         INT,
    customer_email       VARCHAR(255),
    customer_country     VARCHAR(100),
    customer_postal_code VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS dim_pet
(
    pet_id             SERIAL PRIMARY KEY,
    source_customer_id INT,
    customer_pet_type  VARCHAR(100),
    customer_pet_name  VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    pet_category       VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS dim_seller
(
    dim_seller_id      SERIAL PRIMARY KEY,
    source_seller_id   INT,
    seller_first_name  VARCHAR(100),
    seller_last_name   VARCHAR(100),
    seller_email       VARCHAR(255),
    seller_country     VARCHAR(100),
    seller_postal_code VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS dim_product
(
    dim_product_id       SERIAL PRIMARY KEY,
    source_product_id    INT,
    product_name         VARCHAR(255),
    product_category     VARCHAR(100),
    product_price        NUMERIC(10, 2),
    product_weight       NUMERIC(10, 2),
    product_color        VARCHAR(50),
    product_size         VARCHAR(50),
    product_brand        VARCHAR(100),
    product_material     VARCHAR(100),
    product_description  TEXT,
    product_rating       NUMERIC(3, 2),
    product_reviews      INT,
    product_release_date DATE,
    product_expiry_date  DATE
);
CREATE TABLE IF NOT EXISTS dim_store
(
    dim_store_id   SERIAL PRIMARY KEY,
    store_name     VARCHAR(100),
    store_location VARCHAR(200),
    store_city     VARCHAR(100),
    store_state    VARCHAR(100),
    store_country  VARCHAR(100),
    store_phone    VARCHAR(50),
    store_email    VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dim_supplier
(
    dim_supplier_id  SERIAL PRIMARY KEY,
    supplier_name    VARCHAR(255),
    supplier_contact VARCHAR(100),
    supplier_email   VARCHAR(255),
    supplier_phone   VARCHAR(50),
    supplier_address VARCHAR(200),
    supplier_city    VARCHAR(100),
    supplier_country VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS fact_sales
(
    sale_id          SERIAL PRIMARY KEY,
    sale_date        DATE,
    customer_id      INT,
    pet_id           INT,
    seller_id        INT,
    product_id       INT,
    store_id         INT,
    supplier_id      INT,
    sale_quantity    INT,
    sale_total_price NUMERIC(10, 2),
    FOREIGN KEY (customer_id) REFERENCES dim_customer (dim_customer_id),
    FOREIGN KEY (pet_id) REFERENCES dim_pet (pet_id),
    FOREIGN KEY (seller_id) REFERENCES dim_seller (dim_seller_id),
    FOREIGN KEY (product_id) REFERENCES dim_product (dim_product_id),
    FOREIGN KEY (store_id) REFERENCES dim_store (dim_store_id),
    FOREIGN KEY (supplier_id) REFERENCES dim_supplier (dim_supplier_id)
);
