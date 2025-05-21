INSERT INTO dim_customer (
    source_customer_id,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code
)
SELECT DISTINCT
    sale_customer_id,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code
FROM mock_data;

INSERT INTO dim_pet (
    source_customer_id,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed,
    pet_category
)
SELECT DISTINCT
    sale_customer_id,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed,
    pet_category
FROM mock_data;

INSERT INTO dim_seller (
    source_seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
)
SELECT DISTINCT
    sale_seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data;

INSERT INTO dim_product (
    source_product_id,
    product_name,
    product_category,
    product_price,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
)
SELECT DISTINCT
    sale_product_id,
    product_name,
    product_category,
    product_price,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
FROM mock_data;

INSERT INTO dim_store (
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
)
SELECT DISTINCT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data;

INSERT INTO dim_supplier (
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
)
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data;

INSERT INTO fact_sales (
    sale_date,
    customer_id,
    pet_id,
    seller_id,
    product_id,
    store_id,
    supplier_id,
    sale_quantity,
    sale_total_price
)
SELECT
    md.sale_date,
    (SELECT MIN(dc.dim_customer_id)
       FROM dim_customer dc
      WHERE dc.source_customer_id = md.sale_customer_id) AS customer_id,
    (SELECT MIN(dp.pet_id)
       FROM dim_pet dp
      WHERE dp.source_customer_id = md.sale_customer_id
        AND dp.customer_pet_type = md.customer_pet_type
        AND dp.customer_pet_name = md.customer_pet_name
        AND dp.customer_pet_breed = md.customer_pet_breed
        AND dp.pet_category = md.pet_category) AS pet_id,
    (SELECT MIN(ds.dim_seller_id)
       FROM dim_seller ds
      WHERE ds.source_seller_id = md.sale_seller_id) AS seller_id,
    (SELECT MIN(dp2.dim_product_id)
       FROM dim_product dp2
      WHERE dp2.source_product_id = md.sale_product_id) AS product_id,
    (SELECT MIN(dstore.dim_store_id)
       FROM dim_store dstore
      WHERE dstore.store_name = md.store_name
        AND dstore.store_location = md.store_location
        AND dstore.store_city = md.store_city
        AND dstore.store_state = md.store_state
        AND dstore.store_country = md.store_country
        AND dstore.store_phone = md.store_phone
        AND dstore.store_email = md.store_email) AS store_id,
    (SELECT MIN(dsupp.dim_supplier_id)
       FROM dim_supplier dsupp
      WHERE dsupp.supplier_name = md.supplier_name
        AND dsupp.supplier_contact = md.supplier_contact
        AND dsupp.supplier_email = md.supplier_email
        AND dsupp.supplier_phone = md.supplier_phone
        AND dsupp.supplier_address = md.supplier_address
        AND dsupp.supplier_city = md.supplier_city
        AND dsupp.supplier_country = md.supplier_country) AS supplier_id,
    md.sale_quantity,
    md.sale_total_price
FROM mock_data md;
