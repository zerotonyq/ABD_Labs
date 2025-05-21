package ru.bigdata.flink.dto;

import lombok.*;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MockDataDto implements Serializable {
    // Данные о клиенте
    private Integer id;
    private String customer_first_name;
    private String customer_last_name;
    private Integer customer_age;
    private String customer_email;
    private String customer_country;
    private String customer_postal_code;
    private String customer_pet_type; // Тип животного
    private String customer_pet_name; // Имя животного
    private String customer_pet_breed; // Порода животного

    // Данные о продавце
    private Integer sale_seller_id;
    private String seller_first_name;
    private String seller_last_name;
    private String seller_email;
    private String seller_country;
    private String seller_postal_code;

    // Данные о магазине
    private String store_name;
    private String store_location;
    private String store_country;
    private String store_state;
    private String store_city;
    private String store_phone;
    private String store_email;

    // Данные о продукте
    private String product_category;
    private String pet_category;
    private String product_name;
    private String product_color;
    private String product_size;
    private String product_brand;
    private String product_material;
    private String product_description;
    private Double product_price;
    private Double product_weight;
    private Double product_rating;
    private Integer product_reviews;
    private String product_release_date;
    private String product_expiry_date;

    // Недостающее поле для количества продукта
    private Integer product_quantity;  // Добавлено поле для количества товара

    // Данные о поставщике
    private String supplier_name;
    private String supplier_contact;
    private String supplier_email;
    private String supplier_phone;
    private String supplier_address;
    private String supplier_city;
    private String supplier_country;

    // Данные о продаже
    private Integer sale_customer_id;
    private Integer sale_product_id;
    private Integer sale_quantity;
    private Double sale_total_price;
    private String sale_date;
}
