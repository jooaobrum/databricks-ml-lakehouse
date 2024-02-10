WITH orders_by_customers_window AS (
  SELECT t1.*,
         t2.customer_unique_id,
         t3.order_item_id,
         t3.product_id,
         t3.seller_id,
         t3.item_price,
         t3.freight_price
  FROM silver.olist_orders AS t1
  LEFT JOIN silver.olist_customers AS t2
  ON t1.customer_id = t2.customer_id
  LEFT JOIN silver.olist_order_items AS t3
  ON t1.order_id = t3.order_id
  WHERE t1.order_purchase_timestamp <= DATE('{date}')
  AND t1.order_purchase_timestamp > DATE_ADD(DATE('{date}'), -{window})
  AND t1.order_status <> 'processing'
  AND t2.customer_unique_id IS NOT NULL
),

agg_orders AS (
  SELECT customer_unique_id,
         COUNT(order_id) AS total_items_order_{window}_days,
         COUNT(DISTINCT order_id) AS total_orders_{window}_days,
         COUNT(CASE WHEN order_status = 'delivered' THEN order_id END) AS total_orders_delivered_{window}_days,
         COUNT(CASE WHEN order_status = 'unavailable' THEN order_id END) AS total_orders_unavailable_{window}_days,
         SUM(item_price) AS total_spent_price_{window}_days,
         SUM(freight_price) AS total_spent_freight_{window}_days
  FROM orders_by_customers_window
  GROUP BY customer_unique_id
),

agg_items_categories as (

  SELECT  t1.customer_unique_id,
          SUM(CASE WHEN t3.product_category_name_english = 'art' THEN 1 ELSE 0 END) as total_art_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'flowers' THEN 1 ELSE 0 END) as total_flowers_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'home_construction' THEN 1 ELSE 0 END) as total_home_construction_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fashion_male_clothing' THEN 1 ELSE 0 END) as total_fashion_male_clothing_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'kitchen_dining_laundry_garden_furniture' THEN 1 ELSE 0 END) as total_kitchen_dining_laundry_garden_furniture_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'small_appliances' THEN 1 ELSE 0 END) as total_small_appliances_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'la_cuisine' THEN 1 ELSE 0 END) as total_la_cuisine_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'bed_bath_table' THEN 1 ELSE 0 END) as total_bed_bath_table_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'signaling_and_security' THEN 1 ELSE 0 END) as total_signaling_and_security_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'office_furniture' THEN 1 ELSE 0 END) as total_office_furniture_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'computers' THEN 1 ELSE 0 END) as total_computers_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'watches_gifts' THEN 1 ELSE 0 END) as total_watches_gifts_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'auto' THEN 1 ELSE 0 END) as total_auto_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fashion_bags_accessories' THEN 1 ELSE 0 END) as total_fashion_bags_accessories_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'construction_tools_lights' THEN 1 ELSE 0 END) as total_construction_tools_lights_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'cool_stuff' THEN 1 ELSE 0 END) as total_cool_stuff_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'cds_dvds_musicals' THEN 1 ELSE 0 END) as total_cds_dvds_musicals_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'food' THEN 1 ELSE 0 END) as total_food_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'computers_accessories' THEN 1 ELSE 0 END) as total_computers_accessories_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'perfumery' THEN 1 ELSE 0 END) as total_perfumery_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'pet_shop' THEN 1 ELSE 0 END) as total_pet_shop_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'stationery' THEN 1 ELSE 0 END) as total_stationery_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'furniture_decor' THEN 1 ELSE 0 END) as total_furniture_decor_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'drinks' THEN 1 ELSE 0 END) as total_drinks_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'construction_tools_safety' THEN 1 ELSE 0 END) as total_construction_tools_safety_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'telephony' THEN 1 ELSE 0 END) as total_telephony_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'home_confort' THEN 1 ELSE 0 END) as total_home_confort_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'furniture_mattress_and_upholstery' THEN 1 ELSE 0 END) as total_furniture_mattress_and_upholstery_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'party_supplies' THEN 1 ELSE 0 END) as total_party_supplies_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'agro_industry_and_commerce' THEN 1 ELSE 0 END) as total_agro_industry_and_commerce_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'cine_photo' THEN 1 ELSE 0 END) as total_cine_photo_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'books_technical' THEN 1 ELSE 0 END) as total_books_technical_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'home_appliances_2' THEN 1 ELSE 0 END) as total_home_appliances_2_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'music' THEN 1 ELSE 0 END) as total_music_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'costruction_tools_garden' THEN 1 ELSE 0 END) as total_costruction_tools_garden_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'christmas_supplies' THEN 1 ELSE 0 END) as total_christmas_supplies_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'audio' THEN 1 ELSE 0 END) as total_audio_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fashio_female_clothing' THEN 1 ELSE 0 END) as total_fashio_female_clothing_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'air_conditioning' THEN 1 ELSE 0 END) as total_air_conditioning_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'market_place' THEN 1 ELSE 0 END) as total_market_place_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'health_beauty' THEN 1 ELSE 0 END) as total_health_beauty_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fashion_shoes' THEN 1 ELSE 0 END) as total_fashion_shoes_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fashion_underwear_beach' THEN 1 ELSE 0 END) as total_fashion_underwear_beach_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'housewares' THEN 1 ELSE 0 END) as total_housewares_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'home_appliances' THEN 1 ELSE 0 END) as total_home_appliances_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'costruction_tools_tools' THEN 1 ELSE 0 END) as total_costruction_tools_tools_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'furniture_living_room' THEN 1 ELSE 0 END) as total_furniture_living_room_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'arts_and_craftmanship' THEN 1 ELSE 0 END) as total_arts_and_craftmanship_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fixed_telephony' THEN 1 ELSE 0 END) as total_fixed_telephony_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'electronics' THEN 1 ELSE 0 END) as total_electronics_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'diapers_and_hygiene' THEN 1 ELSE 0 END) as total_diapers_and_hygiene_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'books_imported' THEN 1 ELSE 0 END) as total_books_imported_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'baby' THEN 1 ELSE 0 END) as total_baby_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'home_comfort_2' THEN 1 ELSE 0 END) as total_home_comfort_2_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fashion_childrens_clothes' THEN 1 ELSE 0 END) as total_fashion_childrens_clothes_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'luggage_accessories' THEN 1 ELSE 0 END) as total_luggage_accessories_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'small_appliances_home_oven_and_coffee' THEN 1 ELSE 0 END) as total_small_appliances_home_oven_and_coffee_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'food_drink' THEN 1 ELSE 0 END) as total_food_drink_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'furniture_bedroom' THEN 1 ELSE 0 END) as total_furniture_bedroom_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'tablets_printing_image' THEN 1 ELSE 0 END) as total_tablets_printing_image_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'fashion_sport' THEN 1 ELSE 0 END) as total_fashion_sport_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'toys' THEN 1 ELSE 0 END) as total_toys_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'consoles_games' THEN 1 ELSE 0 END) as total_consoles_games_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'garden_tools' THEN 1 ELSE 0 END) as total_garden_tools_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'sports_leisure' THEN 1 ELSE 0 END) as total_sports_leisure_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'industry_commerce_and_business' THEN 1 ELSE 0 END) as total_industry_commerce_and_business_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'security_and_services' THEN 1 ELSE 0 END) as total_security_and_services_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'books_general_interest' THEN 1 ELSE 0 END) as total_books_general_interest_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'construction_tools_construction' THEN 1 ELSE 0 END) as total_construction_tools_construction_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'dvds_blu_ray' THEN 1 ELSE 0 END) as total_dvds_blu_ray_items_{window}_days,
          SUM(CASE WHEN t3.product_category_name_english = 'musical_instruments' THEN 1 ELSE 0 END) as total_musical_instruments_item_{window}_days

  FROM orders_by_customers_window as t1
  LEFT JOIN silver.olist_products as t2
  ON t1.product_id = t2.product_id
  LEFT JOIN silver.olist_product_category_name_translation as t3
  ON t2.product_category_name_portuguese = t3.product_category_name_portuguese
  GROUP BY t1.customer_unique_id


),

agg_customers_payments as (
  SELECT
    t1.customer_unique_id,
    COUNT(DISTINCT CASE WHEN t2.payment_type = 'credit_card' THEN t2.order_id END) AS total_credit_card_{window}_days,
    COUNT(DISTINCT CASE WHEN t2.payment_type = 'boleto' THEN t2.order_id END) AS total_boleto_{window}_days,
    COUNT(DISTINCT CASE WHEN t2.payment_type = 'voucher' THEN t2.order_id END) AS total_voucher_{window}_days,
    COUNT(DISTINCT CASE WHEN t2.payment_type = 'debit_card' THEN t2.order_id END) AS total_debit_card_{window}_days
  FROM
    orders_by_customers_window AS t1
    LEFT JOIN silver.olist_order_payments AS t2 ON t1.order_id = t2.order_id  -- Add the join condition here
  GROUP BY
    t1.customer_unique_id
),

customer_feature_store as (
  SELECT
    '{date}' AS fs_reference_timestamp,
    t1.customer_unique_id,
    t1.total_items_order_{window}_days,
    t1.total_orders_{window}_days,
    t1.total_orders_delivered_{window}_days,
    t1.total_orders_unavailable_{window}_days,
    t1.total_spent_price_{window}_days,
    t1.total_spent_freight_{window}_days,
    t2.total_art_items_{window}_days,
    t2.total_flowers_items_{window}_days,
    t2.total_home_construction_items_{window}_days,
    t2.total_fashion_male_clothing_items_{window}_days,
    t2.total_kitchen_dining_laundry_garden_furniture_items_{window}_days,
    t2.total_small_appliances_items_{window}_days,
    t2.total_la_cuisine_items_{window}_days,
    t2.total_bed_bath_table_items_{window}_days,
    t2.total_signaling_and_security_items_{window}_days,
    t2.total_office_furniture_items_{window}_days,
    t2.total_computers_items_{window}_days,
    t2.total_watches_gifts_items_{window}_days,
    t2.total_auto_items_{window}_days,
    t2.total_fashion_bags_accessories_items_{window}_days,
    t2.total_construction_tools_lights_items_{window}_days,
    t2.total_cool_stuff_items_{window}_days,
    t2.total_cds_dvds_musicals_items_{window}_days,
    t2.total_food_items_{window}_days,
    t2.total_computers_accessories_items_{window}_days,
    t2.total_perfumery_items_{window}_days,
    t2.total_pet_shop_items_{window}_days,
    t2.total_stationery_items_{window}_days,
    t2.total_furniture_decor_items_{window}_days,
    t2.total_drinks_items_{window}_days,
    t2.total_construction_tools_safety_items_{window}_days,
    t2.total_telephony_items_{window}_days,
    t2.total_home_confort_items_{window}_days,
    t2.total_furniture_mattress_and_upholstery_items_{window}_days,
    t2.total_party_supplies_items_{window}_days,
    t2.total_agro_industry_and_commerce_items_{window}_days,
    t2.total_cine_photo_items_{window}_days,
    t2.total_books_technical_items_{window}_days,
    t2.total_home_appliances_2_items_{window}_days,
    t2.total_music_items_{window}_days,
    t2.total_costruction_tools_garden_items_{window}_days,
    t2.total_christmas_supplies_items_{window}_days,
    t2.total_audio_items_{window}_days,
    t2.total_fashio_female_clothing_items_{window}_days,
    t2.total_air_conditioning_items_{window}_days,
    t2.total_market_place_items_{window}_days,
    t2.total_health_beauty_items_{window}_days,
    t2.total_fashion_shoes_items_{window}_days,
    t2.total_fashion_underwear_beach_items_{window}_days,
    t2.total_housewares_items_{window}_days,
    t2.total_home_appliances_items_{window}_days,
    t2.total_costruction_tools_tools_items_{window}_days,
    t2.total_furniture_living_room_items_{window}_days,
    t2.total_arts_and_craftmanship_items_{window}_days,
    t2.total_fixed_telephony_items_{window}_days,
    t2.total_electronics_items_{window}_days,
    t2.total_diapers_and_hygiene_items_{window}_days,
    t2.total_books_imported_items_{window}_days,
    t2.total_baby_items_{window}_days,
    t2.total_home_comfort_2_items_{window}_days,
    t2.total_fashion_childrens_clothes_items_{window}_days,
    t2.total_luggage_accessories_items_{window}_days,
    t2.total_small_appliances_home_oven_and_coffee_items_{window}_days,
    t2.total_food_drink_items_{window}_days,
    t2.total_furniture_bedroom_items_{window}_days,
    t2.total_tablets_printing_image_items_{window}_days,
    t2.total_fashion_sport_items_{window}_days,
    t2.total_toys_items_{window}_days,
    t2.total_consoles_games_items_{window}_days,
    t2.total_garden_tools_items_{window}_days,
    t2.total_sports_leisure_items_{window}_days,
    t2.total_industry_commerce_and_business_items_{window}_days,
    t2.total_security_and_services_items_{window}_days,
    t2.total_books_general_interest_items_{window}_days,
    t2.total_construction_tools_construction_items_{window}_days,
    t2.total_dvds_blu_ray_items_{window}_days,
    t2.total_musical_instruments_item_{window}_days,
    t3.total_credit_card_{window}_days,
    t3.total_boleto_{window}_days,
    t3.total_voucher_{window}_days,
    t3.total_debit_card_{window}_days

    
  FROM
    agg_orders AS t1
    LEFT JOIN agg_items_categories AS t2 ON t1.customer_unique_id = t2.customer_unique_id
    LEFT JOIN agg_customers_payments AS t3 ON t1.customer_unique_id = t3.customer_unique_id


)

SELECT * FROM customer_feature_store;