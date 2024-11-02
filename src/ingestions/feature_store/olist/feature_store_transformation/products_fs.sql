
WITH prod_main_category AS (
  SELECT t1.order_id,
  CASE
          WHEN t2.product_category_name_portuguese IN ('perfumaria', 'beleza_saude', 'fraldas_higiene') THEN 'Beauty & Health'
          WHEN t2.product_category_name_portuguese IN ('artes', 'artes_e_artesanato', 'livros_interesse_geral', 'livros_importados', 'livros_tecnicos', 'dvds_blu_ray', 'cds_dvds_musicais', 'musica', 'cine_foto') THEN 'Arts & Entertainment'
          WHEN t2.product_category_name_portuguese IN ('esporte_lazer', 'fashion_esporte', 'cool_stuff') THEN 'Sports & Leisure'
          WHEN t2.product_category_name_portuguese IN ('bebes', 'brinquedos', 'fashion_roupa_infanto_juvenil') THEN 'Baby & Kids'
          WHEN t2.product_category_name_portuguese IN ('moveis_decoracao', 'cama_mesa_banho', 'utilidades_domesticas', 'moveis_escritorio', 'moveis_sala', 'moveis_cozinha_area_de_servico_jantar_e_jardim', 'moveis_quarto', 'moveis_colchao_e_estofado', 'casa_conforto', 'casa_conforto_2') THEN 'Home & Furniture'
          WHEN t2.product_category_name_portuguese IN ('eletrodomesticos', 'eletrodomesticos_2', 'eletroportateis', 'portateis_casa_forno_e_cafe', 'portateis_cozinha_e_preparadores_de_alimentos', 'climatizacao') THEN 'Electronics & Appliances'
          WHEN t2.product_category_name_portuguese IN ('informatica_acessorios', 'pcs', 'pc_gamer', 'tablets_impressao_imagem', 'telefonia', 'telefonia_fixa', 'eletronicos') THEN 'Technology & Gadgets'
          WHEN product_category_name_portuguese IN ('construcao_ferramentas_seguranca', 'construcao_ferramentas_construcao', 'construcao_ferramentas_ferramentas', 'construcao_ferramentas_iluminacao', 'construcao_ferramentas_jardim', 'ferramentas_jardim', 'sinalizacao_e_seguranca') THEN 'Construction & Tools'
          WHEN t2.product_category_name_portuguese IN ('automotivo', 'agro_industria_e_comercio', 'industria_comercio_e_negocios', 'seguros_e_servicos') THEN 'Automotive & Industry'
          WHEN t2.product_category_name_portuguese IN ('fashion_calcados', 'fashion_bolsas_e_acessorios', 'fashion_roupa_masculina', 'fashion_roupa_feminina', 'fashion_underwear_e_moda_praia') THEN 'Fashion & Accessories'
          ELSE 'Other'
      END AS product_main_category

  FROM olist_silver.olist_order_items as t1
  LEFT JOIN olist_silver.olist_products as t2
  ON t1.product_id = t2.product_id
  ),

  order_products_category AS (
    SELECT DISTINCT order_id,
    CASE WHEN product_main_category = 'Beauty & Health' THEN 1 ELSE 0 END AS beauty_health,
    CASE WHEN product_main_category = 'Arts & Entertainment' THEN 1 ELSE 0 END AS arts_entertainment,
    CASE WHEN product_main_category = 'Sports & Leisure' THEN 1 ELSE 0 END AS sports_leisure,
    CASE WHEN product_main_category = 'Baby & Kids' THEN 1 ELSE 0 END AS baby_kids,
    CASE WHEN product_main_category = 'Home & Furniture' THEN 1 ELSE 0 END AS home_furniture,
    CASE WHEN product_main_category = 'Electronics & Appliances' THEN 1 ELSE 0 END AS electronics_appliances,
    CASE WHEN product_main_category = 'Technology & Gadgets' THEN 1 ELSE 0 END AS technology_gadgets,
    CASE WHEN product_main_category = 'Construction & Tools' THEN 1 ELSE 0 END AS construction_tools,
    CASE WHEN product_main_category = 'Automotive & Industry' THEN 1 ELSE 0 END AS automotive_industry,
    CASE WHEN product_main_category = 'Fashion & Accessories' THEN 1 ELSE 0 END AS fashion_accessories


    FROM prod_main_category
  ),

  order_products_characteristics AS (
    SELECT t1.order_id,
           AVG(t2.product_description_lenght) as avg_order_prod_description_length,
           AVG(t2.product_photos_qty) as avg_order_photos_qty,
           SUM(t2.product_weight_kg) as total_order_weight
    

  FROM olist_silver.olist_order_items as t1
  LEFT JOIN olist_silver.olist_products as t2
  ON t1.product_id = t2.product_id
    
  GROUP BY t1.order_id
  ),

       
SELECT t1.order_delivered_customer_timestamp as dt_ref,
       t1.order_id,
       t2.beauty_health,
       t2.arts_entertainment,
       t2.sports_leisure,
       t2.baby_kids,
       t2.home_furniture,
       t2.electronics_appliances,
       t2.technology_gadgets,
       t2.construction_tools,
       t2.automotive_industry,
       t2.fashion_accessories,
       COALESCE(t3.avg_order_prod_description_length, 0) as avg_order_prod_description_length,
       COALESCE(t3.avg_order_photos_qty,0) as avg_order_photos_qty,
       t3.total_order_weight


       

FROM olist_silver.olist_orders as t1
INNER JOIN order_products_category as t2
ON t1.order_id = t2.order_id
INNER JOIN order_products_characteristics as t3
ON t1.order_id = t3.order_id

WHERE t1.order_status = 'delivered'
AND t1.order_delivered_customer_timestamp = '{dt_ingestion}'
