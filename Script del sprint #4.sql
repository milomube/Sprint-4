-- Sprint #4: BigQuery Avanzado & Analytics Engineering
-- Nivel #1 Entorno e Ingesta Híbrida (Code-First)
-- Script del ejercicio #1: Consulta sobre Tabla no Optimizada (Diagnóstico)
SELECT * FROM `sprint3-analytics-juan-camilo.sprint3_silver.transactions_clean` t
JOIN `sprint3-analytics-juan-camilo.sprint3_silver.companies_clean` c ON t.business_id = c.company_id
WHERE c.country = 'Germany' AND DATE(t.timestamp) = '2022-03-12';

-- Script del ejercicio #2: Re-arquitectura y Optimización del Almacenamiento (Partition & Cluster)
CREATE OR REPLACE TABLE `sprint3-analytics-juan-camilo.sprint3_silver.transactions_recent` AS
SELECT * EXCEPT(timestamp),
    TIMESTAMP_SUB(
        CURRENT_TIMESTAMP(),
        INTERVAL CAST(RAND() * 50 AS INT64) DAY
    ) AS timestamp
FROM `sprint3-analytics-juan-camilo.sprint3_silver.transactions_clean`;

-- Sentencia DDL
CREATE OR REPLACE TABLE `sprint3-analytics-juan-camilo.sprint3_gold.fact_transactions_optimized`
PARTITION BY DATE(timestamp)
CLUSTER BY business_id AS
SELECT * FROM `sprint3-analytics-juan-camilo.sprint3_silver.transactions_recent`;

-- Script del ejercicio #3: La Prueba del Algodón (Benchmark)

SELECT * FROM `sprint3-analytics-juan-camilo.sprint3_silver.transactions_recent`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

SELECT * FROM `sprint3-analytics-juan-camilo.sprint3_gold.fact_transactions_recent`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

-- Script del ejercicio #4: Smart Caching (Vistas Materializadas)

-- Script para crear la vista materializada
CREATE MATERIALIZED VIEW `sprint3-analytics-juan-camilo.sprint3_gold.mv_daily_sales` AS
SELECT DATE(timestamp) AS fecha, SUM(amount) AS ventas_totales
FROM `sprint3-analytics-juan-camilo.sprint3_gold.fact_transactions_optimized`
GROUP BY fecha;

-- Script para validar la vista materializada
SELECT *
FROM `sprint3-analytics-juan-camilo.sprint3_gold.mv_daily_sales`
ORDER BY fecha;

-- Nivel #2: SQL Analítico Avanzado
-- Ejercicio 1: Perfilado de Clientes VIP mediante CTEs

WITH VIP_Stats AS (SELECT user_id, COUNT(*) AS num_compras, ROUND(AVG(amount), 2) AS ticket_medio, MAX(amount) AS max_compra, ROUND(SUM(amount), 2) AS total_gastado
FROM`sprint3-analytics-juan-camilo.sprint3_silver.transactions_clean`
GROUP BY user_id
HAVING SUM(amount) > 500), 

Users_info AS (SELECT user_id, CONCAT(name, ' ', surname) AS nombre_completo, email
FROM `sprint3-analytics-juan-camilo.sprint3_silver.users_combined`)

SELECT v.user_id, u.nombre_completo, u.email, v.num_compras, v.ticket_medio, v.max_compra, v.total_gastado,
FROM VIP_Stats v
JOIN Users_info u ON u.user_id = v.user_id
ORDER BY v.total_gastado DESC;  

-- Ejercicio 2: Análisis de Tendencias (Window Functions sobre Vistas)

SELECT fecha, ROUND(ventas_totales, 2) AS ventas_hoy, ROUND(LAG(ventas_totales) OVER(ORDER BY fecha), 2) AS ventas_ayer, 
  ROUND(SAFE_DIVIDE((ventas_totales - (LAG(ventas_totales) OVER(ORDER BY fecha))), (LAG(ventas_totales) OVER(ORDER BY fecha))) * 100, 2) AS diff_percentual
FROM `sprint3-analytics-juan-camilo.sprint3_gold.mv_daily_sales`
ORDER BY fecha;

-- Ejercicio 3: Totales acumulados (Running Totals)

SELECT fecha, ROUND(ventas_totales, 2) AS ventas_dia,
  ROUND(SUM(ventas_totales) OVER (PARTITION BY EXTRACT(YEAR FROM fecha)
    ORDER BY fecha
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS ventas_acumuladas_ytd
FROM `sprint3-analytics-juan-camilo.sprint3_gold.mv_daily_sales`
ORDER BY fecha;

-- Ejercicio 4: Fidelización y Valor del Cliente (Filtraje Avanzado)

WITH primeras_tres AS (SELECT user_id, timestamp, amount,
        ROW_NUMBER() OVER(
            PARTITION BY user_id
            ORDER BY timestamp, transaction_id) AS num_compra
    FROM `sprint3-analytics-juan-camilo.sprint3_silver.transactions_clean`
    QUALIFY num_compra <= 3)

SELECT t.user_id, CONCAT(u.name, ' ', u.surname) AS nombre_completo, u.email,
    MAX(IF(t.num_compra = 3, t.timestamp, NULL)) AS fecha_tercera_compra,
    ROUND(MAX(IF(t.num_compra = 3, t.amount, NULL)), 2) AS importe_tercera_compra,
    ROUND(AVG(t.amount), 2) AS media_primeras_tres
FROM primeras_tres AS t
JOIN `sprint3-analytics-juan-camilo.sprint3_silver.users_combined` AS u ON t.user_id = u.user_id
GROUP BY t.user_id, u.name, u.surname, u.email
HAVING COUNT(*) = 3
ORDER BY fecha_tercera_compra;

-- Nivel #3: Analytics Engineering (Arrays & Automatización)
-- Ejercicio 1: Desanimiento y Allanamiento de Datos (Unnesting)

CREATE OR REPLACE TABLE `sprint3-analytics-juan-camilo.sprint3_gold.dim_transactions_flat` AS
SELECT t.transaction_id, t.timestamp, t.amount AS total_ticket, producto_id AS product_sku, p.name AS product_name, p.price AS product_price
FROM `sprint3-analytics-juan-camilo.sprint3_silver.transactions_clean` AS t
CROSS JOIN UNNEST(t.product_ids) AS producto_id
INNER JOIN `sprint3-analytics-juan-camilo.sprint3_silver.products_clean` AS p ON producto_id = p.product_id;

-- Ejercicio 2: El Ranking de Ventas (Agregación Simple)

SELECT product_name, COUNT(product_sku) AS unidades_vendidas
FROM `sprint3-analytics-juan-camilo.sprint3_gold.dim_transactions_flat`
GROUP BY product_name
ORDER BY unidades_vendidas DESC
LIMIT 5;

-- Ejercicio 3: Automatización del Pipeline y Visualización

-- UDF calculate_tax

CREATE OR REPLACE FUNCTION `sprint3-analytics-juan-camilo.sprint3_gold.calculate_tax`(amount FLOAT64)
RETURNS FLOAT64 AS (ROUND(amount * 1.21, 2));

-- Tabla modificada

CREATE OR REPLACE TABLE `sprint3-analytics-juan-camilo.sprint3_gold.dim_transactions_flat`AS
SELECT t.transaction_id, t.timestamp, t.amount AS total_ticket, producto_id AS product_sku, p.name AS product_name, p.price AS product_price,
    `sprint3-analytics-juan-camilo.sprint3_gold.calculate_tax`(p.price) AS product_price_tax_inc
FROM `sprint3-analytics-juan-camilo.sprint3_silver.transactions_clean` AS t
CROSS JOIN UNNEST(t.product_ids) AS producto_id
INNER JOIN `sprint3-analytics-juan-camilo.sprint3_silver.products_clean` AS p ON producto_id = p.product_id;

-- Ingresos calculados con IVA

SELECT ROUND(SUM(product_price_tax_inc), 2) AS ingresos_totales, COUNT(DISTINCT transaction_id) AS transacciones_unicas,
    ROUND(SAFE_DIVIDE(SUM(product_price_tax_inc), COUNT(DISTINCT transaction_id)), 2) AS ticket_medio
FROM `sprint3-analytics-juan-camilo.sprint3_gold.dim_transactions_flat`;

-- Evolución diara de las ventas

SELECT DATE(timestamp) AS fecha, ROUND(SUM(product_price_tax_inc), 2) AS ventas_diarias
FROM `sprint3-analytics-juan-camilo.sprint3_gold.dim_transactions_flat`
GROUP BY fecha
ORDER BY fecha;

-- Ranking productos

SELECT product_name, COUNT(*) AS unidades_vendidas, ROUND(SUM(product_price_tax_inc), 2) AS ingresos_totales,
    ROUND(SUM(product_price_tax_inc) - SUM(product_price), 2) AS iva_calculado
FROM `sprint3-analytics-juan-camilo.sprint3_gold.dim_transactions_flat`
GROUP BY product_name
ORDER BY unidades_vendidas DESC;