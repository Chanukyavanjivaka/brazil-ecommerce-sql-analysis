-- 1. Data types of all columns in the customers table
SELECT 
  column_name, 
  data_type
FROM `scaler-dsml-sql-452804.target.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'customers';


-- 2. First and last order dates
SELECT
  MIN(order_purchase_timestamp) AS first_order_date,
  MAX(order_purchase_timestamp) AS last_order_date
FROM `target.orders`;


-- 3. Count of distinct customer cities & states who placed orders
SELECT
  COUNT(DISTINCT c.customer_city)  AS city_count,
  COUNT(DISTINCT c.customer_state) AS state_count
FROM `target.customers` c
JOIN `target.orders` o
  ON c.customer_id = o.customer_id;


-- 4. Yearly order growth with lag
WITH yearly_orders AS (
  SELECT
    EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
    COUNT(*) AS total_orders
  FROM `target.orders`
  GROUP BY year
)
SELECT
  year,
  total_orders,
  LAG(total_orders) OVER (ORDER BY year) AS prev_year_orders,
  total_orders - LAG(total_orders) OVER (ORDER BY year) AS diff_orders
FROM yearly_orders
ORDER BY year;


-- 5. Monthly seasonality (Winter/Spring/Summer/Fall)
WITH olc AS (
  SELECT
    EXTRACT(YEAR FROM order_purchase_timestamp)  AS year,
    EXTRACT(MONTH FROM order_purchase_timestamp) AS month_number,
    FORMAT_TIMESTAMP('%B', order_purchase_timestamp) AS month_name,
    CASE
      WHEN EXTRACT(MONTH FROM order_purchase_timestamp) IN (12, 1, 2) THEN 'Winter'
      WHEN EXTRACT(MONTH FROM order_purchase_timestamp) IN (3, 4, 5) THEN 'Spring'
      WHEN EXTRACT(MONTH FROM order_purchase_timestamp) IN (6, 7, 8) THEN 'Summer'
      ELSE 'Fall'
    END AS season,
    COUNT(*) AS total_orders
  FROM `target.orders`
  GROUP BY 1,2,3,4
)
SELECT
  year, month_number, month_name, season,
  total_orders,
  SUM(total_orders) OVER (PARTITION BY year, season) AS season_orders
FROM olc
ORDER BY year, month_number;


-- 6. Orders by time-of-day category
SELECT
  CASE
    WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 0  AND  6 THEN 'Dawn'
    WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 7  AND 12 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 13 AND 18 THEN 'Afternoon'
    ELSE 'Night'
  END AS day_time,
  COUNT(*) AS order_count
FROM `target.orders`
GROUP BY day_time
ORDER BY order_count DESC;


-- 7. Month-on-month orders per state
SELECT
  EXTRACT(YEAR  FROM o.order_purchase_timestamp) AS year,
  EXTRACT(MONTH FROM o.order_purchase_timestamp) AS month,
  c.customer_state,
  COUNT(*) AS total_orders
FROM `target.orders` o
JOIN `target.customers` c
  ON o.customer_id = c.customer_id
GROUP BY 1,2,3
ORDER BY 1,2;


-- 8. Customer distribution by state
SELECT
  COUNT(DISTINCT customer_id) AS total_customers,
  customer_state
FROM `target.customers`
GROUP BY customer_state
ORDER BY total_customers DESC;


-- 9. % increase in total payment value Jan–Aug 2017 vs Jan–Aug 2018
WITH payment_year AS (
  SELECT
    EXTRACT(YEAR FROM o.order_purchase_timestamp) AS year,
    SUM(p.payment_value) AS total_payment
  FROM `target.orders` o
  JOIN `target.payments` p
    ON o.order_id = p.order_id
  WHERE EXTRACT(MONTH FROM o.order_purchase_timestamp) BETWEEN 1 AND 8
  GROUP BY year
)
SELECT
  MAX(CASE WHEN year = 2017 THEN total_payment END) AS payment_2017,
  MAX(CASE WHEN year = 2018 THEN total_payment END) AS payment_2018,
  ROUND(
    100 * (MAX(CASE WHEN year = 2018 THEN total_payment END)
         - MAX(CASE WHEN year = 2017 THEN total_payment END))
    / MAX(CASE WHEN year = 2017 THEN total_payment END),
    2
  ) AS pct_increase
FROM payment_year;


-- 10. Total & average order price by state
SELECT
  c.customer_state,
  ROUND(SUM(p.payment_value), 2) AS total_price,
  ROUND(AVG(p.payment_value), 2) AS avg_price
FROM `target.payments` p
JOIN `target.orders`   o ON p.order_id = o.order_id
JOIN `target.customers` c ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY c.customer_state;


-- 11. Total & average freight value by state
SELECT
  c.customer_state,
  ROUND(SUM(oi.freight_value), 2) AS total_freight,
  ROUND(AVG(oi.freight_value), 2) AS avg_freight
FROM `target.order_items` oi
JOIN `target.orders`     o ON oi.order_id = o.order_id
JOIN `target.customers`  c ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY c.customer_state;


-- 12. Delivery time & estimated vs actual diff
SELECT
  DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS time_to_deliver,
  DATE_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY) AS diff_estimated_delivery
FROM `target.orders`;


-- 13. Top 5 states highest & lowest avg freight
WITH state_freight AS (
  SELECT
    c.customer_state,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight
  FROM `target.order_items` oi
  JOIN `target.orders`     o ON oi.order_id = o.order_id
  JOIN `target.customers`  c ON o.customer_id = c.customer_id
  GROUP BY c.customer_state
),
ranked AS (
  SELECT
    customer_state,
    avg_freight,
    DENSE_RANK() OVER (ORDER BY avg_freight    ) AS low_rank,
    DENSE_RANK() OVER (ORDER BY avg_freight DESC) AS high_rank
  FROM state_freight
)
SELECT customer_state, avg_freight
FROM ranked
WHERE low_rank  <= 5
   OR high_rank <= 5
ORDER BY avg_freight DESC;


-- 14. Top 5 states highest & lowest avg delivery time
WITH state_delivery AS (
  SELECT
    c.customer_state,
    ROUND(AVG(DATE_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)), 2) AS avg_delivery
  FROM `target.orders`    o
  JOIN `target.customers` c ON o.customer_id = c.customer_id
  GROUP BY c.customer_state
),
drank AS (
  SELECT
    customer_state,
    avg_delivery,
    DENSE_RANK() OVER (ORDER BY avg_delivery    ) AS fastest_rank,
    DENSE_RANK() OVER (ORDER BY avg_delivery DESC) AS slowest_rank
  FROM state_delivery
)
SELECT customer_state, avg_delivery
FROM drank
WHERE fastest_rank <= 5
   OR slowest_rank <= 5
ORDER BY avg_delivery DESC;


-- 15. Top 5 states with earliest actual vs estimated delivery
SELECT
  c.customer_state,
  ROUND(
    AVG(DATE_DIFF(o.order_estimated_delivery_date, o.order_delivered_customer_date, DAY)),
    2
  ) AS avg_early_delivery
FROM `target.orders`    o
JOIN `target.customers` c ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY avg_early_delivery DESC
LIMIT 5;


-- 16. Month-on-month orders by payment type
SELECT
  FORMAT_TIMESTAMP('%Y-%m', o.order_purchase_timestamp) AS month,
  p.payment_type,
  COUNT(*) AS orders_count
FROM `target.orders`   o
JOIN `target.payments` p ON o.order_id = p.order_id
GROUP BY 1,2
ORDER BY month;


-- 17. Order counts by number of payment installments
SELECT
  payment_installments,
  COUNT(DISTINCT order_id) AS num_orders
FROM `target.payments`
WHERE payment_installments > 0
GROUP BY payment_installments
ORDER BY payment_installments;
