----------------------------------------
-- KIỂM TRA DỮ LIỆU LỚP SILVER
----------------------------------------

--  Tổng số dòng
SELECT COUNT(*) AS total_rows
FROM iceberg.silver.ecommerce_clean;

--  Kiểm tra giá trị NULL
SELECT
  COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_id,
  COUNT(*) FILTER (WHERE order_date IS NULL) AS null_order_date,
  COUNT(*) FILTER (WHERE product_id IS NULL) AS null_product_id,
  COUNT(*) FILTER (WHERE price IS NULL) AS null_price,
  COUNT(*) FILTER (WHERE quantity IS NULL) AS null_quantity,
  COUNT(*) FILTER (WHERE total_price IS NULL) AS null_total_price
FROM iceberg.silver.ecommerce_clean;

--  Kiểm tra giá trị âm hoặc bằng 0
SELECT COUNT(*) AS invalid_rows
FROM iceberg.silver.ecommerce_clean
WHERE quantity <= 0 OR price <= 0 OR total_price <= 0;

-- Kiểm tra format text (uppercase & trim)
SELECT DISTINCT category_name
FROM iceberg.silver.ecommerce_clean
WHERE category_name != UPPER(TRIM(category_name))
LIMIT 10;

--  Kiểm tra consistency logic (tổng tiền = số lượng × giá)
SELECT COUNT(*) AS mismatch_count
FROM iceberg.silver.ecommerce_clean
WHERE ABS(total_price - (quantity * price)) > 0.01;

--  Kiểm tra giá trị trong cột phân loại (category, payment_method)
SELECT payment_method, COUNT(*) AS cnt
FROM iceberg.silver.ecommerce_clean
GROUP BY payment_method
ORDER BY cnt DESC;

-- Thống kê nhanh để đánh giá dữ liệu sạch
SELECT
  ROUND(AVG(price), 2) AS avg_price,
  ROUND(AVG(quantity), 2) AS avg_quantity,
  ROUND(AVG(total_price), 2) AS avg_total_price
FROM iceberg.silver.ecommerce_clean;


----------------------------------------
--  KIỂM TRA DỮ LIỆU LỚP GOLD
----------------------------------------

--  Kiểm tra danh sách bảng Gold
SHOW TABLES FROM iceberg.gold;

--  Kiểm tra schema FactSales
DESCRIBE iceberg.gold.fact_sales;

--  Tổng số dòng FactSales
SELECT COUNT(*) AS total_rows
FROM iceberg.gold.fact_sales;

--  Dimension Tables

-- DimCustomer
SELECT COUNT(DISTINCT customerid) AS unique_customers
FROM iceberg.gold.dim_customer;

SELECT * FROM iceberg.gold.dim_customer LIMIT 10;

-- DimProduct
SELECT COUNT(DISTINCT productid) AS unique_products
FROM iceberg.gold.dim_product;

SELECT * FROM iceberg.gold.dim_product LIMIT 10;

-- DimDate
SELECT MIN(fulldate) AS start_date, MAX(fulldate) AS end_date
FROM iceberg.gold.dim_date;

--  Kiểm tra NULL trong FactSales
SELECT 
  COUNT(*) FILTER (WHERE customerkey IS NULL) AS null_customer,
  COUNT(*) FILTER (WHERE productkey IS NULL) AS null_product,
  COUNT(*) FILTER (WHERE categorykey IS NULL) AS null_category,
  COUNT(*) FILTER (WHERE citykey IS NULL) AS null_city,
  COUNT(*) FILTER (WHERE paymentkey IS NULL) AS null_payment,
  COUNT(*) FILTER (WHERE datekey IS NULL) AS null_date
FROM iceberg.gold.fact_sales;

-- Kiểm tra consistency logic (TotalAmount = Quantity * Price)
SELECT COUNT(*) AS mismatch_count
FROM iceberg.gold.fact_sales
WHERE ABS(totalamount - (quantity * price)) > 0.01;

--  Tổng doanh thu
SELECT ROUND(SUM(totalamount), 2) AS total_revenue
FROM iceberg.gold.fact_sales;


