-- Create the database
CREATE DATABASE restaurant_analytics;
GO

-- Switch to the database
USE restaurant_analytics;
GO

-- Create a schema
CREATE SCHEMA restaurant AUTHORIZATION dbo;
GO

-- Create date_dim table
CREATE TABLE restaurant.date_dim (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day_of_week VARCHAR(20),
    is_weekend BIT
);
GO

-- Create order_item_options table
CREATE TABLE restaurant.order_item_options (
    order_id VARCHAR(50),
    lineitem_id VARCHAR(50),
    option_group_name VARCHAR(100),
    option_name VARCHAR(100),
    option_price DECIMAL(10,2),
    option_quantity INT,
    PRIMARY KEY (order_id, lineitem_id, option_name)
);
GO

-- Create order_items table
CREATE TABLE restaurant.order_items (
    app_name VARCHAR(50),
    restaurant_id VARCHAR(50),
    creation_time_utc DATETIME,
    order_id VARCHAR(50),
    user_id VARCHAR(50),
    printed_card_number VARCHAR(50),
    is_loyalty BIT,
    currency VARCHAR(10), 
    lineitem_id VARCHAR(50),
    item_category VARCHAR(50),
    item_name VARCHAR(100),
    item_price DECIMAL(10,2),
    item_quantity INT,
    PRIMARY KEY (order_id, lineitem_id)
);
GO

-------------------------------------------------------------------------------


-- Use your DB
USE restaurant_analytics;
GO

-- What tables do we have?
SELECT s.name AS schema_name, t.name AS table_name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
ORDER BY s.name, t.name;
GO


-------------------------------------------------------------------------------

-- RAW tables (as uploaded)
SELECT 'dbo.raw_order_items'        AS tbl, COUNT(*) AS rows FROM dbo.raw_order_items
UNION ALL SELECT 'dbo.raw_order_item_options', COUNT(*)        FROM dbo.raw_order_item_options
UNION ALL SELECT 'dbo.raw_date_dim',           COUNT(*)        FROM dbo.raw_date_dim;
GO


-------------------------------------------------------------------------------

-- CURATED tables (if you built them already)
SELECT 'restaurant.order_items'         AS tbl, COUNT(*) AS rows FROM restaurant.order_items
UNION ALL SELECT 'restaurant.order_item_options', COUNT(*)       FROM restaurant.order_item_options
UNION ALL SELECT 'restaurant.date_dim',           COUNT(*)       FROM restaurant.date_dim;
GO

-------------------------------------------------------------------------------

SELECT MIN(CREATION_TIME_UTC) AS min_ts,
       MAX(CREATION_TIME_UTC) AS max_ts
FROM dbo.raw_order_items;
GO

-------------------------------------------------------------------------------

-- How many unique restaurants and app names do we have?
SELECT COUNT(DISTINCT RESTAURANT_ID) AS distinct_restaurant_ids,
       COUNT(DISTINCT APP_NAME)      AS distinct_app_names
FROM dbo.raw_order_items;
GO

-- Top APP_NAMEs by volume (brand/site)
SELECT TOP (20) APP_NAME, COUNT(*) AS n
FROM dbo.raw_order_items
GROUP BY APP_NAME
ORDER BY n DESC;
GO

-- Distribution of RESTAURANT_ID within each APP_NAME (spot-check)
SELECT TOP (20) APP_NAME, RESTAURANT_ID, COUNT(*) AS n
FROM dbo.raw_order_items
GROUP BY APP_NAME, RESTAURANT_ID
ORDER BY n DESC;
GO

-------------------------------------------------------------------------------

-- Any non-numeric values sneaking into quantity/price?
SELECT TOP (50) ITEM_PRICE, ITEM_QUANTITY
FROM dbo.raw_order_items
WHERE TRY_CONVERT(decimal(18,2), ITEM_PRICE) IS NULL
   OR TRY_CONVERT(int, ITEM_QUANTITY) IS NULL;
GO

-------------------------------------------------------------------------------

SELECT TOP (25)
  ORDER_ID,
  USER_ID            AS customer_id,
  APP_NAME,
  RESTAURANT_ID,
  ITEM_CATEGORY,
  ITEM_NAME,
  ITEM_PRICE,
  ITEM_QUANTITY,
  CREATION_TIME_UTC
FROM dbo.raw_order_items
ORDER BY CREATION_TIME_UTC DESC;
GO

-------------------------------------------------------------------------------
