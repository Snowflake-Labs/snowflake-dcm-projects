/*=============================================================================
  02_post_deploy.sql — Run AFTER the first successful deployment

  Inserts sample data into the raw tables, refreshes dynamic tables,
  and verifies the pipeline output.

  Safe to re-run: dimension inserts skip rows that already exist, and
  order header/detail inserts use a computed offset so each run appends
  20 fresh orders without colliding with previous runs.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Insert Sample Data
----------------------------------------------------------------------
USE ROLE dcm_developer;

    INSERT INTO dcm_demo_1_dev.raw.truck (TRUCK_ID, TRUCK_BRAND_NAME, MENU_TYPE)
    SELECT TRUCK_ID, TRUCK_BRAND_NAME, MENU_TYPE
    FROM (VALUES
        (103, 'Taco Titan', 'Mexican Street Food'),
        (104, 'The Rolling Dough', 'Artisan Pizza'),
        (105, 'Wok n Roll', 'Asian Fusion'),
        (106, 'Curry in a Hurry', 'Indian Express'),
        (107, 'Seoul Food', 'Korean BBQ'),
        (108, 'The Pita Pit Stop', 'Mediterranean'),
        (109, 'BBQ Barn', 'Slow-cooked Brisket'),
        (110, 'Sweet Retreat', 'Desserts & Shakes')
    ) AS src(TRUCK_ID, TRUCK_BRAND_NAME, MENU_TYPE)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dcm_demo_1_dev.raw.truck t
        WHERE t.TRUCK_ID = src.TRUCK_ID
    );

    INSERT INTO dcm_demo_1_dev.raw.menu (MENU_ITEM_ID, MENU_ITEM_NAME, ITEM_CATEGORY, COST_OF_GOODS_USD, SALE_PRICE_USD)
    SELECT MENU_ITEM_ID, MENU_ITEM_NAME, ITEM_CATEGORY, COST_OF_GOODS_USD, SALE_PRICE_USD
    FROM (VALUES
        (7, 'Beef Birria Tacos', 'Tacos', 3.00, 11.50),
        (8, 'Margherita Pizza', 'Pizza', 4.50, 12.00),
        (9, 'Pad Thai', 'Noodles', 3.50, 10.00),
        (10, 'Chicken Tikka Masala', 'Curry', 4.00, 13.50),
        (11, 'Bulgogi Bowl', 'Bowls', 4.25, 12.50),
        (12, 'Lamb Gyro', 'Wraps', 4.00, 10.00),
        (13, 'Pulled Pork Slider', 'Burgers', 2.50, 8.00),
        (14, 'Chocolate Lava Cake', 'Desserts', 1.50, 6.00),
        (15, 'Iced Matcha Latte', 'Drinks', 1.20, 5.00),
        (16, 'Garlic Parmesan Wings', 'Sides', 3.00, 9.00),
        (17, 'Vegan Poke Bowl', 'Bowls', 4.00, 13.00),
        (18, 'Kimchi Fries', 'Sides', 2.50, 7.50),
        (19, 'Mango Lassi', 'Drinks', 1.00, 4.50),
        (20, 'Double Pepperoni Pizza', 'Pizza', 5.00, 14.00)
    ) AS src(MENU_ITEM_ID, MENU_ITEM_NAME, ITEM_CATEGORY, COST_OF_GOODS_USD, SALE_PRICE_USD)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dcm_demo_1_dev.raw.menu m
        WHERE m.MENU_ITEM_ID = src.MENU_ITEM_ID
    );

    INSERT INTO dcm_demo_1_dev.raw.customer (CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY)
    SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY
    FROM (VALUES
        (4, 'David', 'Miller', 'London'),
        (5, 'Eve', 'Davis', 'New York'),
        (6, 'Frank', 'Wilson', 'Chicago'),
        (7, 'Grace', 'Lee', 'San Francisco'),
        (8, 'Hank', 'Moore', 'Austin'),
        (9, 'Ivy', 'Taylor', 'London'),
        (10, 'Jack', 'Anderson', 'New York'),
        (11, 'Karen', 'Thomas', 'Chicago'),
        (12, 'Leo', 'White', 'Austin'),
        (13, 'Mia', 'Harris', 'San Francisco'),
        (14, 'Noah', 'Martin', 'London'),
        (15, 'Olivia', 'Thompson', 'New York'),
        (16, 'Paul', 'Garcia', 'Austin'),
        (17, 'Quinn', 'Martinez', 'Chicago'),
        (18, 'Rose', 'Robinson', 'London'),
        (19, 'Sam', 'Clark', 'San Francisco'),
        (20, 'Tina', 'Rodriguez', 'New York')
    ) AS src(CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dcm_demo_1_dev.raw.customer c
        WHERE c.CUSTOMER_ID = src.CUSTOMER_ID
    );

    INSERT INTO dcm_demo_1_dev.raw.inventory (ITEM_ID, REGION_ID, IN_STOCK, COUNTED_ON)
    SELECT ITEM_ID, REGION_ID, IN_STOCK, COUNTED_ON
    FROM (VALUES
        (7, 103, 50, '2023-10-27 09:00:00'), (8, 104, 40, '2023-10-27 09:00:00'),
        (9, 105, 30, '2023-10-27 09:00:00'), (10, 106, 45, '2023-10-27 09:00:00'),
        (11, 107, 35, '2023-10-27 09:00:00'), (12, 108, 60, '2023-10-27 09:00:00'),
        (13, 109, 55, '2023-10-27 09:00:00'), (14, 110, 25, '2023-10-27 09:00:00'),
        (7, 103, 42, '2023-10-28 20:00:00'), (8, 104, 35, '2023-10-28 20:00:00'),
        (9, 105, 22, '2023-10-28 20:00:00'), (10, 106, 38, '2023-10-28 20:00:00'),
        (11, 107, 28, '2023-10-28 20:00:00'), (12, 108, 45, '2023-10-28 20:00:00'),
        (15, 103, 100, '2023-10-27 08:00:00'), (16, 104, 80, '2023-10-27 08:00:00'),
        (17, 105, 40, '2023-10-27 08:00:00'), (18, 107, 90, '2023-10-27 08:00:00'),
        (19, 106, 60, '2023-10-27 08:00:00'), (20, 104, 30, '2023-10-27 08:00:00')
    ) AS src(ITEM_ID, REGION_ID, IN_STOCK, COUNTED_ON)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dcm_demo_1_dev.raw.inventory i
        WHERE i.ITEM_ID = src.ITEM_ID
            AND i.REGION_ID = src.REGION_ID
            AND i.COUNTED_ON = src.COUNTED_ON
    );

    SET order_id_offset = (
        SELECT COALESCE(MAX(ORDER_ID), 1000)
        FROM dcm_demo_1_dev.raw.order_header
    );

    INSERT INTO dcm_demo_1_dev.raw.order_header (ORDER_ID, CUSTOMER_ID, TRUCK_ID, ORDER_TS)
    SELECT $order_id_offset + ROW_NUM, CUSTOMER_ID, TRUCK_ID, CURRENT_TIMESTAMP()
    FROM (VALUES
        (1, 4, 103),  (2, 5, 104),  (3, 6, 105),  (4, 7, 106),
        (5, 8, 107),  (6, 9, 108),  (7, 10, 109), (8, 11, 110),
        (9, 12, 101), (10, 13, 102),(11, 14, 103),(12, 15, 104),
        (13, 16, 105),(14, 17, 106),(15, 18, 107),(16, 19, 108),
        (17, 20, 109),(18, 1, 110), (19, 2, 103), (20, 3, 104)
    ) AS src(ROW_NUM, CUSTOMER_ID, TRUCK_ID);

    INSERT INTO dcm_demo_1_dev.raw.order_detail (ORDER_ID, MENU_ITEM_ID, QUANTITY)
    SELECT $order_id_offset + ROW_NUM, MENU_ITEM_ID, QUANTITY
    FROM (VALUES
        (1, 7, 3),  (1, 15, 2),
        (2, 8, 1),  (2, 16, 1),
        (3, 9, 1),  (3, 18, 1),
        (4, 10, 2), (4, 19, 2),
        (5, 11, 1), (5, 18, 1),
        (6, 12, 2), (6, 3, 1),
        (7, 13, 3), (7, 5, 3),
        (8, 14, 2), (8, 15, 2),
        (9, 1, 1),  (9, 6, 1),
        (10, 2, 2), (10, 3, 2)
    ) AS src(ROW_NUM, MENU_ITEM_ID, QUANTITY);

----------------------------------------------------------------------
-- 2. Refresh Dynamic Tables
--    One ALTER statement refreshes the listed tables and their shared upstream
--    (ENRICHED_ORDER_DETAILS) at a single data timestamp.
----------------------------------------------------------------------
ALTER DYNAMIC TABLE dcm_demo_1_dev.analytics.menu_item_popularity,
                    dcm_demo_1_dev.analytics.customer_spending_summary,
                    dcm_demo_1_dev.analytics.truck_performance REFRESH;

----------------------------------------------------------------------
-- 3. Verify
----------------------------------------------------------------------
SELECT * FROM dcm_demo_1_dev.serve.v_dashboard_daily_sales;

SELECT * FROM dcm_demo_1_dev.analytics.enriched_order_details;
SELECT * FROM dcm_demo_1_dev.analytics.menu_item_popularity;
SELECT * FROM dcm_demo_1_dev.analytics.customer_spending_summary;
