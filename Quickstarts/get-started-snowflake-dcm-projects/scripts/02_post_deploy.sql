/*=============================================================================
  02_post_deploy.sql — Run AFTER the first successful deployment

  Seeds the three raw tables, refreshes the dynamic table, and verifies the
  pipeline output through the semantic view.

  Safe to re-run: the MENU insert skips rows that already exist, and the order
  inserts use a computed offset so each run appends 10 fresh orders without
  colliding with previous runs.

  No BEGIN...END block anywhere in this file — `snow sql -f` splits a script at
  semicolons, which would break the block apart. A session variable does the
  same job and survives the split.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Seed the MENU dimension
----------------------------------------------------------------------
USE ROLE dcm_developer;

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

----------------------------------------------------------------------
-- 2. Seed 10 fresh orders
--
-- The offset is a session variable, referenced below as $order_id_offset.
----------------------------------------------------------------------
SET order_id_offset = (
    SELECT COALESCE(MAX(ORDER_ID), 1000)
    FROM dcm_demo_1_dev.raw.order_header
);

INSERT INTO dcm_demo_1_dev.raw.order_header (ORDER_ID, ORDER_TS)
SELECT $order_id_offset + ROW_NUM, CURRENT_TIMESTAMP()
FROM (VALUES
    (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
) AS src(ROW_NUM);

INSERT INTO dcm_demo_1_dev.raw.order_detail (ORDER_ID, MENU_ITEM_ID, QUANTITY)
SELECT $order_id_offset + ROW_NUM, MENU_ITEM_ID, QUANTITY
FROM (VALUES
    (1, 7, 3),   (1, 15, 2),
    (2, 8, 1),   (2, 16, 1),
    (3, 9, 1),   (3, 18, 1),
    (4, 10, 2),  (4, 19, 2),
    (5, 11, 1),  (5, 18, 1),
    (6, 12, 2),  (6, 13, 1),
    (7, 13, 3),  (7, 20, 3),
    (8, 14, 2),  (8, 15, 2),
    (9, 17, 1),  (9, 12, 1),
    (10, 20, 2), (10, 8, 2)
) AS src(ROW_NUM, MENU_ITEM_ID, QUANTITY);

----------------------------------------------------------------------
-- 3. Refresh the dynamic table
--
-- Refreshing the dynamic tables directly, rather than with
-- EXECUTE DCM PROJECT ... REFRESH ALL, so each target is explicit. Add one
-- ALTER per dynamic table as the pipeline grows.
----------------------------------------------------------------------
ALTER DYNAMIC TABLE dcm_demo_1_dev.analytics.enriched_order_details REFRESH;

----------------------------------------------------------------------
-- 4. Verify
----------------------------------------------------------------------
SELECT * FROM dcm_demo_1_dev.analytics.enriched_order_details LIMIT 20;

-- Query the semantic view by naming the dimensions and metrics you want. The
-- semantic view resolves the aggregation for you.
SELECT * FROM SEMANTIC_VIEW(
    dcm_demo_1_dev.serve.order_analytics
    DIMENSIONS order_lines.ITEM_CATEGORY
    METRICS order_lines.TOTAL_REVENUE, order_lines.TOTAL_PROFIT, order_lines.ORDER_COUNT
)
ORDER BY TOTAL_REVENUE DESC;
