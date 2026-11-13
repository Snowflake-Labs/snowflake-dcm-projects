/*=============================================================================
  02_post_deploy.sql: Run AFTER the first DCM Deploy succeeds, as DCM_DEVELOPER

  Inserts sample data into the raw tables, refreshes the dynamic tables, and
  runs a verification query, and resumes the refresh-failure alert.
=============================================================================*/

USE ROLE DCM_DEVELOPER;
USE WAREHOUSE DCM_DEMO_3_WH_DEV;

----------------------------------------------------------------------
-- 1. Insert sample data
----------------------------------------------------------------------
INSERT INTO DCM_DEMO_3_DEV.RAW.MENU VALUES
    (7, 'Beef Birria Tacos', 'Tacos', 3.00, 11.50),       (8, 'Margherita Pizza', 'Pizza', 4.50, 12.00),
    (9, 'Pad Thai', 'Noodles', 3.50, 10.00),              (10, 'Chicken Tikka Masala', 'Curry', 4.00, 13.50),
    (11, 'Bulgogi Bowl', 'Bowls', 4.25, 12.50),           (12, 'Lamb Gyro', 'Wraps', 4.00, 10.00),
    (13, 'Pulled Pork Slider', 'Burgers', 2.50, 8.00),    (14, 'Chocolate Lava Cake', 'Desserts', 1.50, 6.00),
    (15, 'Iced Matcha Latte', 'Drinks', 1.20, 5.00),      (16, 'Garlic Parmesan Wings', 'Sides', 3.00, 9.00),
    (17, 'Vegan Poke Bowl', 'Bowls', 4.00, 13.00),        (18, 'Kimchi Fries', 'Sides', 2.50, 7.50),
    (19, 'Mango Lassi', 'Drinks', 1.00, 4.50),            (20, 'Double Pepperoni Pizza', 'Pizza', 5.00, 14.00);

INSERT INTO DCM_DEMO_3_DEV.RAW.CUSTOMER VALUES
    (1, 'Alice', 'Johnson', 'San Francisco'), (2, 'Bob', 'Smith', 'New York'),        (3, 'Charlie', 'Brown', 'Chicago'),
    (4, 'David', 'Miller', 'London'),         (5, 'Eve', 'Davis', 'New York'),        (6, 'Frank', 'Wilson', 'Chicago'),
    (7, 'Grace', 'Lee', 'San Francisco'),     (8, 'Hank', 'Moore', 'Austin'),         (9, 'Ivy', 'Taylor', 'London'),
    (10, 'Jack', 'Anderson', 'New York'),     (11, 'Karen', 'Thomas', 'Chicago'),     (12, 'Leo', 'White', 'Austin'),
    (13, 'Mia', 'Harris', 'San Francisco'),   (14, 'Noah', 'Martin', 'London'),       (15, 'Olivia', 'Thompson', 'New York'),
    (16, 'Paul', 'Garcia', 'Austin'),         (17, 'Quinn', 'Martinez', 'Chicago'),   (18, 'Rose', 'Robinson', 'London'),
    (19, 'Sam', 'Clark', 'San Francisco'),    (20, 'Tina', 'Rodriguez', 'New York');

-- One row per order line: ORDER_ID, ORDER_TS, CUSTOMER_ID, TRUCK_BRAND_NAME, MENU_ITEM_ID, QUANTITY.
-- Every order here is from October 2023. 03_verify_evolution.sql adds two
-- orders stamped with CURRENT_TIMESTAMP() later.
INSERT INTO DCM_DEMO_3_DEV.RAW.ORDERS VALUES
    (1006, '2023-10-28 14:00:00', 4, 'Taco Titan', 7, 3),          (1006, '2023-10-28 14:00:00', 4, 'Taco Titan', 15, 2),
    (1007, '2023-10-28 14:15:00', 5, 'The Rolling Dough', 8, 1),   (1007, '2023-10-28 14:15:00', 5, 'The Rolling Dough', 16, 1),
    (1008, '2023-10-28 15:30:00', 6, 'Wok n Roll', 9, 1),          (1008, '2023-10-28 15:30:00', 6, 'Wok n Roll', 18, 1),
    (1009, '2023-10-28 16:45:00', 7, 'Curry in a Hurry', 10, 2),   (1009, '2023-10-28 16:45:00', 7, 'Curry in a Hurry', 19, 2),
    (1010, '2023-10-28 17:00:00', 8, 'Seoul Food', 11, 1),         (1010, '2023-10-28 17:00:00', 8, 'Seoul Food', 18, 1),
    (1011, '2023-10-29 11:30:00', 9, 'The Pita Pit Stop', 12, 2),  (1012, '2023-10-29 12:00:00', 10, 'BBQ Barn', 13, 3),
    (1013, '2023-10-29 12:15:00', 11, 'Sweet Retreat', 14, 2),     (1013, '2023-10-29 12:15:00', 11, 'Sweet Retreat', 15, 2),
    (1014, '2023-10-29 13:00:00', 12, 'Wok n Roll', 17, 1),        (1015, '2023-10-29 13:30:00', 13, 'The Rolling Dough', 20, 2),
    (1016, '2023-10-29 14:00:00', 14, 'Taco Titan', 7, 2),         (1017, '2023-10-29 14:20:00', 15, 'The Rolling Dough', 8, 1),
    (1018, '2023-10-29 15:00:00', 16, 'Wok n Roll', 9, 1),         (1019, '2023-10-29 15:45:00', 17, 'Curry in a Hurry', 10, 1),
    (1020, '2023-10-29 16:10:00', 18, 'Seoul Food', 11, 2),        (1021, '2023-10-29 17:00:00', 19, 'The Pita Pit Stop', 12, 1),
    (1022, '2023-10-30 11:00:00', 20, 'BBQ Barn', 13, 1),          (1023, '2023-10-30 11:30:00', 1, 'Sweet Retreat', 14, 2);

----------------------------------------------------------------------
-- 2. Refresh the dynamic tables
--
-- One statement refreshes the two leaf tables and their shared upstream
-- (ENRICHED_ORDER_DETAILS) at a single data timestamp.
----------------------------------------------------------------------
ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.CUSTOMER_SPENDING_SUMMARY,
                    DCM_DEMO_3_DEV.ANALYTICS.TRUCK_PERFORMANCE REFRESH;

----------------------------------------------------------------------
-- 3. Verify
----------------------------------------------------------------------
SELECT * FROM DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS ORDER BY ORDER_TS DESC, ORDER_ID;

----------------------------------------------------------------------
-- 4. Resume the alert
--
-- Template alerts cannot be declared STARTED, so DCM deploys this one suspended.
----------------------------------------------------------------------
ALTER ALERT DCM_DEMO_3_DEV.ANALYTICS.DT_REFRESH_FAILURE RESUME;
