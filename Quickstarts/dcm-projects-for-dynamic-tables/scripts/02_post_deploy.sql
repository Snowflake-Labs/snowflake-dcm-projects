/*=============================================================================
  02_post_deploy.sql — Run AFTER the first DCM Deploy succeeds
  
  Inserts sample data into raw tables, refreshes all dynamic tables,
  and runs a verification query.
=============================================================================*/

----------------------------------------------------------------------
-- 1. Insert Sample Data
----------------------------------------------------------------------
INSERT INTO dcm_demo_3_dev.raw.truck
VALUES
    (103, 'Taco Titan', 'Mexican Street Food'),
    (104, 'The Rolling Dough', 'Artisan Pizza'),
    (105, 'Wok n Roll', 'Asian Fusion'),
    (106, 'Curry in a Hurry', 'Indian Express'),
    (107, 'Seoul Food', 'Korean BBQ'),
    (108, 'The Pita Pit Stop', 'Mediterranean'),
    (109, 'BBQ Barn', 'Slow-cooked Brisket'),
    (110, 'Sweet Retreat', 'Desserts & Shakes');

INSERT INTO dcm_demo_3_dev.raw.menu
VALUES
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
    (20, 'Double Pepperoni Pizza', 'Pizza', 5.00, 14.00);

INSERT INTO dcm_demo_3_dev.raw.customer
VALUES
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
    (20, 'Tina', 'Rodriguez', 'New York');

INSERT INTO dcm_demo_3_dev.raw.inventory
VALUES
    (7, 103, 50, '2023-10-27 09:00:00'), (8, 104, 40, '2023-10-27 09:00:00'),
    (9, 105, 30, '2023-10-27 09:00:00'), (10, 106, 45, '2023-10-27 09:00:00'),
    (11, 107, 35, '2023-10-27 09:00:00'), (12, 108, 60, '2023-10-27 09:00:00'),
    (13, 109, 55, '2023-10-27 09:00:00'), (14, 110, 25, '2023-10-27 09:00:00'),
    (7, 103, 42, '2023-10-28 20:00:00'), (8, 104, 35, '2023-10-28 20:00:00'),
    (9, 105, 22, '2023-10-28 20:00:00'), (10, 106, 38, '2023-10-28 20:00:00'),
    (11, 107, 28, '2023-10-28 20:00:00'), (12, 108, 45, '2023-10-28 20:00:00'),
    (15, 103, 100, '2023-10-27 08:00:00'), (16, 104, 80, '2023-10-27 08:00:00'),
    (17, 105, 40, '2023-10-27 08:00:00'), (18, 107, 90, '2023-10-27 08:00:00'),
    (19, 106, 60, '2023-10-27 08:00:00'), (20, 104, 30, '2023-10-27 08:00:00');

INSERT INTO dcm_demo_3_dev.raw.order_header
VALUES
    (1006, 4, 103, '2023-10-28 14:00:00'), (1007, 5, 104, '2023-10-28 14:15:00'),
    (1008, 6, 105, '2023-10-28 15:30:00'), (1009, 7, 106, '2023-10-28 16:45:00'),
    (1010, 8, 107, '2023-10-28 17:00:00'), (1011, 9, 108, '2023-10-29 11:30:00'),
    (1012, 10, 109, '2023-10-29 12:00:00'), (1013, 11, 110, '2023-10-29 12:15:00'),
    (1014, 12, 101, '2023-10-29 13:00:00'), (1015, 13, 102, '2023-10-29 13:30:00'),
    (1016, 14, 103, '2023-10-29 14:00:00'), (1017, 15, 104, '2023-10-29 14:20:00'),
    (1018, 16, 105, '2023-10-29 15:00:00'), (1019, 17, 106, '2023-10-29 15:45:00'),
    (1020, 18, 107, '2023-10-29 16:10:00'), (1021, 19, 108, '2023-10-29 17:00:00'),
    (1022, 20, 109, '2023-10-30 11:00:00'), (1023, 1, 110, '2023-10-30 11:30:00'),
    (1024, 2, 103, CURRENT_TIMESTAMP()), (1025, 3, 104, CURRENT_TIMESTAMP());

INSERT INTO dcm_demo_3_dev.raw.order_detail
VALUES
    (1006, 7, 3), (1006, 15, 2),
    (1007, 8, 1), (1007, 16, 1),
    (1008, 9, 1), (1008, 18, 1),
    (1009, 10, 2), (1009, 19, 2),
    (1010, 11, 1), (1010, 18, 1),
    (1011, 12, 2), (1011, 3, 1),
    (1012, 13, 3), (1012, 5, 3),
    (1013, 14, 2), (1013, 15, 2),
    (1014, 1, 1), (1014, 6, 1),
    (1015, 2, 2), (1015, 3, 2),
    (1016, 7, 2), (1017, 8, 1),
    (1018, 9, 1), (1019, 10, 1),
    (1020, 11, 2), (1021, 12, 1),
    (1022, 13, 1), (1023, 14, 2),
    (1024, 7, 3), (1025, 8, 1);

----------------------------------------------------------------------
-- 2. Refresh Dynamic Tables
----------------------------------------------------------------------
ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.ENRICHED_ORDER_DETAILS REFRESH;
ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.MENU_ITEM_POPULARITY REFRESH;
ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.CUSTOMER_SPENDING_SUMMARY REFRESH;
ALTER DYNAMIC TABLE DCM_DEMO_3_DEV.ANALYTICS.TRUCK_PERFORMANCE REFRESH;

----------------------------------------------------------------------
-- 3. Verify
----------------------------------------------------------------------
SELECT * FROM dcm_demo_3_dev.analytics.enriched_order_details;
