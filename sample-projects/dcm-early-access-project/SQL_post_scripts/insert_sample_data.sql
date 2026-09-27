-- ### Insert Sample Data

insert into DCM_DEMO_1{{env_suffix}}.RAW.TRUCK 
values
    (103, 'Taco Titan', 'Mexican Street Food'),
    (104, 'The Rolling Dough', 'Artisan Pizza'),
    (105, 'Wok n Roll', 'Asian Fusion'),
    (106, 'Curry in a Hurry', 'Indian Express'),
    (107, 'Seoul Food', 'Korean BBQ'),
    (108, 'The Pita Pit Stop', 'Mediterranean'),
    (109, 'BBQ Barn', 'Slow-cooked Brisket'),
    (110, 'Sweet Retreat', 'Desserts & Shakes');

insert into DCM_DEMO_1{{env_suffix}}.RAW.MENU 
values
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

insert into DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER 
values
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

insert into DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY 
values
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

insert into DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER 
values
    (1006, 4, 103, '2023-10-28 14:00:00'), (1007, 5, 104, '2023-10-28 14:15:00'),
    (1008, 6, 105, '2023-10-28 15:30:00'), (1009, 7, 106, '2023-10-28 16:45:00'),
    (1010, 8, 107, '2023-10-28 17:00:00'), (1011, 9, 108, '2023-10-29 11:30:00'),
    (1012, 10, 109, '2023-10-29 12:00:00'), (1013, 11, 110, '2023-10-29 12:15:00'),
    (1014, 12, 101, '2023-10-29 13:00:00'), (1015, 13, 102, '2023-10-29 13:30:00'),
    (1016, 14, 103, '2023-10-29 14:00:00'), (1017, 15, 104, '2023-10-29 14:20:00'),
    (1018, 16, 105, '2023-10-29 15:00:00'), (1019, 17, 106, '2023-10-29 15:45:00'),
    (1020, 18, 107, '2023-10-29 16:10:00'), (1021, 19, 108, '2023-10-29 17:00:00'),
    (1022, 20, 109, '2023-10-30 11:00:00'), (1023, 1, 110, '2023-10-30 11:30:00'),
    (1024, 2, 103, '2023-10-30 12:15:00'), (1025, 3, 104, '2023-10-30 13:00:00');

insert into DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL 
values
    (1006, 7, 3), (1006, 15, 2), -- 3 Tacos, 2 Matcha
    (1007, 8, 1), (1007, 16, 1), -- Pizza & Wings
    (1008, 9, 1), (1008, 18, 1), -- Pad Thai & Kimchi Fries
    (1009, 10, 2), (1009, 19, 2), -- Curry & Lassi
    (1010, 11, 1), (1010, 18, 1), -- Bulgogi & Fries
    (1011, 12, 2), (1011, 3, 1),  -- Gyro & Truffle Fries
    (1012, 13, 3), (1012, 5, 3),  -- Sliders & Coffee
    (1013, 14, 2), (1013, 15, 2), -- Lava Cake & Matcha
    (1014, 1, 1), (1014, 6, 1),   -- Falafel & Chicken Gyro
    (1015, 2, 2), (1015, 3, 2);   -- Burgers & Fries



-- ### Insert "Dirty" Data (Lowercase Cities) to demo quality expectations

-- insert into DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER 
-- values
--     (5001, 'Yves', 'Laurent', 'london'),
--     (5002, 'Pierre', 'Cardin', 'chicago'),
--     (5003, 'Jean', 'Paul', 'austin'),
--     (5004, 'Marie', 'Curie', 'New York'),
--     (5005, 'Victor', 'Hugo', 'London'),
--     (5006, 'Coco', 'Chanel', 'Chicago'),
--     (5007, 'Christian', 'Dior', 'San Francisco'),
--     (5008, 'Hubert', 'Givenchy', 'Austin'),
--     (5009, 'Thierry', 'Mugler', 'london'),
--     (5010, 'Hedi', 'Slimane', 'chicago'),
--     (5011, 'Isabel', 'Marant', 'New York'),
--     (5012, 'Simon', 'Jacquemus', 'London'),
--     (5013, 'Jeanne', 'Lanvin', 'Chicago'),
--     (5014, 'Louis', 'Vuitton', 'San Francisco'),
--     (5015, 'Azzedine', 'Alaia', 'Austin');

-- insert into DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER 
-- values
--     (5001, 5001, 101, CURRENT_TIMESTAMP()),
--     (5002, 5002, 102, CURRENT_TIMESTAMP()),
--     (5003, 5003, 103, CURRENT_TIMESTAMP()),
--     (5004, 5004, 104, CURRENT_TIMESTAMP()),
--     (5005, 5005, 105, CURRENT_TIMESTAMP()),
--     (5006, 5006, 106, CURRENT_TIMESTAMP()),
--     (5007, 5007, 107, CURRENT_TIMESTAMP()),
--     (5008, 5008, 108, CURRENT_TIMESTAMP()),
--     (5009, 5009, 109, CURRENT_TIMESTAMP()),
--     (5010, 5010, 110, CURRENT_TIMESTAMP()),
--     (5011, 5011, 101, CURRENT_TIMESTAMP()),
--     (5012, 5012, 102, CURRENT_TIMESTAMP()),
--     (5013, 5013, 103, CURRENT_TIMESTAMP()),
--     (5014, 5014, 104, CURRENT_TIMESTAMP()),
--     (5015, 5015, 105, CURRENT_TIMESTAMP());

-- insert into DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL 
-- values
--     (5001, 1, 2),  
--     (5002, 2, 1),
--     (5003, 7, 3),  
--     (5004, 8, 1),
--     (5005, 9, 2),  
--     (5006, 10, 1),
--     (5007, 11, 2), 
--     (5008, 12, 1),
--     (5009, 13, 4), 
--     (5010, 14, 2),
--     (5011, 15, 3), 
--     (5012, 1, 1),
--     (5013, 3, 5),  
--     (5014, 20, 1),
--     (5015, 18, 2);