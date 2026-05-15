-- internal stage
define stage DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE 
    directory = ( enable = true )
    comment = 'Internal stage for daily incoming Tasty Bytes order files (CSV)'
;

-- external stage
define stage DCM_DEMO_1{{env_suffix}}.RAW.PUBLIC_S3_BUCKET
    URL = 's3://sfquickstarts/'
    file_format = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV'
    comment = 'For demo purposes only. no imports from here.'      
;


define table DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING (
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    TRUCK_ID NUMBER,
    ORDER_TS TIMESTAMP_NTZ,
    MENU_ITEM_ID NUMBER,
    QUANTITY NUMBER
)
change_tracking = TRUE
;


define procedure DCM_DEMO_1{{env_suffix}}.RAW.SP_UPSERT_FROM_INCOMING(
    TARGET_TABLE STRING,
    COLUMN_NAMES STRING,
    DEDUP_KEY STRING DEFAULT NULL
)
returns STRING
language SQL
as
begin
    let stmt STRING;
    if (:DEDUP_KEY is not null) then
        stmt := 'INSERT INTO ' || :TARGET_TABLE || ' (' || :COLUMN_NAMES || ')' ||
                ' SELECT DISTINCT ' || :COLUMN_NAMES ||
                ' FROM DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING src' ||
                ' WHERE NOT EXISTS (SELECT 1 FROM ' || :TARGET_TABLE || ' dest' ||
                ' WHERE dest.' || :DEDUP_KEY || ' = src.' || :DEDUP_KEY || ')';
    else
        stmt := 'INSERT INTO ' || :TARGET_TABLE || ' (' || :COLUMN_NAMES || ')' ||
                ' SELECT ' || :COLUMN_NAMES ||
                ' FROM DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING';
    end if;
    execute immediate :stmt;
    return 'Inserted into ' || :TARGET_TABLE;
end;


define file format DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = true
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;



define task DCM_DEMO_1{{env_suffix}}.RAW.INGEST_DAILY_ORDERS
    warehouse = 'DCM_DEMO_1_WH{{env_suffix}}'
    after DCM_DEMO_1{{env_suffix}}.RAW.INSERT_SAMPLE_DATA
    STARTED
as
begin
    copy into 
        DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING
    from 
        @DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE
    file_format = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV'
    ON_ERROR = 'CONTINUE';


    call DCM_DEMO_1{{env_suffix}}.RAW.SP_UPSERT_FROM_INCOMING(
        'DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER',
        'ORDER_ID, CUSTOMER_ID, TRUCK_ID, ORDER_TS',
        'ORDER_ID'
    );

    call DCM_DEMO_1{{env_suffix}}.RAW.SP_UPSERT_FROM_INCOMING(
        'DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL',
        'ORDER_ID, MENU_ITEM_ID, QUANTITY'
    );

    truncate table DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING;
    
    remove @DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE pattern='.*.csv';
end;



define task DCM_DEMO_1{{env_suffix}}.RAW.INSERT_SAMPLE_DATA
    warehouse = 'DCM_DEMO_1_WH{{env_suffix}}'
    schedule = 'USING CRON 0 5 * * * UTC'
    STARTED
as
begin
    
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

end;