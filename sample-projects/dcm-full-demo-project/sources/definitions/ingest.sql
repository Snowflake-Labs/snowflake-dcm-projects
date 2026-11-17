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


-- Inserts new order lines from the stream into a target table
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
                ' FROM DCM_DEMO_1{{env_suffix}}.RAW.INCOMING_ORDERS_STREAM src' ||
                ' WHERE NOT EXISTS (SELECT 1 FROM ' || :TARGET_TABLE || ' dest' ||
                ' WHERE dest.' || :DEDUP_KEY || ' = src.' || :DEDUP_KEY || ')';
    else
        stmt := 'INSERT INTO ' || :TARGET_TABLE || ' (' || :COLUMN_NAMES || ')' ||
                ' SELECT ' || :COLUMN_NAMES ||
                ' FROM DCM_DEMO_1{{env_suffix}}.RAW.INCOMING_ORDERS_STREAM';
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


define stream DCM_DEMO_1{{env_suffix}}.RAW.INCOMING_ORDERS_STREAM
    on table DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING
    append_only = TRUE
    comment = 'Captures order lines landing in DAILY_ORDERS_INCOMING'
;



-- Task graph: owns schedule, warehouse, and desired state for its member tasks
define task graph DCM_DEMO_1{{env_suffix}}.RAW.ORDERS_PIPELINE
    schedule = 'USING CRON 0 5 * * * UTC'
    roots = (
        DCM_DEMO_1{{env_suffix}}.RAW.INSERT_SAMPLE_DATA
    )
    task_defaults (
        warehouse = 'DCM_DEMO_1_WH{{env_suffix}}',
        comment = 'Daily sample data, ingest, and upsert pipeline'
    )
    STARTED
;



define task DCM_DEMO_1{{env_suffix}}.RAW.INGEST_DAILY_ORDERS
    after DCM_DEMO_1{{env_suffix}}.RAW.INSERT_SAMPLE_DATA
as
begin
    copy into 
        DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING
    from 
        @DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE
    file_format = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV'
    ON_ERROR = 'CONTINUE';

    remove @DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE pattern='.*.csv';
end;



define task DCM_DEMO_1{{env_suffix}}.RAW.PROCESS_INCOMING_ORDERS
    after DCM_DEMO_1{{env_suffix}}.RAW.INGEST_DAILY_ORDERS
    when
        SYSTEM$STREAM_HAS_DATA('DCM_DEMO_1{{env_suffix}}.RAW.INCOMING_ORDERS_STREAM')
as
begin
    -- Explicit transaction so both upserts read the same stream contents
    begin transaction;

    call DCM_DEMO_1{{env_suffix}}.RAW.SP_UPSERT_FROM_INCOMING(
        'DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER',
        'ORDER_ID, CUSTOMER_ID, TRUCK_ID, ORDER_TS',
        'ORDER_ID'
    );

    call DCM_DEMO_1{{env_suffix}}.RAW.SP_UPSERT_FROM_INCOMING(
        'DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL',
        'ORDER_ID, MENU_ITEM_ID, QUANTITY'
    );

    commit;
end;



define task DCM_DEMO_1{{env_suffix}}.RAW.INSERT_SAMPLE_DATA
as
begin
    insert into DCM_DEMO_1{{env_suffix}}.RAW.TRUCK (TRUCK_ID, TRUCK_BRAND_NAME, MENU_TYPE)
    select TRUCK_ID, TRUCK_BRAND_NAME, MENU_TYPE
    from (values
        (101, 'Freezing Point', 'Ice Cream'),
        (102, 'Smoky BBQ', 'BBQ'),
        (103, 'Taco Titan', 'Mexican Street Food'),
        (104, 'The Rolling Dough', 'Artisan Pizza'),
        (105, 'Wok n Roll', 'Asian Fusion'),
        (106, 'Curry in a Hurry', 'Indian Express'),
        (107, 'Seoul Food', 'Korean BBQ'),
        (108, 'The Pita Pit Stop', 'Mediterranean'),
        (109, 'BBQ Barn', 'Slow-cooked Brisket'),
        (110, 'Sweet Retreat', 'Desserts & Shakes')
    ) as src(TRUCK_ID, TRUCK_BRAND_NAME, MENU_TYPE)
    where not exists (
        select 1
        from DCM_DEMO_1{{env_suffix}}.RAW.TRUCK t
        where t.TRUCK_ID = src.TRUCK_ID
    );

    insert into DCM_DEMO_1{{env_suffix}}.RAW.MENU (MENU_ITEM_ID, MENU_ITEM_NAME, ITEM_CATEGORY, COST_OF_GOODS_USD, SALE_PRICE_USD)
    select MENU_ITEM_ID, MENU_ITEM_NAME, ITEM_CATEGORY, COST_OF_GOODS_USD, SALE_PRICE_USD
    from (values
        (1, 'Falafel Wrap', 'Wraps', 2.50, 9.00),
        (2, 'Classic Burger', 'Burgers', 3.50, 11.00),
        (3, 'Truffle Fries', 'Sides', 1.50, 6.00),
        (5, 'Coffee', 'Drinks', 0.75, 3.00),
        (6, 'Chicken Gyro', 'Wraps', 3.00, 10.00),
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
    ) as src(MENU_ITEM_ID, MENU_ITEM_NAME, ITEM_CATEGORY, COST_OF_GOODS_USD, SALE_PRICE_USD)
    where not exists (
        select 1
        from DCM_DEMO_1{{env_suffix}}.RAW.MENU m
        where m.MENU_ITEM_ID = src.MENU_ITEM_ID
    );

    insert into DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER (CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY)
    select CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY
    from (values
        (1, 'Alice', 'Smith', 'London'),
        (2, 'Bob', 'Johnson', 'New York'),
        (3, 'Charlie', 'Brown', 'Chicago'),
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
    ) as src(CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY)
    where not exists (
        select 1
        from DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER c
        where c.CUSTOMER_ID = src.CUSTOMER_ID
    );

    insert into DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY (ITEM_ID, REGION_ID, IN_STOCK, COUNTED_ON)
    select ITEM_ID, REGION_ID, IN_STOCK, COUNTED_ON
    from (values
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
    ) as src(ITEM_ID, REGION_ID, IN_STOCK, COUNTED_ON)
    where not exists (
        select 1
        from DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY i
        where i.ITEM_ID = src.ITEM_ID
            and i.REGION_ID = src.REGION_ID
            and i.COUNTED_ON = src.COUNTED_ON
    );

    -- Land new order lines for the stream to pick up
    let order_id_offset NUMBER := (
        select greatest(
            coalesce((select max(ORDER_ID) from DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER), 1000),
            coalesce((select max(ORDER_ID) from DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING), 1000)
        )
    );

    insert into DCM_DEMO_1{{env_suffix}}.RAW.DAILY_ORDERS_INCOMING (ORDER_ID, CUSTOMER_ID, TRUCK_ID, ORDER_TS, MENU_ITEM_ID, QUANTITY)
    select :order_id_offset + ROW_NUM, CUSTOMER_ID, TRUCK_ID, current_timestamp(), MENU_ITEM_ID, QUANTITY
    from (values
        (1, 4, 103, 7, 3),   (1, 4, 103, 15, 2),
        (2, 5, 104, 8, 1),   (2, 5, 104, 16, 1),
        (3, 6, 105, 9, 1),   (3, 6, 105, 18, 1),
        (4, 7, 106, 10, 2),  (4, 7, 106, 19, 2),
        (5, 8, 107, 11, 1),  (5, 8, 107, 18, 1),
        (6, 9, 108, 12, 2),  (6, 9, 108, 3, 1),
        (7, 10, 109, 13, 3), (7, 10, 109, 5, 3),
        (8, 11, 110, 14, 2), (8, 11, 110, 15, 2),
        (9, 12, 101, 1, 1),  (9, 12, 101, 6, 1),
        (10, 13, 102, 2, 2), (10, 13, 102, 3, 2)
    ) as src(ROW_NUM, CUSTOMER_ID, TRUCK_ID, MENU_ITEM_ID, QUANTITY);

end;


define table DCM_DEMO_1{{env_suffix}}.RAW.PIPE_ORDERS (
	ORDER_ID number,
	CUSTOMER_ID number,
	TRUCK_ID number,
	ORDER_TS timestamp_ntz,
	MENU_ITEM_ID number,
	QUANTITY number
)
;


define pipe DCM_DEMO_1{{env_suffix}}.RAW.ORDERS_PIPE
	auto_ingest = false
as
	copy into DCM_DEMO_1{{env_suffix}}.RAW.PIPE_ORDERS
	from @DCM_DEMO_1{{env_suffix}}.RAW.PUBLIC_S3_BUCKET/dcm_sample_orders/
	file_format = (format_name = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV')
;


define view DCM_DEMO_1{{env_suffix}}.RAW.V_CUSTOMER_CHANGES
	change_tracking = true
as
select
	CUSTOMER_ID,
	CITY
from
	DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER_VIEW_STREAM
	on view DCM_DEMO_1{{env_suffix}}.RAW.V_CUSTOMER_CHANGES
;


define stream DCM_DEMO_1{{env_suffix}}.RAW.ORDER_FILES_STREAM
	on stage DCM_DEMO_1{{env_suffix}}.RAW.TASTY_BYTES_ORDERS_STAGE
;