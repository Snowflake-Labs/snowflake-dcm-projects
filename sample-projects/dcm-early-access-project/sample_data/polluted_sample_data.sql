-- ### Insert "Dirty" Data (Lowercase Cities) to demo quality expectations

insert into DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER 
values
    (5001, 'Yves', 'Laurent', 'london'),
    (5002, 'Pierre', 'Cardin', 'chicago'),
    (5003, 'Jean', 'Paul', 'austin'),
    (5004, 'Marie', 'Curie', 'New York'),
    (5005, 'Victor', 'Hugo', 'London'),
    (5006, 'Coco', 'Chanel', 'Chicago'),
    (5007, 'Christian', 'Dior', 'San Francisco'),
    (5008, 'Hubert', 'Givenchy', 'Austin'),
    (5009, 'Thierry', 'Mugler', 'london'),
    (5010, 'Hedi', 'Slimane', 'chicago'),
    (5011, 'Isabel', 'Marant', 'New York'),
    (5012, 'Simon', 'Jacquemus', 'London'),
    (5013, 'Jeanne', 'Lanvin', 'Chicago'),
    (5014, 'Louis', 'Vuitton', 'San Francisco'),
    (5015, 'Azzedine', 'Alaia', 'Austin')
;

insert into DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER 
values
    (5001, 5001, 101, CURRENT_TIMESTAMP()),
    (5002, 5002, 102, CURRENT_TIMESTAMP()),
    (5003, 5003, 103, CURRENT_TIMESTAMP()),
    (5004, 5004, 104, CURRENT_TIMESTAMP()),
    (5005, 5005, 105, CURRENT_TIMESTAMP()),
    (5006, 5006, 106, CURRENT_TIMESTAMP()),
    (5007, 5007, 107, CURRENT_TIMESTAMP()),
    (5008, 5008, 108, CURRENT_TIMESTAMP()),
    (5009, 5009, 109, CURRENT_TIMESTAMP()),
    (5010, 5010, 110, CURRENT_TIMESTAMP()),
    (5011, 5011, 101, CURRENT_TIMESTAMP()),
    (5012, 5012, 102, CURRENT_TIMESTAMP()),
    (5013, 5013, 103, CURRENT_TIMESTAMP()),
    (5014, 5014, 104, CURRENT_TIMESTAMP()),
    (5015, 5015, 105, CURRENT_TIMESTAMP());

insert into DCM_DEMO_1{{env_suffix}}.RAW.ORDER_DETAIL 
values
    (5001, 1, 2),  
    (5002, 2, 1),
    (5003, 7, 3),  
    (5004, 8, 1),
    (5005, 9, 2),  
    (5006, 10, 1),
    (5007, 11, 2), 
    (5008, 12, 1),
    (5009, 13, 4), 
    (5010, 14, 2),
    (5011, 15, 3), 
    (5012, 1, 1),
    (5013, 3, 5),  
    (5014, 20, 1),
    (5015, 18, 2)
;