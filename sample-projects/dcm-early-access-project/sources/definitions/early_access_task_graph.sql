define schema DCM_DEMO_1{{env_suffix}}.ORCHESTRATION
;


define task DCM_DEMO_1{{env_suffix}}.ORCHESTRATION.CHECK_CUSTOMERS
as
select
	count(*) as CUSTOMER_COUNT
from
	DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
;


define task DCM_DEMO_1{{env_suffix}}.ORCHESTRATION.CHECK_ORDERS
	after DCM_DEMO_1{{env_suffix}}.ORCHESTRATION.CHECK_CUSTOMERS
as
select
	count(*) as ORDER_COUNT
from
	DCM_DEMO_1{{env_suffix}}.RAW.ORDER_HEADER
;


define task graph DCM_DEMO_1{{env_suffix}}.ORCHESTRATION.ORDER_CHECKS
	schedule = '60 MINUTE'
	roots = (DCM_DEMO_1{{env_suffix}}.ORCHESTRATION.CHECK_CUSTOMERS)
	task_defaults (
		warehouse = 'DCM_DEMO_1_WH{{env_suffix}}',
		log_level = 'INFO'
	)
	suspended
;