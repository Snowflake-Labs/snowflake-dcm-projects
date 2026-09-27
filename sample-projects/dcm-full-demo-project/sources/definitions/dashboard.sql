define streamlit DCM_DEMO_1{{env_suffix}}.SERVE.DASHBOARD
	from 'asset://dashboard/'
	main_file = 'streamlit_app.py'
	query_warehouse = DCM_DEMO_1_WH{{env_suffix}}
	compute_pool = {{compute_pool}}
	runtime_name = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'
	title = 'DCM sales dashboard'
	external_access_integrations = ()
	imports = ()
;