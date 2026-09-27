define code bundle DCM_DEMO_1{{env_suffix}}.SERVE.NOTEBOOK_BUNDLE
	from 'asset://notebook_job/'
	comment = 'Notebook asset with runtime arguments supplied by its task'
;


define task DCM_DEMO_1{{env_suffix}}.SERVE.RUN_NOTEBOOK
	warehouse = DCM_DEMO_1_WH{{env_suffix}}
	schedule = 'USING CRON 0 6 * * * UTC'
	suspended
as
	execute code bundle DCM_DEMO_1{{env_suffix}}.SERVE.NOTEBOOK_BUNDLE
		entrypoint = 'demo.ipynb'
		arguments = ('--environment', '{{env_suffix}}')
;


define code bundle DCM_DEMO_1{{env_suffix}}.SERVE.PYTHON_BUNDLE
	from 'asset://python_job/'
	comment = 'Executable Python script on warehouse compute'
;


define task DCM_DEMO_1{{env_suffix}}.SERVE.RUN_PYTHON
	warehouse = DCM_DEMO_1_WH{{env_suffix}}
	schedule = '60 MINUTE'
	suspended
as
	execute code bundle DCM_DEMO_1{{env_suffix}}.SERVE.PYTHON_BUNDLE
		entrypoint = 'main.py'
		arguments = ('--environment', '{{env_suffix}}')
;