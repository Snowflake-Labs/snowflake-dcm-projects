define api integration DCM_DEMO_1_GITHUB_API{{env_suffix}}
	api_provider = git_https_api
	api_allowed_prefixes = ('https://github.com')
	allowed_authentication_secrets = all
	enabled = true
;


define network rule DCM_DEMO_1{{env_suffix}}.GOV.EXAMPLE_API_EGRESS
	mode = egress
	type = host_port
	value_list = ('example.com:443')
;


define external access integration DCM_DEMO_1_EXTERNAL_ACCESS{{env_suffix}}
	allowed_network_rules = (DCM_DEMO_1{{env_suffix}}.GOV.EXAMPLE_API_EGRESS)
	allowed_authentication_secrets = none
	enabled = true
	comment = 'Demo external access to example.com over HTTPS without authentication secrets'
;


define storage integration DCM_DEMO_1_S3_STORAGE{{env_suffix}}
	type = external_stage
	storage_provider = 'S3'
	storage_aws_role_arn = 'arn:aws:iam::123456789012:role/dcm-demo-storage'
	enabled = true
	storage_allowed_locations = ('s3://example-dcm-demo/data/')
	comment = 'Demo with placeholder AWS role and bucket; configure real locations and IAM trust before accessing S3'
;


-- Uses the storage integration; no files are listed or loaded during deployment
define stage DCM_DEMO_1{{env_suffix}}.RAW.S3_INTEGRATION_STAGE
	url = 's3://example-dcm-demo/data/'
	storage_integration = DCM_DEMO_1_S3_STORAGE{{env_suffix}}
	file_format = 'DCM_DEMO_1{{env_suffix}}.RAW.DCM_DEMO_CSV'
	comment = 'Demo only; requires real bucket and IAM trust before use'
;


-- Uses the external access integration; reaches example.com only when called
define procedure DCM_DEMO_1{{env_suffix}}.RAW.SP_CHECK_EXAMPLE_API()
returns STRING
language PYTHON
runtime_version = '3.11'
packages = ('snowflake-snowpark-python', 'requests')
handler = 'check_example_api'
external_access_integrations = (DCM_DEMO_1_EXTERNAL_ACCESS{{env_suffix}})
as
$$
def check_example_api(session):
    import requests

    response = requests.get("https://example.com", timeout=10)
    return f"HTTP {response.status_code}"
$$
;
