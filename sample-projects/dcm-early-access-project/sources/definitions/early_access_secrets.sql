{% if include_secrets %}
define secret DCM_DEMO_1{{env_suffix}}.GOV.DEMO_API_KEY
	type = generic_string
	secret_string = {{ _snow.env_secret('DEMO_API_KEY') }}
	comment = 'Build {{ _snow.env_var("BUILD_NUMBER") | replace("\u0027", "\u0027\u0027") }}'
;


define secret DCM_DEMO_1{{env_suffix}}.GOV.DEMO_LOGIN
	type = password
	username = 'demo_service'
	password = {{ _snow.env_secret('DEMO_PASSWORD') }}
;


{% if oauth_integration %}
define secret DCM_DEMO_1{{env_suffix}}.GOV.DEMO_OAUTH
	type = oauth2
	api_authentication = {{oauth_integration}}
	oauth_refresh_token = {{ _snow.env_secret('DEMO_OAUTH_REFRESH_TOKEN') }}
;
{% endif %}


{% if cloud_token_integration %}
define secret DCM_DEMO_1{{env_suffix}}.GOV.DEMO_CLOUD_TOKEN
	type = cloud_provider_token
	api_authentication = {{cloud_token_integration}}
;
{% endif %}
{% endif %}