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
{% endif %}