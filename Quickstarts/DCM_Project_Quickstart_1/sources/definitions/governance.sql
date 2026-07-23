define schema DCM_DEMO_1{{env_suffix}}.GOV;

-- Network policy demo: this only CREATES the objects; it does NOT activate them.
-- Existing account/user-level network policies are unaffected.
define network rule DCM_DEMO_1{{env_suffix}}.GOV.ALLOW_ALL_IPS
    MODE = INGRESS
    TYPE = IPV4
    VALUE_LIST = ('0.0.0.0/0')
;

define network policy DCM_DEMO_1{{env_suffix}}_POLICY
    allowed_network_rule_list = ('DCM_DEMO_1{{env_suffix}}.GOV.ALLOW_ALL_IPS')
    comment = 'Demo-only policy - allows all IPs, never auto-activated'
;

define authentication policy DCM_DEMO_1{{env_suffix}}.GOV.GITHUB_AUTH_POLICY
    authentication_methods = ('PROGRAMMATIC_ACCESS_TOKEN')
    pat_policy = ( 
        default_expiry_in_days=15,
        max_expiry_in_days=90,
        network_policy_evaluation = ENFORCED_NOT_REQUIRED
    )
;


define masking policy DCM_DEMO_1{{env_suffix}}.GOV.EMAIL_MASK
    as (VAL string) returns string ->
    case
        when current_role() in ('ACCOUNTADMIN', 'DCM_ADMIN') then VAL
        when current_role() in ('DCM_DEVELOPER') then regexp_replace(VAL, '.+\\@', '*****@')
        else '***MASKED***'
    end
    comment = 'Masks email addresses for non-privileged roles'
;

---------------------------------

define tag DCM_DEMO_1{{env_suffix}}.GOV.PII
    allowed_values 'PII'
;

define tag DCM_DEMO_1{{env_suffix}}.GOV.DATA_DOMAIN
    allowed_values 'SALES', 'MARKETING', 'FINANCE', 'HR', 'CUSTOMER'
;
   
attach tag DCM_DEMO_1{{env_suffix}}.GOV.PII = 'PII'
    to table DCM_DEMO_1{{env_suffix}}.ANALYTICS.ENRICHED_ORDER_DETAILS
        column CUSTOMER_CITY
;

attach tag DCM_DEMO_1{{env_suffix}}.GOV.DATA_DOMAIN = 'CUSTOMER'
    to table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER
;
