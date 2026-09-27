define database role DCM_DEMO_1{{env_suffix}}.DATA_READER
;


define database role DCM_DEMO_1{{env_suffix}}.DATA_EDITOR
;


grant usage on database DCM_DEMO_1{{env_suffix}} to database role DCM_DEMO_1{{env_suffix}}.DATA_READER;
grant usage on schema DCM_DEMO_1{{env_suffix}}.RAW to database role DCM_DEMO_1{{env_suffix}}.DATA_READER;
grant select on table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER to database role DCM_DEMO_1{{env_suffix}}.DATA_READER;
grant insert on table DCM_DEMO_1{{env_suffix}}.RAW.CUSTOMER to database role DCM_DEMO_1{{env_suffix}}.DATA_EDITOR;
grant database role DCM_DEMO_1{{env_suffix}}.DATA_READER to database role DCM_DEMO_1{{env_suffix}}.DATA_EDITOR;
grant database role DCM_DEMO_1{{env_suffix}}.DATA_EDITOR to role DCM_DEMO_1_ADMIN{{env_suffix}};
grant database role DCM_DEMO_1{{env_suffix}}.DATA_READER to role DCM_DEMO_1{{env_suffix}}_READ;

grant execute task on account to role DCM_DEMO_1_ADMIN{{env_suffix}};
grant manage grants on database DCM_DEMO_1{{env_suffix}} to role DCM_DEMO_1_ADMIN{{env_suffix}};
grant manage grants on schema DCM_DEMO_1{{env_suffix}}.RAW to role DCM_DEMO_1_ADMIN{{env_suffix}};
grant inherited usage on all schemas in database DCM_DEMO_1{{env_suffix}} to role DCM_DEMO_1_ADMIN{{env_suffix}};

grant select on semantic view DCM_DEMO_1{{env_suffix}}.SERVE.ORDERS_SEMANTIC_VIEW to role DCM_DEMO_1{{env_suffix}}_READ;
grant usage on streamlit DCM_DEMO_1{{env_suffix}}.SERVE.DASHBOARD to role DCM_DEMO_1{{env_suffix}}_READ;


define schema DCM_DEMO_1{{env_suffix}}.LEGACY_GRANTS
	comment = 'Isolated examples of ON ALL and ON FUTURE, pending future deprecation'
;


define table DCM_DEMO_1{{env_suffix}}.LEGACY_GRANTS.EXAMPLE (ID number)
;


grant usage on schema DCM_DEMO_1{{env_suffix}}.LEGACY_GRANTS to role DCM_DEMO_1{{env_suffix}}_READ;
grant select on all tables in schema DCM_DEMO_1{{env_suffix}}.LEGACY_GRANTS to role DCM_DEMO_1{{env_suffix}}_READ;
grant select on future tables in schema DCM_DEMO_1{{env_suffix}}.LEGACY_GRANTS to role DCM_DEMO_1{{env_suffix}}_READ;


define share DCM_DEMO_SHARE{{env_suffix}}
	comment = 'Synthetic demo data; no consumer accounts assigned'
;


grant usage on database DCM_DEMO_1{{env_suffix}} to share DCM_DEMO_SHARE{{env_suffix}};
grant usage on schema DCM_DEMO_1{{env_suffix}}.RAW to share DCM_DEMO_SHARE{{env_suffix}};
grant select on table DCM_DEMO_1{{env_suffix}}.RAW.MENU to share DCM_DEMO_SHARE{{env_suffix}};


define role DCM_DEMO_DMF_RUNNER{{env_suffix}}
;


grant role DCM_DEMO_DMF_RUNNER{{env_suffix}} to role {{project_owner_role}};
grant usage on database DCM_DEMO_1{{env_suffix}} to role DCM_DEMO_DMF_RUNNER{{env_suffix}};
grant usage on schema DCM_DEMO_1{{env_suffix}}.RAW to role DCM_DEMO_DMF_RUNNER{{env_suffix}};
grant select on table DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY to role DCM_DEMO_DMF_RUNNER{{env_suffix}};
grant execute data metric function on account to role DCM_DEMO_DMF_RUNNER{{env_suffix}};
grant usage on function DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY_SPREAD(table(number)) to role DCM_DEMO_DMF_RUNNER{{env_suffix}};