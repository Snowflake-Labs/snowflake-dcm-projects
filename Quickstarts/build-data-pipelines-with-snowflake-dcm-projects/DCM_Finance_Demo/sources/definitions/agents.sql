-- ### Cortex Agent over the semantic view
--
-- An agent that answers trading questions in plain language. Its one tool is
-- Cortex Analyst over TRADING_ANALYTICS, so it reads the same gold numbers,
-- with the same names, as everyone else.
--
-- The specification is a YAML file in the agents/ folder, the same format
-- Agent Studio reads and writes, so it can be edited there and copied back.
-- Specification files are not rendered by Jinja, so each target has its own
-- file with literal object names; agent_spec in manifest.yml picks the file.
-- A deploy updates the agent's live version.

DEFINE AGENT DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.TRADING_AGENT
    COMMENT = 'Answers questions about trading activity from the gold layer'
    SPECIFICATION
        FROM 'asset://trading_agent/{{agent_spec}}.yaml'
    PROFILE
        FROM 'asset://agent_profile';
