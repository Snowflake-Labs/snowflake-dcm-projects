-- ### Cortex Agent over the semantic view
--
-- An agent that answers trading questions in plain language. Its one tool is
-- Cortex Analyst over TRADING_ANALYTICS, so it reads the same gold numbers,
-- with the same names, as everyone else.
--
-- The specification is inline, between $$ delimiters, because the database and
-- warehouse names carry {{env_suffix}}: Jinja renders inline specifications,
-- not specification files. A deploy updates the agent's live version.

DEFINE AGENT DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.TRADING_AGENT
    COMMENT = 'Answers questions about trading activity from the gold layer'
    SPECIFICATION = $$
models:
  orchestration: auto

instructions:
  response: |
    You are the trading assistant for a small brokerage. Answer questions
    about trades, trading value and customers. Lead with the answer and
    state the date range behind any figure.
  sample_questions:
    - question: "What was the total trading value on each day?"
    - question: "Which customers traded the most by value?"

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: trading
      description: "Completed trades by day and by customer, with counts and value."

tool_resources:
  trading:
    semantic_view: DCM_DEMO_2_FINANCE{{env_suffix}}.GOLD.TRADING_ANALYTICS
    execution_environment:
      type: warehouse
      warehouse: DCM_DEMO_2_FINANCE_WH{{env_suffix}}
$$
    PROFILE = $${"display_name": "Trading Assistant"}$$;
