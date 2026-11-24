define agent DCM_DEMO_1{{env_suffix}}.SERVE.MENU_ANALYST_AGENT
	comment = 'Specification and profile from the menu_analyst asset'
	specification 
    from 'asset://menu_analyst/menu_analyst_spec.agent.yaml'
	profile 
    from 'asset://menu_analyst'
;


define agent DCM_DEMO_1{{env_suffix}}.SERVE.CITY_SALES_AGENT
	comment = 'Specification and profile inlined'
	specification = 
$$
  models:
    orchestration: auto

  instructions:
    response: |
      You are the City Sales Assistant for a fleet of food trucks. Answer
      questions about orders and revenue by customer city. Lead with the answer
      and include the date range for any figures.
    sample_questions:
      - question: "Which cities had the highest revenue?"
      - question: "How many orders were placed in each city?"

  tools:
    - tool_spec:
        type: cortex_analyst_text_to_sql
        name: orders
        description: "Food truck orders with customer city, revenue, profit, and order counts."

  tool_resources:
    orders:
      semantic_view: DCM_DEMO_1{{env_suffix}}.SERVE.ORDERS_SEMANTIC_VIEW
      execution_environment:
        type: warehouse
        warehouse: DCM_DEMO_1_WH{{env_suffix}}
$$

	profile = 
  $$ {
      "display_name": "City Sales Assistant", 
      "color": "blue"
  } $$
;
