define table DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_LOYALTY_TIERS (
	CUSTOMER_ID NUMBER,
	FIRST_NAME VARCHAR,
	LAST_NAME VARCHAR,
	TOTAL_SPEND_USD NUMBER(10, 2),
	TOTAL_ORDERS NUMBER,
	LOYALTY_TIER VARCHAR
)
;


-- Java UDF: produces a branded order reference code, e.g. TB-000042-T007
define function DCM_DEMO_1{{env_suffix}}.ANALYTICS.FORMAT_ORDER_REFERENCE(
	ORDER_ID NUMBER,
	TRUCK_ID NUMBER
)
returns VARCHAR
language JAVA
runtime_version = '11'
handler = 'OrderReferenceFormatter.format'
as
$$
class OrderReferenceFormatter {
    public static String format(long orderId, long truckId) {
        return String.format("TB-%06d-T%03d", orderId, truckId);
    }
}
$$
;


-- Python procedure: classifies customers into loyalty tiers and persists results
define procedure DCM_DEMO_1{{env_suffix}}.ANALYTICS.SP_CLASSIFY_CUSTOMER_LOYALTY()
returns STRING
language PYTHON
runtime_version = '3.11'
packages = ('snowflake-snowpark-python')
handler = 'classify_customers'
as
$$
def classify_customers(session):
    from snowflake.snowpark.functions import col, when, lit

    df = session.table("DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_SPENDING_SUMMARY")

    df_with_tiers = df.with_column(
        "LOYALTY_TIER",
        when(col("TOTAL_SPEND_USD") >= 500, lit("GOLD"))
        .when(col("TOTAL_SPEND_USD") >= 100, lit("SILVER"))
        .otherwise(lit("BRONZE"))
    ).select(
        "CUSTOMER_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "TOTAL_SPEND_USD",
        "TOTAL_ORDERS",
        "LOYALTY_TIER"
    )

    df_with_tiers.write.mode("overwrite").save_as_table(
        "DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_LOYALTY_TIERS"
    )

    counts = (
        session.table("DCM_DEMO_1{{env_suffix}}.ANALYTICS.CUSTOMER_LOYALTY_TIERS")
        .group_by("LOYALTY_TIER")
        .count()
        .sort("LOYALTY_TIER")
        .collect()
    )

    lines = [f"{row['LOYALTY_TIER']}: {row['COUNT']}" for row in counts]
    return "Loyalty tiers updated. " + " | ".join(lines)
$$
;
