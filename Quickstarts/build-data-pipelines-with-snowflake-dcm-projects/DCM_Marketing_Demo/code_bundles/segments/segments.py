"""segments.py: the Marketing team's Spark job.

Reads the shared raw tables from the platform, counts each customer's completed
trades across all of their accounts, and assigns an activity tier. It writes the result into CUSTOMER_SEGMENTS, a table the DCM project created: it
empties the table and appends the new rows, so the table itself, with its
columns and comments, is never replaced.

Run by the BUILD_SEGMENTS task in pipeline.yml:
  EXECUTE CODE BUNDLE <db>.SEGMENTS.SEGMENTS_JOB
    ENTRYPOINT = 'segments.py'
    ARGUMENTS = ('--raw-db', 'DCM_DEMO_2_DEV', '--target-db', 'DCM_DEMO_2_MARKETING_DEV');
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-db", required=True, help="platform raw database, e.g. DCM_DEMO_2_DEV")
    ap.add_argument("--target-db", required=True, help="Marketing database, e.g. DCM_DEMO_2_MARKETING_DEV")
    args, _ = ap.parse_known_args()

    raw = f"{args.raw_db}.RAW"
    target = f"{args.target_db}.SEGMENTS.CUSTOMER_SEGMENTS"

    spark = SparkSession.builder.getOrCreate()

    customers = spark.table(f"{raw}.CUSTOMER_STG")
    accounts = spark.table(f"{raw}.ACCOUNT_STG")
    trades = spark.table(f"{raw}.TRADE_STG").filter(F.col("T_ST_ID") == "CMPT")

    activity = (
        trades.join(accounts, trades.T_CA_ID == accounts.CA_ID)
        .groupBy("CA_C_ID")
        .agg(
            F.count("T_ID").alias("TRADE_COUNT"),
            F.sum(F.col("T_QTY") * F.col("T_TRADE_PRICE")).alias("TOTAL_VALUE"),
        )
    )

    segments = (
        customers.join(activity, customers.C_ID == activity.CA_C_ID, "left")
        .select(
            F.col("C_ID"),
            F.concat_ws(" ", "C_F_NAME", "C_L_NAME").alias("CUSTOMER_NAME"),
            F.coalesce("TRADE_COUNT", F.lit(0)).cast("long").alias("TRADE_COUNT"),
            F.round(F.coalesce("TOTAL_VALUE", F.lit(0)), 2).cast("decimal(18,2)").alias("TOTAL_VALUE"),
        )
        .withColumn(
            "ACTIVITY_TIER",
            F.when(F.col("TRADE_COUNT") >= 4, "ACTIVE")
            .when(F.col("TRADE_COUNT") >= 1, "OCCASIONAL")
            .otherwise("DORMANT"),
        )
        .withColumn("SEGMENTED_AT", F.current_timestamp().cast("timestamp_ntz"))
    )

    # Same column order as the table definition in segments.sql. Overwrite mode
    # would recreate the table with CREATE OR REPLACE, dropping what DCM
    # declared, so empty it and append instead.
    spark.sql(f"TRUNCATE TABLE {target}")
    segments.select(
        "C_ID", "CUSTOMER_NAME", "TRADE_COUNT", "TOTAL_VALUE", "ACTIVITY_TIER", "SEGMENTED_AT"
    ).write.insertInto(target)

    for row in spark.table(target).groupBy("ACTIVITY_TIER").count().orderBy("ACTIVITY_TIER").collect():
        print(f"{row['ACTIVITY_TIER']}: {row['count']}")


if __name__ == "__main__":
    main()
