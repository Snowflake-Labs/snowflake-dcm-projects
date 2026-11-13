"""segments.py: the Marketing team's Spark job.

Reads the shared raw tables from the platform, counts each customer's completed
trades across all of their accounts, assigns an activity tier, and adds a
synthetic preferred channel and contact email generated with Faker, a PyPI
package the code bundle installs from pyproject.toml. It writes the result into
CUSTOMER_SEGMENTS, an Iceberg table the DCM project created: it
empties the table and appends the new rows, so the table itself, with its
columns and comments, is never replaced.

Run by the BUILD_SEGMENTS task in pipeline.yml:
  EXECUTE CODE BUNDLE <db>.SEGMENTS.SEGMENTS_JOB
    ENTRYPOINT = 'segments.py'
    ARGUMENTS = ('--raw-db', 'DCM_DEMO_2_DEV', '--target-db', 'DCM_DEMO_2_MARKETING_DEV');
"""

import argparse

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CHANNELS = ["EMAIL", "SMS", "PUSH", "DIRECT_MAIL"]


def contact_rows(customer_ids):
    """Synthetic marketing attributes, the same for a customer on every run.

    Faker is seeded with the customer ID, so reruns produce identical values.
    The customer list is small, so this runs on the driver; a UDF would need
    Faker installed wherever the UDF executes.
    """
    fake = Faker()
    rows = []
    for c_id in customer_ids:
        fake.seed_instance(int(c_id))
        rows.append((c_id, fake.random_element(CHANNELS), fake.email()))
    return rows


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

    ids = [r["C_ID"] for r in customers.select("C_ID").collect()]
    contacts = spark.createDataFrame(
        contact_rows(ids), "CONTACT_C_ID long, PREFERRED_CHANNEL string, CONTACT_EMAIL string"
    )

    segments = (
        customers.join(activity, customers.C_ID == activity.CA_C_ID, "left")
        .join(contacts, customers.C_ID == contacts.CONTACT_C_ID, "left")
        .select(
            F.col("C_ID"),
            F.concat_ws(" ", "C_F_NAME", "C_L_NAME").alias("CUSTOMER_NAME"),
            F.coalesce("TRADE_COUNT", F.lit(0)).cast("long").alias("TRADE_COUNT"),
            F.round(F.coalesce("TOTAL_VALUE", F.lit(0)), 2).cast("decimal(18,2)").alias("TOTAL_VALUE"),
            F.col("PREFERRED_CHANNEL"),
            F.col("CONTACT_EMAIL"),
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
        "C_ID", "CUSTOMER_NAME", "TRADE_COUNT", "TOTAL_VALUE", "ACTIVITY_TIER",
        "PREFERRED_CHANNEL", "CONTACT_EMAIL", "SEGMENTED_AT"
    ).write.insertInto(target)

    for row in spark.table(target).groupBy("ACTIVITY_TIER").count().orderBy("ACTIVITY_TIER").collect():
        print(f"{row['ACTIVITY_TIER']}: {row['count']}")


if __name__ == "__main__":
    main()
