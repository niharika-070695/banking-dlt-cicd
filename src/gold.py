from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    to_date,
    sum,
    count
)


@dp.materialized_view(
    name="gold_daily_account_summary",
    comment="Daily transaction summary by account"
)
def gold_daily_account_summary():

    return (
        spark.read.table("silver_transactions")
        .filter(col("transaction_status") == "SUCCESS")
        .withColumn(
            "transaction_date",
            to_date("transaction_ts")
        )
        .groupBy(
            "account_id",
            "transaction_date"
        )
        .agg(
            count("transaction_id").alias("transaction_count"),
            sum("amount").alias("total_transaction_amount")
        )
    )