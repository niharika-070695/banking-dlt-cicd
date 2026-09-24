from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="silver_transactions",
    comment="Cleaned banking transaction data"
)
@dp.expect_or_drop(
    "valid_transaction_id",
    "transaction_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_account_id",
    "account_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_amount",
    "amount IS NOT NULL AND amount > 0"
)
def silver_transactions():

    return (
        spark.readStream.table("bronze_transactions")
        .dropDuplicates(["transaction_id"])
        .withColumn(
            "amount",
            col("amount").cast("decimal(12,2)")
        )
        .withColumn(
            "transaction_ts",
            col("transaction_ts").cast("timestamp")
        )
    )