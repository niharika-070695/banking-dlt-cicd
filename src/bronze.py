from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


@dp.table(
    name="bronze_transactions",
    comment="Raw banking transaction data"
)
def bronze_transactions():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/banking/demo/source/")
        .filter(col("_metadata.file_name").startswith("transactions_"))
        .withColumn(
            "source_file",
            col("_metadata.file_name")
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
    )