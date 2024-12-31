from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime

def main(input_path, dataset_id, table_id):
    spark = SparkSession.builder.appName("GCS to BigQuery ETL").getOrCreate()

    # Load data from GCS into Spark DataFrame
    # Assuming tab-separated values (TSV) format
    df = spark.read.option("header", "true").option("sep", ",").csv(input_path)

    # Transform timestamp to datetime
    df = df.withColumn("Datetime", from_unixtime("Timestamp"))

    # Drop the 'Timestamp' column if you don't need it anymore
    df = df.drop("Timestamp")

    # Set BigQuery client and table reference
    # Write the DataFrame to BigQuery
    df.write.format("bigquery") \
        .option("temporaryGcsBucket", "sensor_data_input_demo") \
        .option("table", f"{dataset_id}.{table_id}") \
        .mode("append") \
        .save()

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3])
