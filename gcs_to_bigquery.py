from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, TimestampType
from pyspark.sql.functions import from_unixtime, col

def main(input_path, dataset_id, table_id):
    spark = SparkSession.builder.appName("GCS to BigQuery ETL").getOrCreate()
    
    # Define schema
    schema = StructType([
        StructField("UserId", IntegerType(), True),
        StructField("MovieId", IntegerType(), True),
        StructField("Rating", IntegerType(), True),
        StructField("Timestamp", LongType(), True)
    ])

    # Load data from GCS into Spark DataFrame with specified schema
    df = spark.read.option("header", "true").option("sep", ",").schema(schema).csv(input_path)

    # Convert UNIX timestamp to datetime
    df = df.withColumn("Datetime", from_unixtime(col("Timestamp")).cast(TimestampType()))

    # Optionally drop the 'Timestamp' column if it's no longer needed
    df = df.drop("Timestamp")

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
