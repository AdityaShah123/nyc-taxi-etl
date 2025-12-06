# spark_jobs/analytics_yellow_s3.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse

def create_spark(name="analytics-yellow"):
    return SparkSession.builder.appName(name).getOrCreate()

def compute_aggregates(input_base, output_base, from_year, to_year):
    spark = create_spark()
    # read curated parquet for years range
    df = spark.read.parquet(f"{input_base}/yellow/")
    df = df.filter((F.year("pickup_datetime") >= from_year) & (F.year("pickup_datetime") <= to_year))
    # avg fare per mile by hour
    fare_hour = df.withColumn("pickup_hour", F.hour("pickup_datetime")) \
                  .withColumn("fare_per_mile", F.col("fare_amount")/F.col("trip_distance")) \
                  .groupBy("pickup_hour") \
                  .agg(F.avg("fare_per_mile").alias("avg_fare_per_mile"), F.count("*").alias("trip_count"))
    fare_hour.write.mode("overwrite").parquet(f"{output_base}/yellow/avg_fare_per_mile_by_hour/")
    # trips by zone/dow
    trips_by_dow = df.withColumn("pickup_dow", F.date_format("pickup_datetime","E")) \
                     .groupBy("pickup_dow").count().withColumnRenamed("count","trip_count")
    trips_by_dow.write.mode("overwrite").parquet(f"{output_base}/yellow/trips_by_dow/")
    # busiest pu/do
    df.groupBy("PULocationID").count().withColumnRenamed("count","trip_count") \
      .orderBy(F.desc("trip_count")).limit(100).write.mode("overwrite").parquet(f"{output_base}/yellow/busiest_pickup/")
    df.groupBy("DOLocationID").count().withColumnRenamed("count","trip_count") \
      .orderBy(F.desc("trip_count")).limit(100).write.mode("overwrite").parquet(f"{output_base}/yellow/busiest_dropoff/")
    spark.stop()

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--from-year", type=int, required=True)
    parser.add_argument("--to-year", type=int, required=True)
    args=parser.parse_args()
    compute_aggregates(args.input_base, args.output_base, args.from_year, args.to_year)
