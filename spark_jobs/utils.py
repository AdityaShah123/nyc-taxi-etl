# spark_jobs/utils.py
from pyspark.sql import functions as F, types as T

def yellow_schema():
    """
    Optional explicit schema for yellow taxi CSVs.
    Edit fields to match your CSV header exactly if necessary.
    """
    return T.StructType([
        T.StructField("VendorID", T.StringType(), True),
        T.StructField("tpep_pickup_datetime", T.StringType(), True),
        T.StructField("tpep_dropoff_datetime", T.StringType(), True),
        T.StructField("passenger_count", T.IntegerType(), True),
        T.StructField("trip_distance", T.DoubleType(), True),
        T.StructField("RatecodeID", T.StringType(), True),
        T.StructField("store_and_fwd_flag", T.StringType(), True),
        T.StructField("PULocationID", T.IntegerType(), True),
        T.StructField("DOLocationID", T.IntegerType(), True),
        T.StructField("payment_type", T.StringType(), True),
        T.StructField("fare_amount", T.DoubleType(), True),
        T.StructField("extra", T.DoubleType(), True),
        T.StructField("mta_tax", T.DoubleType(), True),
        T.StructField("tip_amount", T.DoubleType(), True),
        T.StructField("tolls_amount", T.DoubleType(), True),
        T.StructField("improvement_surcharge", T.DoubleType(), True),
        T.StructField("total_amount", T.DoubleType(), True),
    ])


def safe_cast(df, col, dtype):
    """Cast column to dtype ignoring errors by replacing bad values with null."""
    return df.withColumn(col, F.when(F.col(col).rlike(r"^-?\d+(\.\d+)?$"), F.col(col).cast(dtype)).otherwise(F.lit(None)))


def basic_clean_transform(df):
    """
    Core cleaning and feature engineering shared by ETL and analytics.
    - parse timestamps
    - filter invalid rows
    - create trip_duration_min, avg_speed_mph, fare_per_mile, time features
    """
    # Ensure numeric columns are properly typed (if strings in CSV)
    for c in ["trip_distance", "fare_amount", "tip_amount", "total_amount"]:
        if c in df.columns:
            df = safe_cast(df, c, "double")

    # Parse timestamps (tolerant to bad strings)
    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
    if "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))

    # Basic time sanity
    df = df.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )

    # Trip duration minutes
    df = df.withColumn(
        "trip_duration_min",
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / F.lit(60.0)
    )

    # Filter unrealistic trips
    df = df.filter((F.col("trip_duration_min") > 0.5) & (F.col("trip_duration_min") < 24 * 60))  # 0.5 minutes to 1 day
    df = df.filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < 500))

    # Fare sanity
    if "fare_amount" in df.columns:
        df = df.filter(F.col("fare_amount") >= 0)

    # Derived metrics (guard against division by zero)
    df = df.withColumn("avg_speed_mph", F.when(F.col("trip_duration_min") > 0, F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)).otherwise(None))
    df = df.withColumn("fare_per_mile", F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance")).otherwise(None))

    # Time features
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("pickup_dow", F.date_format("tpep_pickup_datetime", "E"))

    return df
