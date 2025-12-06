from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("version-check").getOrCreate()

try:
    hadoop_ver = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    print("Hadoop version used by Spark:", hadoop_ver)
except Exception as e:
    print("Failed to get Hadoop version:", e)

spark.stop()
