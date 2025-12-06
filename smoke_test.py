# smoke_test.py — quick Spark smoke test
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("smoke-test").getOrCreate()

print("Spark version:", spark.version)

# print java version that the JVM uses
try:
    jv = spark._jvm.java.lang.System.getProperty("java.version")
except Exception as e:
    jv = f"could not read JVM property: {e}"
print("JVM java.version =", jv)

print("range(5) =>")
spark.range(5).show()

spark.stop()
print("smoke test completed OK")
