from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test-MySQL") \
    .getOrCreate()

data = [(1, "Test Product", 999)]
df = spark.createDataFrame(
    data,
    ["ProductID", "ProductName", "Total_Amount"]
)

df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://host.docker.internal:3306/bigdata_project") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "top_products_all_time") \
    .option("user", "root") \
    .option("password", "27052000") \
    .mode("append") \
    .save()

spark.stop()