from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spark-Job-5c").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    hdfs_path = "hdfs://hadoop-master:9000/data/*"
    df_raw = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(hdfs_path)
    df = df_raw.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    print("\n=== [CÂU 5c] DOANH THU TRÊN MỖI SẢN PHẨM TRONG NĂM ===")
    # Công thức: Doanh thu = Số lượng * Đơn giá - Chiết khấu
    df_revenue_prod = df.withColumn("Revenue", col("Amount") * col("Price") - col("Discount")) \
        .groupBy("ProductID", "ProductName") \
        .agg(_sum("Revenue").alias("Total_Revenue")) \
        .orderBy(desc("Total_Revenue"))
        
    df_revenue_prod.show(truncate=False)
    df_revenue_prod.write \
    .mode("overwrite") \
    .option("header","true") \
    .csv("hdfs://hadoop-master:9000/output/cau5c")
    spark.stop()