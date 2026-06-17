import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Lỗi: Thiếu tham số K! Cú pháp: spark-submit job_5a_top_k_all_time.py <K>")
        sys.exit(1)
        
    K = int(sys.argv[1])

    spark = SparkSession.builder.appName("Spark-Job-5a").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    hdfs_path = "hdfs://hadoop-master:9000/data/*"
    df_raw = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(hdfs_path)
    df = df_raw.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    print(f"\n=== [CÂU 5a] TOP {K} SẢN PHẨM BÁN CHẠY NHẤT TOÀN HỆ THỐNG ===")
    top_products = df.groupBy("ProductID", "ProductName") \
        .agg(_sum("Amount").alias("Total_Amount")) \
        .orderBy(desc("Total_Amount")) \
        .limit(K)
        
    top_products.show(truncate=False)
    top_products.write \
    .mode("overwrite") \
    .option("header","true") \
    .csv("hdfs://hadoop-master:9000/output/cau5a")
    
    spark.stop()