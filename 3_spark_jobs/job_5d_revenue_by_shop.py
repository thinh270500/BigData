import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, input_file_name, regexp_extract

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Lỗi: Thiếu tham số thời điểm D! Cú pháp: spark-submit job_5d_revenue_by_shop.py <YYYYMM>")
        sys.exit(1)
        
    D = sys.argv[1]

    spark = SparkSession.builder.appName("Spark-Job-5d").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    hdfs_path = "hdfs://hadoop-master:9000/data/*"
    df_raw = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(hdfs_path)
    df = df_raw.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    # Trích xuất ShopID và YearMonth từ tên file nguồn
    df_enhanced = df.withColumn("file_name", input_file_name()) \
                    .withColumn("ShopID", regexp_extract(col("file_name"), r"Shop-(\d+)", 1)) \
                    .withColumn("YearMonth", regexp_extract(col("file_name"), r"(\d{6})", 1))

    print(f"\n=== [CÂU 5d] DOANH THU CỦA TỪNG SHOP TRONG THÁNG {D} ===")
    df_revenue_shop = df_enhanced.filter(col("YearMonth") == D) \
        .withColumn("Revenue", col("Amount") * col("Price") - col("Discount")) \
        .groupBy("ShopID") \
        .agg(_sum("Revenue").alias("Shop_Revenue")) \
        .orderBy(col("ShopID").cast("int"))
        
    df_revenue_shop.show(df_revenue_shop.count(), truncate=False)
    spark.stop()