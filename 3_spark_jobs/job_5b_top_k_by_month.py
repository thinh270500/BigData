import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, input_file_name, regexp_extract

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Lỗi: Thiếu tham số! Cú pháp: spark-submit job_5b_top_k_by_month.py <K> <YYYYMM>")
        sys.exit(1)
        
    K = int(sys.argv[1])
    D = sys.argv[2] # Ví dụ: "202101"

    spark = SparkSession.builder.appName("Spark-Job-5b").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    hdfs_path = "hdfs://hadoop-master:9000/data/*"
    df_raw = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(hdfs_path)
    df = df_raw.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    # Trích xuất tháng năm YYYYMM từ tên file nguồn gộp của NiFi
    df_enhanced = df.withColumn("YearMonth", regexp_extract(input_file_name(), r"(\d{6})", 1))

    print(f"\n=== [CÂU 5b] TOP {K} SẢN PHẨM BÁN CHẠY NHẤT TRONG THÁNG {D} ===")
    top_products_at_time = df_enhanced.filter(col("YearMonth") == D) \
        .groupBy("ProductID", "ProductName") \
        .agg(_sum("Amount").alias("Total_Amount")) \
        .orderBy(desc("Total_Amount")) \
        .limit(K)
        
    top_products_at_time.show(truncate=False)
    spark.stop()