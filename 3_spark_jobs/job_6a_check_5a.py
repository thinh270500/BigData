import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Lỗi: Thiếu tham số K! Cú pháp: spark-submit job_6a_check_5a.py <K>")
        sys.exit(1)
        
    K = int(sys.argv[1])

    spark = SparkSession.builder \
        .appName("SparkSQL-Check-5a") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Đọc dữ liệu từ HDFS
    hdfs_path = "hdfs://hadoop-master:9000/data/*"
    df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(hdfs_path)
    df_named = df.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    # Đăng ký bảng ảo
    df_named.createOrReplaceTempView("orders_table")

    print(f"\n=== [CÂU 6a] ĐỐI CHIẾU SPARKSQL: TOP {K} SẢN PHẨM BÁN CHẠY ===")
    query = f"""
        SELECT 
            ProductID, 
            ProductName, 
            SUM(Amount) AS Total_Amount
        FROM orders_table
        GROUP BY ProductID, ProductName
        ORDER BY Total_Amount DESC
        LIMIT {K}
    """
    
    spark.sql(query).show(truncate=False)
    spark.stop()