from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkSQL-Check-5c") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Đọc dữ liệu từ HDFS
    hdfs_path = "hdfs://hadoop-master:9000/data/*"
    df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(hdfs_path)
    df_named = df.toDF("OrderID", "ProductID", "ProductName", "Amount", "Price", "Discount")

    # Đăng ký bảng ảo
    df_named.createOrReplaceTempView("orders_table")

    print("\n=== [CÂU 6c] ĐỐI CHIẾU SPARKSQL: DOANH THU SẢN PHẨM CẢ NĂM ===")
    query = """
        SELECT 
            ProductID, 
            ProductName, 
            SUM(Amount * Price - Discount) AS Total_Revenue
        FROM orders_table
        GROUP BY ProductID, ProductName
        ORDER BY Total_Revenue DESC
    """
    
    spark.sql(query).show(truncate=False)
    spark.stop()