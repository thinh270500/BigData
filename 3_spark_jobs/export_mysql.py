from pyspark.sql import SparkSession

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Export-To-Postgres-All-Jobs") \
    .getOrCreate()

# 2. Cấu hình thông tin kết nối trực tiếp vào container PostgreSQL nội bộ
# Đổi port từ 3306 (MySQL) thành 5432 (Postgres) và dùng IP nội bộ 172.19.0.2
jdbc_url = "jdbc:mysql://host.docker.internal:3306/bigdata_project"

mysql_props = {
    "user": "root",                     # Nếu chạy báo lỗi Auth, anh đổi thành "postgres" nhé
    "password": "27052000",
    "driver": "com.mysql.cj.jdbc.Driver"   # Sử dụng Driver của Postgres
}

# 3. Mở rộng danh sách bao gồm đầy đủ cả 4 câu a, b, c, d
jobs = {
    "hdfs://hadoop-master:9000/output/cau5a": "top_products_all_time",
    "hdfs://hadoop-master:9000/output/cau5b": "top_products_by_month",
    "hdfs://hadoop-master:9000/output/cau5c": "revenue_product",
    "hdfs://hadoop-master:9000/output/cau5d": "revenue_by_shop"
}

# 4. Vòng lặp tự động đọc từ HDFS và ghi đè vào Cơ sở dữ liệu
for hdfs_path, table_name in jobs.items():

    print(f"\n==================== XỬ LÝ BẢNG: {table_name} ====================")

    try:
        # Đọc dữ liệu từ thư mục kết quả trên HDFS
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(hdfs_path)
        
        # In kiểm tra cấu trúc dữ liệu trước khi đẩy đi
        print(f"Dữ liệu đọc từ HDFS ({hdfs_path}):")
        df.show(5, truncate=False)

        # Tiến hành write .jdbc đẩy thẳng vào Database
        df.write \
            .jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=mysql_props
            )

        print(f"-> THÀNH CÔNG: Đã ghi dữ liệu vào bảng: '{table_name}'")
        
    except Exception as e:
        print(f"-> THẤT BẠI: Lỗi khi xử lý dữ liệu từ {hdfs_path}. Chi tiết: {str(e)}")

# 5. Đóng Spark Session để giải phóng RAM
spark.stop()
print("\n==================== HOÀN THÀNH TOÀN BỘ PIPELINE EXPORT ====================")