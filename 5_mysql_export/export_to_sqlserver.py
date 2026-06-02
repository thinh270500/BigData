from pyspark.sql import SparkSession

# --- CẤU HÌNH ---
# Dùng host.docker.internal để trỏ về máy Windows thật (nơi SQL Server đang chạy)
jdbc_url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=TEN_DATABASE_CUA_ANH;encrypt=false"
connection_properties = {
    "user": "sa",           # Hoặc username SQL của anh
    "password": "PASSWORD_CUA_ANH",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

spark = SparkSession.builder \
    .appName("ExportToSQLServer") \
    .getOrCreate()

# 1. Đọc lại dữ liệu (Ví dụ đọc kết quả 5a từ HDFS)
df = spark.read.format("csv").load("hdfs://hadoop-master:9000/data/*") 
# (Anh có thể thay bằng đường dẫn file kết quả cụ thể nếu anh đã lưu ra)

# 2. Đẩy vào SQL Server
# 'mode="overwrite"' nghĩa là nếu bảng đã tồn tại thì nó ghi đè mới, rất tiện khi test
df.write.jdbc(url=jdbc_url, table="TopProducts_Export", mode="overwrite", properties=connection_properties)

print("Đã đẩy dữ liệu thành công sang SQL Server!")
spark.stop()