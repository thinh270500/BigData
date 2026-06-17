import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    substring,
    length,
    expr
)

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: spark-submit job_5d_revenue_by_shop.py <YYYYMM>")
        sys.exit(1)

    D = sys.argv[1]

    spark = SparkSession.builder \
        .appName("Job-5d-Revenue-By-Shop") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(
        "hdfs://hadoop-master:9000/data/*",
        header=True,
        inferSchema=True
    )

    df = df.withColumn(
        "OrderID_str",
        col("OrderID").cast("string")
    )

    # YYYYMM
    df = df.withColumn(
        "YearMonth",
        substring(col("OrderID_str"), 2, 6)
    )

    # ShopID
    # OrderID dài 16 ký tự -> ShopID 2 chữ số
    # OrderID dài 15 ký tự -> ShopID 1 chữ số

    df = df.withColumn(
        "ShopID",
        expr("""
            CASE
                WHEN length(OrderID_str)=16
                    THEN substring(OrderID_str,1,2)
                ELSE
                    substring(OrderID_str,1,1)
            END
        """)
    )

    df = df.withColumn(
        "Revenue",
        col("Amount") * col("Price") - col("Discount")
    )

    result = (
        df.filter(col("YearMonth") == D)
          .groupBy("ShopID")
          .agg(_sum("Revenue").alias("Shop_Revenue"))
          .orderBy(col("ShopID").cast("int"))
    )

    print(f"\n=== DOANH THU CAC SHOP THANG {D} ===")

    result.show(100, truncate=False)
    result.write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv("hdfs://hadoop-master:9000/output/cau5d")

    spark.stop()