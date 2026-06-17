import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    desc,
    substring,
    length
)

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: spark-submit job_5b_top_k_by_month.py <K> <YYYYMM>")
        sys.exit(1)

    K = int(sys.argv[1])
    D = sys.argv[2]

    spark = SparkSession.builder \
        .appName("Job-5b-TopK-By-Month") \
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

    # lấy YYYYMM từ OrderID
    df = df.withColumn(
        "YearMonth",
        substring(col("OrderID_str"), 2, 6)
    )

    result = (
        df.filter(col("YearMonth") == D)
          .groupBy("ProductID", "ProductName")
          .agg(_sum("Amount").alias("Total_Amount"))
          .orderBy(desc("Total_Amount"))
          .limit(K)
    )

    print(f"\n=== TOP {K} SAN PHAM THANG {D} ===")

    result.show(truncate=False)
    result.write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv("hdfs://hadoop-master:9000/output/cau5b")

    spark.stop()