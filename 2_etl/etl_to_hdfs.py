import os
import subprocess
import time
import glob

REALTIME_DIR = os.path.expanduser("~/bigdata-project/realtime-data")
HDFS_DIR = "/data"
CONTAINER = "hadoop-master"
CHECK_INTERVAL = 5  # kiểm tra mỗi 5 giây

print("ETL agent đang chạy — theo dõi thư mục realtime-data...")

processed = set()

while True:
    files = glob.glob(os.path.join(REALTIME_DIR, "*.csv"))
    new_files = [f for f in files if f not in processed]

    if new_files:
        print(f"Phát hiện {len(new_files)} file mới, đang đẩy lên HDFS...")
        for f in new_files:
            filename = os.path.basename(f)
            cmd = [
                "docker", "cp", f,
                f"{CONTAINER}:/tmp/{filename}"
            ]
            subprocess.run(cmd, check=True)

            hdfs_cmd = [
                "docker", "exec", CONTAINER,
                "hdfs", "dfs", "-put", "-f",
                f"/tmp/{filename}", f"{HDFS_DIR}/{filename}"
            ]
            subprocess.run(hdfs_cmd, check=True)
            processed.add(f)

        print(f"  → Đã đẩy {len(new_files)} files lên HDFS {HDFS_DIR}/")

    time.sleep(CHECK_INTERVAL)
