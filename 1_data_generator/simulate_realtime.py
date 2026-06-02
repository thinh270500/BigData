import os
import shutil
import time
import glob

# Cấu hình
SOURCE_DIR = os.path.expanduser("~/bigdata-project/data")
DEST_DIR = os.path.expanduser("~/bigdata-project/realtime-data")
T = 20           # copy mỗi 20 giây (demo), thực tế đặt 3600
BATCH = 60      # mỗi lần copy 60 files (= 1 giờ của 60 shops)

os.makedirs(DEST_DIR, exist_ok=True)

all_files = sorted(glob.glob(os.path.join(SOURCE_DIR, "*.csv")))
total = len(all_files)
print(f"Tổng số file: {total}")
print(f"Bắt đầu giả lập... (copy {BATCH} file mỗi {T} giây)\n")

for i in range(0, total, BATCH):
    batch = all_files[i:i + BATCH]
    for f in batch:
        shutil.copy(f, DEST_DIR)

    timestamp = os.path.basename(batch[0]).split("-", 2)[2].replace(".csv", "")
    print(f"[{timestamp}] Đã copy {len(batch)} files → realtime-data/")
    time.sleep(T)

print("Hoàn thành giả lập!")
