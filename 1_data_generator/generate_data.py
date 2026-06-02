import os
import random
from datetime import datetime, timedelta

# Cấu hình
NUM_SHOPS = 60
HOURS_PER_DAY = 18  # 6h - 23h
OUTPUT_DIR = os.path.expanduser("~/bigdata-project/data")

products = [
    (101, "Ca phe Mocha Da", 39000),
    (102, "Espresso", 59000),
    (103, "Ca phe sua da", 29000),
    (104, "Tra sua tran chau", 35000),
    (105, "Nuoc ep cam", 25000),
    (106, "Sinh to bo", 45000),
    (107, "Banh mi thit", 25000),
    (108, "Banh ngot", 30000),
    (109, "Combo sang", 55000),
    (110, "Nuoc suoi", 10000),
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

start_date = datetime(2021, 1, 1, 6, 0, 0)
total_files = 0

for day in range(365):
    for hour in range(HOURS_PER_DAY):
        dt = start_date + timedelta(days=day, hours=hour)
        for shop in range(1, NUM_SHOPS + 1):
            filename = f"Shop-{shop}-{dt.strftime('%Y%m%d-%H')}.csv"
            filepath = os.path.join(OUTPUT_DIR, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write("OrderID,ProductID,ProductName,Amount,Price,Discount\n")
                # Mỗi file có 5-15 dòng order
                num_orders = random.randint(20, 50)
                order_base = random.randint(100000, 999999)
                for i in range(num_orders):
                    order_id = f"{shop}{dt.strftime('%Y%m%d%H')}{i:04d}"
                    prod = random.choice(products)
                    amount = random.randint(1, 5)
                    discount = random.choice([0, 0, 0, 5, 10])
                    f.write(f"{order_id},{prod[0]},{prod[1]},{amount},{prod[2]},{discount}\n")

            total_files += 1
            if total_files % 10000 == 0:
                print(f"Đã sinh {total_files} files...")

print(f"Hoàn thành! Tổng cộng {total_files} files.")
