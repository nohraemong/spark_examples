import csv
import random
import time
from datetime import datetime, timedelta
import os

# 샘플 데이터 디렉토리 생성
os.makedirs("/tmp/streaming_data", exist_ok=True)

# 샘플 데이터 생성 함수
def generate_sample_data():
    actions = ["view", "click", "purchase", "add_to_cart"]
    user_ids = [f"user_{i:03d}" for i in range(1, 101)]
    product_ids = [f"prod_{i:03d}" for i in range(1, 51)]
    
    data = []
    base_time = datetime.now()
    
    for i in range(100):
        timestamp = (base_time + timedelta(seconds=i*2)).strftime("%Y-%m-%d %H:%M:%S")
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        action = random.choice(actions)
        amount = round(random.uniform(10.0, 500.0), 2) if action == "purchase" else 0.0
        
        data.append([timestamp, user_id, product_id, action, amount])
    
    return data

# 배치별로 파일 생성
def create_batch_files():
    for batch in range(5):
        filename = f"/tmp/streaming_data/batch_{batch:02d}.csv"
        data = generate_sample_data()
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["timestamp", "user_id", "product_id", "action", "amount"])
            writer.writerows(data)
        
        print(f"생성됨: {filename}")
        time.sleep(2)  # 2초 간격으로 파일 생성

if __name__ == "__main__":
    create_batch_files()
    print("샘플 데이터 생성 완료!")
