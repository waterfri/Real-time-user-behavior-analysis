import json
import time
import random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'], # 连接到宿主机 Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # 序列化
)

events_types = ["page_view", "click", "purchase", "add_to_cart"]
channels = ["organic", "ads", "social", "referral"]

print("Sending events to Kafka... Press Ctrl+C to stop.")

try:
    while True:
        event = {
            "event_time": datetime.now(timezone.utc).isoformat(),
            "event_type": random.choice(events_types),
            "user_id": f"u{random.randint(1, 50)}",
            "traffic": {"channel": random.choice(channels)}
        }
        producer.send("events_raw", value=event)
        print("Sent:", event)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n Stopped sending events.")
finally:
    producer.close()
