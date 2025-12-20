#!/usr/bin/env python3
from kafka import KafkaProducer
import json
import time
import uuid
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting to produce test events...")

for i in range(100):
    event = {
        "event_time": datetime.utcnow().isoformat() + "Z",
        "event_type": "view",
        "product_id": f"prod_{i % 10}",
        "category_id": f"cat_{i % 5}",
        "category_code": "electronics.smartphone",
        "brand": "TestBrand",
        "price": 99.99 + i,
        "user_id": f"user_{i % 20}",
        "user_session": str(uuid.uuid4())
    }
    
    producer.send('user-behavior-events', event)
    
    if i % 10 == 0:
        print(f"Sent {i+1} events...")
    
    time.sleep(0.1)

producer.flush()
print("Production complete. Sent 100 events.")