from kafka import KafkaProducer
import json
import time
import random

def generate_event():
    """Генерирует простое тестовое событие"""
    return {
        "event_time": "2024-01-01 12:00:00 UTC",
        "event_type": random.choice(["view", "cart", "purchase"]),
        "product_id": str(random.randint(1000000, 2000000)),
        "user_id": str(random.randint(1000, 2000)),
        "price": round(random.uniform(10, 1000), 2),
        "message": "test_event"
    }

def main():
    print("Waiting for Kafka to start...")
    time.sleep(30)  # Даем время Kafka запуститься
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        print("Starting to send test events...")
        
        # Отправляем 5 тестовых событий
        for i in range(5):
            event = generate_event()
            producer.send('user-events', event)
            print(f"Sent event {i+1}: {event}")
            time.sleep(2)  # Пауза между событиями
        
        producer.flush()
        print("Successfully sent 5 test events!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()