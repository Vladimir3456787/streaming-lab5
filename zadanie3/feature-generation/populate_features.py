import redis
import json
import random
import time
from datetime import datetime, timedelta
import sys

def populate_redis_features():
    """Заполнение Redis тестовыми данными"""
    
    # Подключение к Redis
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=False)
        r.ping()
        print("Connected to Redis")
    except:
        print("Cannot connect to Redis. Make sure Redis is running.")
        sys.exit(1)
    
    # Очистка старых данных (опционально)
    # r.flushall()
    
    # Генерация фич для 100 пользователей
    print("Generating user features...")
    for i in range(1, 101):
        user_id = f"user_{i}"
        
        # User features
        user_features = {
            "user_id": user_id,
            "avg_transaction_amount": random.uniform(10, 1000),
            "total_transactions": random.randint(1, 100),
            "recent_transaction_count": random.randint(0, 10),
            "transaction_velocity": random.uniform(0, 1),
            "preferred_category": random.choice(["electronics", "clothing", "home", "books"]),
            "session_duration_avg": random.randint(60, 600),
            "days_since_last_purchase": random.uniform(0, 30),
            "created_at": time.time()
        }
        
        # Сохранение в Redis с TTL 5 минут
        r.setex(
            f"user:{user_id}:features",
            300,
            json.dumps(user_features)
        )
        
        # Генерация истории транзакций
        transactions = []
        for j in range(random.randint(5, 20)):
            transaction = {
                "amount": random.uniform(10, 500),
                "timestamp": time.time() - random.uniform(0, 86400 * 30),
                "category": random.choice(["electronics", "clothing", "home", "books"]),
                "product_id": f"product_{random.randint(1, 50)}"
            }
            transactions.append(json.dumps(transaction))
        
        if transactions:
            r.lpush(f"user:{user_id}:transactions", *transactions)
            r.ltrim(f"user:{user_id}:transactions", 0, 99)
    
    # Генерация фич для 50 товаров
    print("Generating product features...")
    for i in range(1, 51):
        product_id = f"product_{i}"
        
        product_features = {
            "product_id": product_id,
            "price": round(random.uniform(5, 1000), 2),
            "category_id": random.randint(1, 10),
            "category_name": random.choice(["electronics", "clothing", "home", "books"]),
            "popularity_score": random.uniform(0, 1),
            "rating": round(random.uniform(1, 5), 1),
            "click_rate": random.uniform(0, 0.5),
            "stock_quantity": random.randint(0, 1000)
        }
        
        r.setex(
            f"product:{product_id}:features",
            3600,  # 1 час TTL
            json.dumps(product_features)
        )
    
    print(f"Feature generation completed at {datetime.now()}")
    print(f"- Users: 100")
    print(f"- Products: 50")
    print(f"- Transactions: ~1000")
    
    # Проверка данных
    print("\nSample data:")
    sample_user = r.get("user:user_1:features")
    if sample_user:
        print(f"User 1 features: {json.loads(sample_user)}")

def generate_kafka_events():
    """Генерация тестовых событий для Kafka"""
    from kafka import KafkaProducer
    import json
    import time
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Generating Kafka events...")
    
    for i in range(100):
        event = {
            "event_time": datetime.utcnow().isoformat() + "Z",
            "event_type": random.choice(["view", "cart", "purchase"]),
            "product_id": f"product_{random.randint(1, 50)}",
            "category_id": random.randint(1, 10),
            "category_code": random.choice(["electronics.smartphone", "clothing.shirt", "home.furniture"]),
            "brand": random.choice(["Apple", "Samsung", "Nike", "IKEA"]),
            "price": round(random.uniform(10, 1000), 2),
            "user_id": f"user_{random.randint(1, 100)}",
            "user_session": f"session_{random.randint(1000, 9999)}"
        }
        
        producer.send('user-behavior-events', event)
        
        if i % 10 == 0:
            print(f"Sent {i} events...")
        
        time.sleep(0.1)  # 10 событий в секунду
    
    producer.flush()
    print("Kafka events generation completed")

if __name__ == "__main__":
    # Заполнение Redis
    populate_redis_features()
    
    # Запрос на генерацию Kafka событий
    generate_kafka = input("\nGenerate Kafka events? (y/n): ")
    if generate_kafka.lower() == 'y':
        try:
            generate_kafka_events()
        except Exception as e:
            print(f"Error generating Kafka events: {e}")
            print("Make sure Kafka is running and topic 'user-behavior-events' exists")