from prometheus_client import start_http_server, Gauge, Counter
import random, time

# Метрики
data_drift = Gauge("data_drift_score", "Data drift score")
kafka_lag = Gauge("kafka_consumer_group_lag", "Kafka lag", ["topic"])
late_events = Counter("late_arriving_events_total", "Late events")
processing = Gauge("processing_rate_events_per_second", "Processing rate")
state_size = Gauge("flink_state_size_bytes", "State size")
checkpoint = Gauge("flink_checkpoint_duration_ms", "Checkpoint duration")

start_http_server(5000)

topics = ["user-events", "product-views"]
print("✅ Сервер метрик запущен на порту 5000")

while True:
    # Data drift (0-0.3, иногда >0.2 для алертов)
    drift = random.uniform(0, 0.3)
    if random.random() < 0.2:
        drift = random.uniform(0.2, 0.4)
    data_drift.set(drift)
    
    # Kafka lag
    for topic in topics:
        lag = random.randint(0, 2000)
        if topic == "user-events" and random.random() < 0.3:
            lag = random.randint(1000, 3000)  # High lag
        kafka_lag.labels(topic=topic).set(lag)
    
    # Late events
    if random.random() < 0.4:
        late_events.inc(random.randint(1, 10))
    
    # Processing rate
    processing.set(random.randint(800, 2000))
    
    # State size
    state_size.set(random.randint(100*1024*1024, 2*1024*1024*1024))
    
    # Checkpoint duration
    checkpoint.set(random.randint(1000, 45000))
    if random.random() < 0.1:  # 10% шанс долгого checkpoint
        checkpoint.set(random.randint(60000, 120000))
    
    time.sleep(5)
