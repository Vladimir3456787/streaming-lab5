"""
Тестирование задержки системы
Измерение p95 latency < 75 мс
"""

import time
import statistics
import json
import uuid
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer, KafkaConsumer

class LatencyTester:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.consumer = KafkaConsumer(
            'recommendations',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        
        self.latencies = []
        self.test_results = {}
    
    def generate_event(self) -> Dict:
        """Генерация тестового события"""
        return {
            'event_time': datetime.utcnow().isoformat() + 'Z',
            'event_type': 'view',
            'product_id': str(uuid.uuid4()),
            'user_id': str(uuid.uuid4()),
            'price': 99.99,
            'category_code': 'electronics.smartphone',
            'brand': 'test_brand',
            'user_session': str(uuid.uuid4()),
            'test_id': str(uuid.uuid4()),
            'sent_timestamp': int(time.time() * 1000)
        }
    
    def send_events(self, num_events: int = 1000):
        """Отправка событий и измерение задержки"""
        print(f"Отправка {num_events} событий...")
        
        events = []
        for i in range(num_events):
            event = self.generate_event()
            events.append(event)
            self.producer.send('user-behavior-events', event)
        
        self.producer.flush()
        print("События отправлены")
        
        # Ожидание рекомендаций
        received = 0
        start_time = time.time()
        
        while received < num_events and time.time() - start_time < 30:
            messages = self.consumer.poll(timeout_ms=1000)
            
            for tp, msgs in messages.items():
                for msg in msgs:
                    test_id = msg.value.get('test_id')
                    if test_id:
                        sent_time = msg.value.get('sent_timestamp')
                        if sent_time:
                            latency = time.time() * 1000 - sent_time
                            self.latencies.append(latency)
                            received += 1
        
        return received
    
    def calculate_metrics(self) -> Dict:
        """Расчет метрик latency"""
        if not self.latencies:
            return {}
        
        sorted_latencies = sorted(self.latencies)
        n = len(sorted_latencies)
        
        metrics = {
            'count': n,
            'mean': statistics.mean(sorted_latencies),
            'median': statistics.median(sorted_latencies),
            'min': min(sorted_latencies),
            'max': max(sorted_latencies),
            'p50': sorted_latencies[int(n * 0.5)],
            'p75': sorted_latencies[int(n * 0.75)],
            'p90': sorted_latencies[int(n * 0.90)],
            'p95': sorted_latencies[int(n * 0.95)],
            'p99': sorted_latencies[int(n * 0.99)],
            'std_dev': statistics.stdev(sorted_latencies) if n > 1 else 0
        }
        
        return metrics
    
    def run_test(self, num_events: int = 1000) -> Dict:
        """Запуск теста"""
        print(f"Запуск теста с {num_events} событиями...")
        
        received = self.send_events(num_events)
        
        if received == 0:
            print("Не получено ни одного ответа")
            return {}
        
        metrics = self.calculate_metrics()
        
        # Проверка SLA
        sla_passed = metrics['p95'] < 75
        metrics['sla_passed'] = sla_passed
        metrics['sla_requirement'] = 'p95 < 75ms'
        metrics['received_events'] = received
        
        print("\nРезультаты теста:")
        print(f"  Обработано событий: {received}/{num_events}")
        print(f"  Средняя задержка: {metrics['mean']:.2f} ms")
        print(f"  p95 latency: {metrics['p95']:.2f} ms")
        print(f"  SLA p95 < 75ms: {'✅' if sla_passed else '❌'}")
        
        return metrics

def main():
    tester = LatencyTester('localhost:9092')
    
    # Запуск тестов с разным нагрузкой
    test_cases = [100, 500, 1000, 2000]
    results = {}
    
    for num_events in test_cases:
        print(f"\n{'='*50}")
        print(f"Тест с {num_events} событиями")
        print('='*50)
        
        result = tester.run_test(num_events)
        results[num_events] = result
        
        if result.get('sla_passed') is False:
            print(f"⚠️  SLA нарушено для нагрузки {num_events}")
        
        time.sleep(2)
    
    # Сводный отчет
    print("\n" + "="*60)
    print("СВОДНЫЙ ОТЧЕТ ПО ЛАТЕНСИ")
    print("="*60)
    
    for load, metrics in results.items():
        if metrics:
            status = "✅" if metrics['sla_passed'] else "❌"
            print(f"Нагрузка {load:4d}: "
                  f"p95={metrics['p95']:6.2f}ms, "
                  f"mean={metrics['mean']:6.2f}ms "
                  f"{status}")

if __name__ == "__main__":
    main()