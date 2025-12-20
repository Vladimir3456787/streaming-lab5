#!/usr/bin/env python3
"""
Мониторинг lag в Kafka для интеграции с Prometheus
"""

import time
import logging
import json
from typing import Dict, List, Optional
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import threading
import random

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaLagSimulator:
    """Симулятор метрик Kafka для демонстрации"""
    
    def __init__(self):
        # Prometheus метрики
        self.kafka_consumer_group_lag = Gauge(
            'kafka_consumer_group_lag',
            'Lag for Kafka consumer group',
            ['consumer_group', 'topic', 'partition']
        )
        
        self.kafka_consumer_offset = Gauge(
            'kafka_consumer_offset',
            'Current consumer offset',
            ['consumer_group', 'topic', 'partition']
        )
        
        self.kafka_topic_messages = Counter(
            'kafka_topic_messages_total',
            'Total messages produced to topic',
            ['topic']
        )
        
        self.kafka_consumer_messages = Counter(
            'kafka_consumer_messages_total',
            'Total messages consumed from topic',
            ['consumer_group', 'topic']
        )
        
        self.kafka_consumption_lag = Histogram(
            'kafka_consumption_lag_seconds',
            'Lag between production and consumption',
            ['topic'],
            buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60]
        )
        
        # Состояние симулятора
        self.topics = ['user-events', 'product-views', 'purchases', 'sessions']
        self.consumer_groups = ['flink-streaming', 'spark-analytics', 'monitoring']
        self.partitions = 3
        
        # Текущие оффсеты
        self.producer_offsets = {}
        self.consumer_offsets = {}
        
        self._initialize_offsets()
        
        logger.info(f"Инициализирован симулятор Kafka с {len(self.topics)} топиками")
    
    def _initialize_offsets(self):
        """Инициализация начальных оффсетов"""
        for topic in self.topics:
            self.producer_offsets[topic] = {}
            for partition in range(self.partitions):
                self.producer_offsets[topic][partition] = random.randint(1000, 5000)
            
            for consumer_group in self.consumer_groups:
                if consumer_group not in self.consumer_offsets:
                    self.consumer_offsets[consumer_group] = {}
                
                self.consumer_offsets[consumer_group][topic] = {}
                for partition in range(self.partitions):
                    # Потребители отстают от продюсера
                    lag = random.randint(0, 100)
                    self.consumer_offsets[consumer_group][topic][partition] = \
                        self.producer_offsets[topic][partition] - lag
    
    def simulate_production(self):
        """Симуляция производства сообщений"""
        while True:
            try:
                # Выбираем случайный топик
                topic = random.choice(self.topics)
                partition = random.randint(0, self.partitions - 1)
                
                # Увеличиваем оффсет продюсера
                self.producer_offsets[topic][partition] += random.randint(1, 10)
                
                # Обновляем метрики
                self.kafka_topic_messages.labels(topic=topic).inc(random.randint(1, 5))
                
                # Пауза
                time.sleep(random.uniform(0.1, 0.5))
                
            except Exception as e:
                logger.error(f"Ошибка симуляции производства: {e}")
                time.sleep(1)
    
    def simulate_consumption(self):
        """Симуляция потребления сообщений"""
        while True:
            try:
                # Выбираем случайного потребителя и топик
                consumer_group = random.choice(self.consumer_groups)
                topic = random.choice(self.topics)
                partition = random.randint(0, self.partitions - 1)
                
                # Текущие оффсеты
                producer_offset = self.producer_offsets[topic][partition]
                consumer_offset = self.consumer_offsets[consumer_group][topic][partition]
                
                # Потребляем сообщения (увеличиваем consumer offset)
                if consumer_offset < producer_offset:
                    consume_count = random.randint(1, min(20, producer_offset - consumer_offset))
                    self.consumer_offsets[consumer_group][topic][partition] += consume_count
                    
                    # Обновляем метрики потребления
                    self.kafka_consumer_messages.labels(
                        consumer_group=consumer_group,
                        topic=topic
                    ).inc(consume_count)
                    
                    # Симулируем задержку потребления
                    lag_seconds = random.uniform(0.05, 2.0)
                    self.kafka_consumption_lag.labels(topic=topic).observe(lag_seconds)
                
                # Пауза
                time.sleep(random.uniform(0.2, 1.0))
                
            except Exception as e:
                logger.error(f"Ошибка симуляции потребления: {e}")
                time.sleep(1)
    
    def update_metrics(self):
        """Обновление метрик Prometheus"""
        while True:
            try:
                for topic in self.topics:
                    for partition in range(self.partitions):
                        producer_offset = self.producer_offsets[topic][partition]
                        
                        for consumer_group in self.consumer_groups:
                            consumer_offset = self.consumer_offsets[consumer_group][topic][partition]
                            lag = producer_offset - consumer_offset
                            
                            # Устанавливаем метрики lag
                            self.kafka_consumer_group_lag.labels(
                                consumer_group=consumer_group,
                                topic=topic,
                                partition=partition
                            ).set(lag)
                            
                            # Устанавливаем метрики offset
                            self.kafka_consumer_offset.labels(
                                consumer_group=consumer_group,
                                topic=topic,
                                partition=partition
                            ).set(consumer_offset)
                
                # Пауза между обновлениями
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Ошибка обновления метрик: {e}")
                time.sleep(10)
    
    def run(self, port=9308):
        """Запуск симулятора"""
        # Запуск HTTP сервера для Prometheus
        start_http_server(port)
        logger.info(f"Prometheus метрики Kafka доступны на порту {port}")
        
        # Запуск потоков симуляции
        threads = []
        
        # Поток производства
        producer_thread = threading.Thread(target=self.simulate_production, daemon=True)
        producer_thread.start()
        threads.append(producer_thread)
        
        # Поток потребления
        consumer_thread = threading.Thread(target=self.simulate_consumption, daemon=True)
        consumer_thread.start()
        threads.append(consumer_thread)
        
        # Поток обновления метрик
        metrics_thread = threading.Thread(target=self.update_metrics, daemon=True)
        metrics_thread.start()
        threads.append(metrics_thread)
        
        # Бесконечный цикл основного потока
        try:
            while True:
                time.sleep(60)
                logger.info("Симулятор Kafka работает...")
        except KeyboardInterrupt:
            logger.info("Симулятор Kafka остановлен")
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")

def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Симулятор метрик Kafka')
    parser.add_argument('--port', type=int, default=9308,
                       help='Порт для Prometheus метрик')
    
    args = parser.parse_args()
    
    # Запуск симулятора
    simulator = KafkaLagSimulator()
    simulator.run(args.port)

if __name__ == "__main__":
    main()