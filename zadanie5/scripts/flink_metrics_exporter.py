#!/usr/bin/env python3
"""
Экспортер метрик Flink в Prometheus формат
Симулятор метрик Flink для демонстрации
"""

import time
import logging
import random
from datetime import datetime
from prometheus_client import start_http_server, Gauge, Counter, Histogram, Info
import threading

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FlinkMetricsSimulator:
    """Симулятор метрик Apache Flink"""
    
    def __init__(self):
        # Информационные метрики
        self.flink_info = Info(
            'flink_job_info',
            'Information about Flink job',
            ['job_name', 'job_id', 'operator']
        )
        
        # Метрики JobManager
        self.flink_jobmanager_numTaskManagers = Gauge(
            'flink_jobmanager_numTaskManagers',
            'Number of registered TaskManagers'
        )
        
        self.flink_jobmanager_numSlotsTotal = Gauge(
            'flink_jobmanager_numSlotsTotal',
            'Total number of slots'
        )
        
        self.flink_jobmanager_numSlotsAvailable = Gauge(
            'flink_jobmanager_numSlotsAvailable',
            'Number of available slots'
        )
        
        # Метрики Job
        self.flink_jobmanager_job_uptime = Gauge(
            'flink_jobmanager_job_uptime',
            'Job uptime in milliseconds',
            ['job_name', 'job_id']
        )
        
        self.flink_jobmanager_job_lastCheckpointDuration = Gauge(
            'flink_jobmanager_job_lastCheckpointDuration',
            'Duration of last checkpoint in milliseconds',
            ['job_name', 'job_id']
        )
        
        self.flink_jobmanager_job_lastCheckpointSize = Gauge(
            'flink_jobmanager_job_lastCheckpointSize',
            'Size of last checkpoint in bytes',
            ['job_name', 'job_id']
        )
        
        self.flink_jobmanager_job_numberOfCompletedCheckpoints = Counter(
            'flink_jobmanager_job_numberOfCompletedCheckpoints',
            'Number of completed checkpoints',
            ['job_name', 'job_id']
        )
        
        self.flink_jobmanager_job_numberOfFailedCheckpoints = Counter(
            'flink_jobmanager_job_numberOfFailedCheckpoints',
            'Number of failed checkpoints',
            ['job_name', 'job_id']
        )
        
        # Метрики TaskManager
        self.flink_taskmanager_Status_JVM_CPU_Load = Gauge(
            'flink_taskmanager_Status_JVM_CPU_Load',
            'JVM CPU load',
            ['taskmanager_id', 'host']
        )
        
        self.flink_taskmanager_Status_JVM_Memory_Heap_Used = Gauge(
            'flink_taskmanager_Status_JVM_Memory_Heap_Used',
            'JVM heap memory used in bytes',
            ['taskmanager_id', 'host']
        )
        
        self.flink_taskmanager_Status_JVM_Memory_Heap_Max = Gauge(
            'flink_taskmanager_Status_JVM_Memory_Heap_Max',
            'JVM heap memory max in bytes',
            ['taskmanager_id', 'host']
        )
        
        # Метрики операторов
        self.flink_taskmanager_job_task_numRecordsIn = Counter(
            'flink_taskmanager_job_task_numRecordsIn',
            'Number of records received',
            ['job_name', 'task_name', 'operator_name', 'taskmanager_id']
        )
        
        self.flink_taskmanager_job_task_numRecordsOut = Counter(
            'flink_taskmanager_job_task_numRecordsOut',
            'Number of records sent',
            ['job_name', 'task_name', 'operator_name', 'taskmanager_id']
        )
        
        self.flink_taskmanager_job_task_numRecordsInPerSecond = Gauge(
            'flink_taskmanager_job_task_numRecordsInPerSecond',
            'Records received per second',
            ['job_name', 'task_name', 'operator_name', 'taskmanager_id']
        )
        
        self.flink_taskmanager_job_task_numRecordsOutPerSecond = Gauge(
            'flink_taskmanager_job_task_numRecordsOutPerSecond',
            'Records sent per second',
            ['job_name', 'task_name', 'operator_name', 'taskmanager_id']
        )
        
        self.flink_taskmanager_job_task_latency = Histogram(
            'flink_taskmanager_job_task_latency',
            'Processing latency in milliseconds',
            ['job_name', 'task_name', 'operator_name'],
            buckets=[1, 5, 10, 50, 100, 500, 1000, 5000]
        )
        
        self.flink_taskmanager_job_task_backPressuredTimeMsPerSecond = Gauge(
            'flink_taskmanager_job_task_backPressuredTimeMsPerSecond',
            'Backpressured time per second in milliseconds',
            ['job_name', 'task_name', 'operator_name', 'taskmanager_id']
        )
        
        self.flink_taskmanager_job_task_stateSize_bytes = Gauge(
            'flink_taskmanager_job_task_stateSize_bytes',
            'Size of operator state in bytes',
            ['job_name', 'task_name', 'operator_name', 'taskmanager_id']
        )
        
        # Состояние симулятора
        self.job_name = "streaming-ecommerce-job"
        self.job_id = "a1b2c3d4e5f6"
        self.taskmanagers = [
            {"id": "tm-1", "host": "flink-taskmanager-1", "slots": 4},
            {"id": "tm-2", "host": "flink-taskmanager-2", "slots": 4},
            {"id": "tm-3", "host": "flink-taskmanager-3", "slots": 4}
        ]
        
        self.operators = [
            {"name": "kafka-source", "task": "Source: Kafka"},
            {"name": "map-transform", "task": "Map: Transform"},
            {"name": "filter-events", "task": "Filter: Events"},
            {"name": "window-aggregate", "task": "Window: Aggregate"},
            {"name": "sink-elasticsearch", "task": "Sink: Elasticsearch"}
        ]
        
        self.job_start_time = time.time() * 1000  # в миллисекундах
        self.checkpoint_counter = 0
        
        # Инициализация информационных метрик
        for operator in self.operators:
            self.flink_info.labels(
                job_name=self.job_name,
                job_id=self.job_id,
                operator=operator['name']
            ).info({'version': '1.14.0', 'status': 'RUNNING'})
        
        logger.info(f"Инициализирован симулятор Flink с job: {self.job_name}")
    
    def simulate_jobmanager_metrics(self):
        """Симуляция метрик JobManager"""
        while True:
            try:
                # Количество TaskManagers (случайные изменения)
                num_tms = len(self.taskmanagers)
                if random.random() < 0.01:  # 1% шанс изменения
                    num_tms = max(1, num_tms + random.randint(-1, 1))
                
                self.flink_jobmanager_numTaskManagers.set(num_tms)
                
                # Слоты
                total_slots = num_tms * 4
                available_slots = max(0, total_slots - random.randint(2, 8))
                
                self.flink_jobmanager_numSlotsTotal.set(total_slots)
                self.flink_jobmanager_numSlotsAvailable.set(available_slots)
                
                # Uptime
                uptime = (time.time() * 1000) - self.job_start_time
                self.flink_jobmanager_job_uptime.labels(
                    job_name=self.job_name,
                    job_id=self.job_id
                ).set(uptime)
                
                # Checkpoint метрики
                checkpoint_duration = random.randint(1000, 5000)
                if random.random() < 0.1:  # 10% шанс долгого checkpoint
                    checkpoint_duration = random.randint(10000, 30000)
                
                self.flink_jobmanager_job_lastCheckpointDuration.labels(
                    job_name=self.job_name,
                    job_id=self.job_id
                ).set(checkpoint_duration)
                
                checkpoint_size = random.randint(100 * 1024 * 1024, 500 * 1024 * 1024)  # 100-500 MB
                self.flink_jobmanager_job_lastCheckpointSize.labels(
                    job_name=self.job_name,
                    job_id=self.job_id
                ).set(checkpoint_size)
                
                # Увеличиваем счетчики checkpoint
                if random.random() < 0.3:  # 30% шанс нового checkpoint
                    self.flink_jobmanager_job_numberOfCompletedCheckpoints.labels(
                        job_name=self.job_name,
                        job_id=self.job_id
                    ).inc()
                    
                    self.checkpoint_counter += 1
                    
                    # Редкие сбои checkpoint
                    if self.checkpoint_counter % 20 == 0:
                        self.flink_jobmanager_job_numberOfFailedCheckpoints.labels(
                            job_name=self.job_name,
                            job_id=self.job_id
                        ).inc()
                
                # Пауза
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Ошибка симуляции JobManager метрик: {e}")
                time.sleep(5)
    
    def simulate_taskmanager_metrics(self):
        """Симуляция метрик TaskManager"""
        while True:
            try:
                for tm in self.taskmanagers:
                    # CPU load
                    cpu_load = random.uniform(0.1, 0.8)
                    if random.random() < 0.05:  # 5% шанс высокой загрузки
                        cpu_load = random.uniform(0.8, 1.0)
                    
                    self.flink_taskmanager_Status_JVM_CPU_Load.labels(
                        taskmanager_id=tm['id'],
                        host=tm['host']
                    ).set(cpu_load)
                    
                    # Memory usage
                    heap_used = random.randint(512 * 1024 * 1024, 2 * 1024 * 1024 * 1024)  # 512MB - 2GB
                    heap_max = 4 * 1024 * 1024 * 1024  # 4GB
                    
                    self.flink_taskmanager_Status_JVM_Memory_Heap_Used.labels(
                        taskmanager_id=tm['id'],
                        host=tm['host']
                    ).set(heap_used)
                    
                    self.flink_taskmanager_Status_JVM_Memory_Heap_Max.labels(
                        taskmanager_id=tm['id'],
                        host=tm['host']
                    ).set(heap_max)
                
                # Пауза
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Ошибка симуляции TaskManager метрик: {e}")
                time.sleep(5)
    
    def simulate_operator_metrics(self):
        """Симуляция метрик операторов"""
        while True:
            try:
                for tm in self.taskmanagers:
                    for operator in self.operators:
                        # Records in/out
                        records_in = random.randint(100, 1000)
                        records_out = records_in - random.randint(0, 50)  # Некоторые отфильтровываются
                        
                        self.flink_taskmanager_job_task_numRecordsIn.labels(
                            job_name=self.job_name,
                            task_name=operator['task'],
                            operator_name=operator['name'],
                            taskmanager_id=tm['id']
                        ).inc(records_in)
                        
                        self.flink_taskmanager_job_task_numRecordsOut.labels(
                            job_name=self.job_name,
                            task_name=operator['task'],
                            operator_name=operator['name'],
                            taskmanager_id=tm['id']
                        ).inc(records_out)
                        
                        # Records per second
                        records_in_per_second = random.randint(50, 500)
                        records_out_per_second = max(0, records_in_per_second - random.randint(0, 50))
                        
                        self.flink_taskmanager_job_task_numRecordsInPerSecond.labels(
                            job_name=self.job_name,
                            task_name=operator['task'],
                            operator_name=operator['name'],
                            taskmanager_id=tm['id']
                        ).set(records_in_per_second)
                        
                        self.flink_taskmanager_job_task_numRecordsOutPerSecond.labels(
                            job_name=self.job_name,
                            task_name=operator['task'],
                            operator_name=operator['name'],
                            taskmanager_id=tm['id']
                        ).set(records_out_per_second)
                        
                        # Latency
                        latency = random.uniform(1, 100)
                        if operator['name'] == 'window-aggregate':
                            latency = random.uniform(100, 1000)  # Окна медленнее
                        
                        self.flink_taskmanager_job_task_latency.labels(
                            job_name=self.job_name,
                            task_name=operator['task'],
                            operator_name=operator['name']
                        ).observe(latency)
                        
                        # Backpressure
                        backpressure = random.randint(0, 100)
                        if random.random() < 0.05:  # 5% шанс backpressure
                            backpressure = random.randint(200, 500)
                        
                        self.flink_taskmanager_job_task_backPressuredTimeMsPerSecond.labels(
                            job_name=self.job_name,
                            task_name=operator['task'],
                            operator_name=operator['name'],
                            taskmanager_id=tm['id']
                        ).set(backpressure)
                        
                        # State size
                        state_size = random.randint(10 * 1024 * 1024, 100 * 1024 * 1024)  # 10-100 MB
                        if operator['name'] == 'window-aggregate':
                            state_size = random.randint(100 * 1024 * 1024, 500 * 1024 * 1024)  # 100-500 MB
                        
                        self.flink_taskmanager_job_task_stateSize_bytes.labels(
                            job_name=self.job_name,
                            task_name=operator['task'],
                            operator_name=operator['name'],
                            taskmanager_id=tm['id']
                        ).set(state_size)
                
                # Пауза
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Ошибка симуляции операторных метрик: {e}")
                time.sleep(5)
    
    def run(self, port=9250):
        """Запуск симулятора"""
        # Запуск HTTP сервера для Prometheus
        start_http_server(port)
        logger.info(f"Prometheus метрики Flink доступны на порту {port}")
        
        # Запуск потоков симуляции
        threads = []
        
        # Поток метрик JobManager
        jm_thread = threading.Thread(target=self.simulate_jobmanager_metrics, daemon=True)
        jm_thread.start()
        threads.append(jm_thread)
        
        # Поток метрик TaskManager
        tm_thread = threading.Thread(target=self.simulate_taskmanager_metrics, daemon=True)
        tm_thread.start()
        threads.append(tm_thread)
        
        # Поток метрик операторов
        op_thread = threading.Thread(target=self.simulate_operator_metrics, daemon=True)
        op_thread.start()
        threads.append(op_thread)
        
        # Бесконечный цикл основного потока
        try:
            while True:
                time.sleep(60)
                logger.info(f"Симулятор Flink работает. Job: {self.job_name}")
        except KeyboardInterrupt:
            logger.info("Симулятор Flink остановлен")
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")

def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Симулятор метрик Apache Flink')
    parser.add_argument('--port', type=int, default=9250,
                       help='Порт для Prometheus метрик')
    
    args = parser.parse_args()
    
    # Запуск симулятора
    simulator = FlinkMetricsSimulator()
    simulator.run(args.port)

if __name__ == "__main__":
    main()