import time
import numpy as np
from collections import deque
from typing import Dict, List
import threading
import logging

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Сборщик метрик производительности"""
    
    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        self.latencies = deque(maxlen=window_size)
        self.batch_sizes = deque(maxlen=window_size)
        self.errors = deque(maxlen=window_size)
        self.cache_hits = 0
        self.cache_misses = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
    
    def record_prediction(self, latency_ms: float, batch_size: int = 1):
        """Запись метрик предсказания"""
        with self.lock:
            self.latencies.append(latency_ms)
            self.batch_sizes.append(batch_size)
    
    def record_error(self):
        """Запись ошибки"""
        with self.lock:
            self.errors.append(time.time())
    
    def record_cache_hit(self):
        """Запись попадания в кэш"""
        with self.lock:
            self.cache_hits += 1
    
    def record_cache_miss(self):
        """Запись промаха кэша"""
        with self.lock:
            self.cache_misses += 1
    
    def get_stats(self) -> Dict[str, float]:
        """Получение статистики"""
        with self.lock:
            if not self.latencies:
                return {
                    "p50": 0.0,
                    "p95": 0.0,
                    "p99": 0.0,
                    "mean": 0.0,
                    "throughput": 0.0,
                    "error_rate": 0.0,
                    "cache_hit_rate": 0.0,
                    "total_predictions": 0
                }
            
            latencies_array = np.array(self.latencies)
            
            # Удаляем выбросы (более 3 стандартных отклонений)
            mean = np.mean(latencies_array)
            std = np.std(latencies_array)
            filtered = latencies_array[latencies_array < mean + 3 * std]
            
            if len(filtered) == 0:
                filtered = latencies_array
            
            # Рассчитываем процентили
            p50 = np.percentile(filtered, 50)
            p95 = np.percentile(filtered, 95)
            p99 = np.percentile(filtered, 99)
            
            # Throughput (запросов в секунду)
            total_time = time.time() - self.start_time
            throughput = len(self.latencies) / total_time if total_time > 0 else 0
            
            # Error rate
            error_rate = len(self.errors) / max(len(self.latencies), 1)
            
            # Cache hit rate
            total_cache = self.cache_hits + self.cache_misses
            cache_hit_rate = self.cache_hits / total_cache if total_cache > 0 else 0
            
            return {
                "p50": float(p50),
                "p95": float(p95),
                "p99": float(p99),
                "mean": float(np.mean(filtered)),
                "std": float(std),
                "throughput": float(throughput),
                "error_rate": float(error_rate),
                "cache_hit_rate": float(cache_hit_rate),
                "total_predictions": len(self.latencies)
            }
    
    def reset(self):
        """Сброс статистики"""
        with self.lock:
            self.latencies.clear()
            self.batch_sizes.clear()
            self.errors.clear()
            self.cache_hits = 0
            self.cache_misses = 0
            self.start_time = time.time()