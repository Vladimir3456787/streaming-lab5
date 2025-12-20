#!/usr/bin/env python3
"""
Мониторинг дрейфа данных для потоковой обработки
Обрабатывает данные из local_data/2019-Oct.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
import logging
import os
import sys
import gc
from typing import Dict, List, Optional, Tuple, Any
import hashlib
import pickle
from collections import defaultdict, Counter

# Flask для веб-интерфейса и метрик
from flask import Flask, jsonify, request, render_template_string
from prometheus_client import CollectorRegistry, Gauge, Counter as PromCounter, Histogram, push_to_gateway, generate_latest

# Evidently для анализа дрейфа
try:
    from evidently.report import Report
    from evidently.metrics import DataDriftTable, DatasetDriftMetric, ColumnDriftMetric
    from evidently.metrics.base_metric import Metric
    from evidently.test_suite import TestSuite
    from evidently.tests import TestNumberOfRows, TestNumberOfColumns, TestColumnsType, TestAllColumnsShareOfMissingValues
    EVIDENTLY_AVAILABLE = True
except ImportError:
    EVIDENTLY_AVAILABLE = False
    print("Evidently не установлен. Установите: pip install evidently")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/data/monitor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Flask приложение
app = Flask(__name__)

class StreamingDataProcessor:
    """Обработчик потоковых данных с мониторингом дрейфа"""
    
    def __init__(self, data_path: str, reference_size: int = 10000, batch_size: int = 1000):
        self.data_path = data_path
        self.reference_size = reference_size
        self.batch_size = batch_size
        self.reference_data = None
        self.reference_stats = {}
        self.current_batch_stats = {}
        self.drift_history = []
        self.processed_records = 0
        self.start_time = datetime.now()
        
        # Инициализация Prometheus метрик
        self.registry = CollectorRegistry()
        
        # Метрики качества данных
        self.data_drift_score = Gauge(
            'data_drift_score',
            'Data drift detection score (0-1)',
            registry=self.registry
        )
        
        self.late_arriving_events = PromCounter(
            'late_arriving_events_total',
            'Total count of late arriving events',
            registry=self.registry
        )
        
        self.data_events_processed = PromCounter(
            'data_events_processed_total',
            'Total count of processed events',
            registry=self.registry
        )
        
        self.data_price_mean = Gauge(
            'data_price_mean',
            'Mean price of processed events',
            registry=self.registry
        )
        
        self.data_price_std = Gauge(
            'data_price_std',
            'Standard deviation of price',
            registry=self.registry
        )
        
        self.data_event_type_count = Gauge(
            'data_event_type_count',
            'Count of events by type',
            ['event_type'],
            registry=self.registry
        )
        
        self.data_category_count = Gauge(
            'data_category_count',
            'Count of events by category',
            ['category_code'],
            registry=self.registry
        )
        
        self.processing_latency = Histogram(
            'processing_latency_seconds',
            'Processing latency in seconds',
            buckets=[0.1, 0.5, 1, 2, 5, 10],
            registry=self.registry
        )
        
        # Загрузка референсных данных
        self._load_reference_data()
        
        logger.info(f"Инициализирован обработчик данных. Референсных записей: {len(self.reference_data)}")
    
    def _load_reference_data(self):
        """Загрузка референсных данных из CSV файла"""
        logger.info(f"Загрузка референсных данных из {self.data_path}")
        
        try:
            # Используем chunksize для обработки больших файлов
            chunks = pd.read_csv(
                self.data_path,
                chunksize=self.reference_size,
                nrows=self.reference_size * 2,  # Берем в 2 раза больше для семплирования
                parse_dates=['event_time'],
                infer_datetime_format=True
            )
            
            # Собираем первые reference_size записей
            self.reference_data = pd.concat([chunk for chunk in chunks]).head(self.reference_size)
            
            # Преобразование типов данных
            self.reference_data['price'] = pd.to_numeric(self.reference_data['price'], errors='coerce')
            
            # Вычисление статистики референсных данных
            self._calculate_reference_stats()
            
            logger.info(f"Загружено {len(self.reference_data)} референсных записей")
            logger.info(f"Колонки: {list(self.reference_data.columns)}")
            
            # Сохранение референсных данных для отладки
            self.reference_data.to_csv('/app/data/reference_data.csv', index=False)
            
        except Exception as e:
            logger.error(f"Ошибка загрузки референсных данных: {e}")
            # Создание минимального датасета для продолжения работы
            self.reference_data = pd.DataFrame({
                'event_time': [datetime.now()],
                'event_type': ['view'],
                'product_id': ['0'],
                'category_id': ['0'],
                'category_code': ['unknown'],
                'brand': ['unknown'],
                'price': [0.0],
                'user_id': ['0'],
                'user_session': ['0']
            })
    
    def _calculate_reference_stats(self):
        """Вычисление статистики референсных данных"""
        if self.reference_data is None or self.reference_data.empty:
            return
        
        # Статистика по числовым полям
        numeric_stats = {}
        if 'price' in self.reference_data.columns:
            price_series = self.reference_data['price'].dropna()
            if not price_series.empty:
                numeric_stats['price'] = {
                    'mean': float(price_series.mean()),
                    'std': float(price_series.std()),
                    'min': float(price_series.min()),
                    'max': float(price_series.max()),
                    'percentiles': {
                        p: float(price_series.quantile(p/100))
                        for p in [5, 25, 50, 75, 95]
                    }
                }
        
        # Распределение категориальных полей
        categorical_stats = {}
        
        if 'event_type' in self.reference_data.columns:
            event_type_dist = self.reference_data['event_type'].value_counts(normalize=True).to_dict()
            categorical_stats['event_type'] = event_type_dist
        
        if 'category_code' in self.reference_data.columns:
            # Берем топ-20 категорий
            category_dist = self.reference_data['category_code'].fillna('unknown').value_counts(normalize=True).head(20).to_dict()
            categorical_stats['category_code'] = category_dist
        
        if 'brand' in self.reference_data.columns:
            brand_dist = self.reference_data['brand'].fillna('unknown').value_counts(normalize=True).head(20).to_dict()
            categorical_stats['brand'] = brand_dist
        
        # Сохранение статистики
        self.reference_stats = {
            'numeric': numeric_stats,
            'categorical': categorical_stats,
            'total_records': len(self.reference_data),
            'column_types': str(self.reference_data.dtypes.to_dict())
        }
        
        # Сохранение статистики в файл
        with open('/app/data/reference_stats.json', 'w') as f:
            json.dump(self.reference_stats, f, indent=2, default=str)
        
        logger.info(f"Рассчитана статистика по {len(self.reference_data)} референсным записям")
    
    def _calculate_lightweight_drift(self, current_batch: pd.DataFrame) -> float:
        """
        Облегченный расчет дрейфа данных без использования тяжелых библиотек
        Возвращает оценку дрейфа от 0 до 1
        """
        drift_scores = []
        
        # 1. Проверка дрейфа числовых признаков (цена)
        if 'price' in current_batch.columns and 'price' in self.reference_stats.get('numeric', {}):
            current_price = current_batch['price'].dropna()
            ref_stats = self.reference_stats['numeric']['price']
            
            if len(current_price) >= 10:  # Минимальное количество для анализа
                # Z-тест для среднего значения
                current_mean = current_price.mean()
                ref_mean = ref_stats['mean']
                ref_std = max(ref_stats['std'], 0.001)  # Избегаем деления на 0
                
                z_score_mean = abs(current_mean - ref_mean) / (ref_std / np.sqrt(len(current_price)))
                score_mean = min(z_score_mean / 5.0, 1.0)  # Нормализация
                drift_scores.append(score_mean)
                
                # KS-тест для распределения (через квантили)
                ref_percentiles = list(ref_stats['percentiles'].values())
                try:
                    current_percentiles = [float(current_price.quantile(p/100)) for p in [5, 25, 50, 75, 95]]
                    ks_statistic = max(np.abs(np.array(ref_percentiles) - np.array(current_percentiles)))
                    score_dist = min(ks_statistic / (ref_stats['max'] - ref_stats['min']), 1.0)
                    drift_scores.append(score_dist)
                except:
                    pass
        
        # 2. Проверка дрейфа категориальных признаков
        for column in ['event_type', 'category_code', 'brand']:
            if column in self.reference_stats.get('categorical', {}) and column in current_batch.columns:
                ref_dist = self.reference_stats['categorical'][column]
                current_series = current_batch[column].fillna('unknown')
                
                if len(current_series) >= 10:
                    # Вычисляем распределение в текущем батче
                    current_counts = current_series.value_counts(normalize=True).to_dict()
                    
                    # Дивергенция Дженсена-Шеннона
                    js_score = self._jensen_shannon_divergence(ref_dist, current_counts)
                    drift_scores.append(js_score)
        
        # 3. Проверка распределения времени событий
        if 'event_time' in current_batch.columns:
            try:
                current_batch['event_hour'] = pd.to_datetime(current_batch['event_time']).dt.hour
                hour_dist_current = current_batch['event_hour'].value_counts(normalize=True).sort_index()
                
                # Сравнение с равномерным распределением (базовый тест)
                uniform_diff = np.abs(hour_dist_current - 1/24).mean()
                drift_scores.append(min(uniform_diff * 10, 1.0))
            except:
                pass
        
        # Возвращаем средний дрейф или 0 если не удалось вычислить
        return float(np.mean(drift_scores)) if drift_scores else 0.0
    
    def _jensen_shannon_divergence(self, p_dist: Dict, q_dist: Dict) -> float:
        """Расчет дивергенции Дженсена-Шеннона между двумя распределениями"""
        # Объединяем все ключи из обоих распределений
        all_keys = set(p_dist.keys()) | set(q_dist.keys())
        
        # Создаем нормализованные векторы распределений
        p = np.zeros(len(all_keys))
        q = np.zeros(len(all_keys))
        
        for i, key in enumerate(all_keys):
            p[i] = p_dist.get(key, 1e-10)
            q[i] = q_dist.get(key, 1e-10)
        
        # Нормализация
        p = p / p.sum()
        q = q / q.sum()
        
        # Среднее распределение
        m = 0.5 * (p + q)
        
        # KL дивергенция (с защитой от log(0))
        epsilon = 1e-10
        kl_pm = np.sum(p * np.log((p + epsilon) / (m + epsilon)))
        kl_qm = np.sum(q * np.log((q + epsilon) / (m + epsilon)))
        
        # JS дивергенция
        js = 0.5 * (kl_pm + kl_qm)
        
        # Ограничиваем значение от 0 до 1
        return min(max(js, 0), 1)
    
    def _evidently_drift_analysis(self, current_batch: pd.DataFrame) -> Dict:
        """Детальный анализ дрейфа с использованием Evidently"""
        if not EVIDENTLY_AVAILABLE or self.reference_data is None or len(current_batch) < 100:
            return {}
        
        try:
            # Семплирование для экономии памяти
            ref_sample = self.reference_data.sample(min(5000, len(self.reference_data)))
            current_sample = current_batch.sample(min(5000, len(current_batch)))
            
            # Создание отчета о дрейфе данных
            drift_report = Report(metrics=[
                DataDriftTable(),
                DatasetDriftMetric()
            ])
            
            drift_report.run(
                reference_data=ref_sample,
                current_data=current_sample
            )
            
            # Извлечение результатов
            report_dict = drift_report.as_dict()
            
            # Сохранение отчета в HTML
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_path = f"/app/data/drift_report_{timestamp}.html"
            drift_report.save_html(report_path)
            
            logger.info(f"Детальный отчет о дрейфе сохранен: {report_path}")
            
            # Извлечение оценки дрейфа
            drift_score = 0.0
            if report_dict.get('metrics'):
                for metric in report_dict['metrics']:
                    if metric.get('metric') == 'DatasetDriftMetric':
                        drift_score = metric.get('result', {}).get('dataset_drift', 0.0)
                        break
            
            return {
                'drift_score': drift_score,
                'report_path': report_path,
                'details': report_dict
            }
            
        except Exception as e:
            logger.error(f"Ошибка анализа дрейфа с Evidently: {e}")
            return {}
    
    def process_batch(self, batch_data: pd.DataFrame) -> Dict:
        """
        Обработка батча данных и вычисление метрик
        Возвращает словарь с результатами
        """
        start_time = time.time()
        results = {
            'processed_records': len(batch_data),
            'drift_score': 0.0,
            'evidently_drift_score': 0.0,
            'late_events': 0,
            'price_stats': {},
            'warnings': [],
            'processing_time': 0
        }
        
        try:
            # Проверка на поздние события (более 1 часа)
            if 'event_time' in batch_data.columns:
                try:
                    current_time = datetime.now()
                    batch_data['event_time_parsed'] = pd.to_datetime(batch_data['event_time'])
                    time_threshold = current_time - timedelta(hours=1)
                    
                    late_events = (batch_data['event_time_parsed'] < time_threshold).sum()
                    results['late_events'] = int(late_events)
                    
                    # Обновление метрики
                    self.late_arriving_events.inc(late_events)
                except Exception as e:
                    logger.warning(f"Ошибка проверки поздних событий: {e}")
            
            # Обновление счетчика обработанных событий
            self.data_events_processed.inc(len(batch_data))
            self.processed_records += len(batch_data)
            
            # Расчет статистики по цене
            if 'price' in batch_data.columns:
                price_series = batch_data['price'].dropna()
                if not price_series.empty:
                    results['price_stats'] = {
                        'mean': float(price_series.mean()),
                        'std': float(price_series.std()),
                        'min': float(price_series.min()),
                        'max': float(price_series.max()),
                        'count': int(len(price_series))
                    }
                    
                    # Обновление метрик Prometheus
                    self.data_price_mean.set(results['price_stats']['mean'])
                    self.data_price_std.set(results['price_stats']['std'])
            
            # Обновление счетчиков по типам событий
            if 'event_type' in batch_data.columns:
                event_counts = batch_data['event_type'].value_counts().to_dict()
                for event_type, count in event_counts.items():
                    self.data_event_type_count.labels(event_type=str(event_type)).set(count)
            
            # Обновление счетчиков по категориям
            if 'category_code' in batch_data.columns:
                category_counts = batch_data['category_code'].fillna('unknown').value_counts().head(20).to_dict()
                for category, count in category_counts.items():
                    self.data_category_count.labels(category_code=str(category)).set(count)
            
            # Расчет дрейфа данных
            if len(batch_data) >= 50:  # Минимальный размер для анализа дрейфа
                # Легковесный расчет дрейфа
                drift_score = self._calculate_lightweight_drift(batch_data)
                results['drift_score'] = drift_score
                
                # Обновление метрики Prometheus
                self.data_drift_score.set(drift_score)
                
                # Детальный анализ с Evidently если дрейф высокий
                if drift_score > 0.1 and EVIDENTLY_AVAILABLE:
                    evidently_results = self._evidently_drift_analysis(batch_data)
                    if evidently_results:
                        results['evidently_drift_score'] = evidently_results.get('drift_score', 0.0)
                        results['evidently_report'] = evidently_results.get('report_path')
                
                # Проверка порогов и предупреждения
                if drift_score > 0.2:
                    results['warnings'].append(f"Высокий дрейф данных: {drift_score:.3f}")
                elif drift_score > 0.1:
                    results['warnings'].append(f"Умеренный дрейф данных: {drift_score:.3f}")
            
            # Сохранение в историю
            self.drift_history.append({
                'timestamp': datetime.now().isoformat(),
                'drift_score': results['drift_score'],
                'batch_size': len(batch_data),
                'processed_total': self.processed_records
            })
            
            # Ограничение размера истории
            if len(self.drift_history) > 1000:
                self.drift_history = self.drift_history[-1000:]
            
            # Отправка метрик в Prometheus Pushgateway
            try:
                push_to_gateway(
                    os.getenv('PUSHGATEWAY_URL', 'localhost:9091'),
                    job='data_drift_monitor',
                    registry=self.registry,
                    grouping_key={'instance': 'drift-monitor'}
                )
            except Exception as e:
                logger.warning(f"Не удалось отправить метрики в Pushgateway: {e}")
            
            # Время обработки
            processing_time = time.time() - start_time
            results['processing_time'] = processing_time
            self.processing_latency.observe(processing_time)
            
            # Логирование
            if results['warnings']:
                for warning in results['warnings']:
                    logger.warning(warning)
            
            logger.info(f"Обработан батч: {len(batch_data)} записей, дрейф: {drift_score:.3f}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки батча: {e}")
            results['error'] = str(e)
        
        return results
    
    def get_status(self) -> Dict:
        """Получение статуса обработчика"""
        uptime = datetime.now() - self.start_time
        
        return {
            'status': 'running',
            'start_time': self.start_time.isoformat(),
            'uptime_seconds': uptime.total_seconds(),
            'processed_records': self.processed_records,
            'reference_records': len(self.reference_data) if self.reference_data is not None else 0,
            'drift_history_count': len(self.drift_history),
            'last_drift_score': self.drift_history[-1]['drift_score'] if self.drift_history else 0,
            'memory_usage_mb': self._get_memory_usage()
        }
    
    def _get_memory_usage(self) -> float:
        """Получение использования памяти процессом"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0

class DataStreamSimulator:
    """Симулятор потоковых данных из файла"""
    
    def __init__(self, data_path: str, batch_size: int = 1000, speed_factor: float = 1.0):
        self.data_path = data_path
        self.batch_size = batch_size
        self.speed_factor = speed_factor
        self.file_position = 0
        self.total_records = self._count_records()
        
        logger.info(f"Инициализирован симулятор данных. Всего записей: {self.total_records}")
    
    def _count_records(self) -> int:
        """Подсчет общего количества записей в файле"""
        try:
            # Используем wc -l для быстрого подсчета строк
            import subprocess
            result = subprocess.run(
                ['wc', '-l', self.data_path],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                count = int(result.stdout.strip().split()[0]) - 1  # Минус заголовок
                return count
        except:
            pass
        
        # Медленный способ если wc не доступен
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f) - 1
        except:
            return 0
    
    def get_next_batch(self) -> Optional[pd.DataFrame]:
        """Получение следующего батча данных"""
        try:
            # Чтение батча с пропуском уже прочитанных строк
            df = pd.read_csv(
                self.data_path,
                skiprows=range(1, self.file_position + 1),  # +1 для заголовка
                nrows=self.batch_size,
                parse_dates=['event_time'],
                infer_datetime_format=True
            )
            
            if df.empty:
                # Достигнут конец файла, начинаем сначала
                self.file_position = 0
                logger.info("Достигнут конец файла, начинаем сначала")
                return self.get_next_batch()
            
            # Обновление позиции
            self.file_position += len(df)
            
            # Преобразование типов
            if 'price' in df.columns:
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
            
            # Добавление случайных изменений для симуляции дрейфа
            df = self._simulate_drift(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка чтения батча: {e}")
            return None
    
    def _simulate_drift(self, df: pd.DataFrame) -> pd.DataFrame:
        """Симуляция дрейфа данных для тестирования"""
        import random
        
        # С вероятностью 5% добавляем дрейф
        if random.random() < 0.05:
            # Изменяем цены
            if 'price' in df.columns:
                drift_factor = random.uniform(0.8, 1.2)
                df['price'] = df['price'] * drift_factor
            
            # С вероятностью 50% меняем распределение типов событий
            if random.random() < 0.5 and 'event_type' in df.columns:
                # Увеличиваем долю определенного типа событий
                event_types = df['event_type'].unique()
                if len(event_types) > 1:
                    boosted_type = random.choice(event_types)
                    mask = df['event_type'] == boosted_type
                    if mask.any():
                        # Дублируем часть событий этого типа
                        boost_count = int(len(df) * 0.1)  # 10% буст
                        boosted_rows = df[mask].sample(min(boost_count, mask.sum()), replace=True)
                        df = pd.concat([df, boosted_rows])
        
        return df
    
    def get_progress(self) -> Dict:
        """Получение прогресса чтения файла"""
        return {
            'file_position': self.file_position,
            'total_records': self.total_records,
            'progress_percent': (self.file_position / max(self.total_records, 1)) * 100
        }

# Глобальные объекты
processor = None
simulator = None

@app.route('/')
def index():
    """Главная страница веб-интерфейса"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Flink Streaming Data Drift Monitor</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .container { max-width: 1200px; margin: 0 auto; }
            .status { background: #f5f5f5; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
            .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .metric-card { background: white; border: 1px solid #ddd; border-radius: 5px; padding: 15px; }
            .warning { color: #ff9800; font-weight: bold; }
            .critical { color: #f44336; font-weight: bold; }
            .ok { color: #4caf50; font-weight: bold; }
            .chart { width: 100%; height: 300px; margin: 20px 0; }
            button { padding: 10px 20px; background: #2196f3; color: white; border: none; border-radius: 4px; cursor: pointer; }
            button:hover { background: #0b7dda; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Flink Streaming Data Drift Monitor</h1>
            <div class="status">
                <h2>Статус системы</h2>
                <div id="status"></div>
            </div>
            <div class="metrics">
                <div class="metric-card">
                    <h3>Data Drift Score</h3>
                    <div id="drift-score">Загрузка...</div>
                </div>
                <div class="metric-card">
                    <h3>Обработано записей</h3>
                    <div id="processed">Загрузка...</div>
                </div>
                <div class="metric-card">
                    <h3>Поздние события</h3>
                    <div id="late-events">Загрузка...</div>
                </div>
                <div class="metric-card">
                    <h3>Статус</h3>
                    <div id="system-status">Загрузка...</div>
                </div>
            </div>
            <div>
                <h2>Управление</h2>
                <button onclick="startProcessing()">Старт обработки</button>
                <button onclick="stopProcessing()">Стоп обработки</button>
                <button onclick="forceDriftCheck()">Проверить дрейф</button>
            </div>
            <div id="warnings"></div>
            <div id="chart"></div>
        </div>
        <script>
            async function updateStatus() {
                const response = await fetch('/status');
                const data = await response.json();
                
                document.getElementById('status').innerHTML = `
                    <p>Запущен: ${data.start_time}</p>
                    <p>Аптайм: ${Math.round(data.uptime_seconds)} секунд</p>
                    <p>Референсных записей: ${data.reference_records}</p>
                    <p>Обработано: ${data.processed_records}</p>
                    <p>Использование памяти: ${data.memory_usage_mb.toFixed(2)} MB</p>
                `;
                
                document.getElementById('processed').textContent = data.processed_records.toLocaleString();
                document.getElementById('system-status').innerHTML = `<span class="ok">✓ Работает</span>`;
            }
            
            async function updateMetrics() {
                const response = await fetch('/metrics/latest');
                const data = await response.json();
                
                const driftScore = data.drift_score || 0;
                let driftClass = 'ok';
                if (driftScore > 0.2) driftClass = 'critical';
                else if (driftScore > 0.1) driftClass = 'warning';
                
                document.getElementById('drift-score').innerHTML = `
                    <span class="${driftClass}">${driftScore.toFixed(3)}</span>
                    ${driftScore > 0.2 ? '⚠️ ВЫСОКИЙ ДРЕЙФ' : ''}
                `;
                
                document.getElementById('late-events').textContent = data.late_events || 0;
                
                // Обновление предупреждений
                const warningsDiv = document.getElementById('warnings');
                if (data.warnings && data.warnings.length > 0) {
                    warningsDiv.innerHTML = '<h3>Предупреждения:</h3>' + 
                        data.warnings.map(w => `<p class="warning">⚠️ ${w}</p>`).join('');
                } else {
                    warningsDiv.innerHTML = '';
                }
            }
            
            async function startProcessing() {
                await fetch('/start', { method: 'POST' });
                alert('Обработка запущена');
            }
            
            async function stopProcessing() {
                await fetch('/stop', { method: 'POST' });
                alert('Обработка остановлена');
            }
            
            async function forceDriftCheck() {
                const response = await fetch('/check-drift', { method: 'POST' });
                const result = await response.json();
                alert(`Проверка дрейфа выполнена. Результат: ${result.drift_score.toFixed(3)}`);
            }
            
            // Обновление каждые 5 секунд
            setInterval(() => {
                updateStatus();
                updateMetrics();
            }, 5000);
            
            // Первоначальная загрузка
            updateStatus();
            updateMetrics();
        </script>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route('/status')
def status():
    """Получение статуса системы"""
    if processor is None:
        return jsonify({'error': 'Processor not initialized'}), 500
    
    status_data = processor.get_status()
    
    if simulator is not None:
        progress = simulator.get_progress()
        status_data.update(progress)
    
    return jsonify(status_data)

@app.route('/metrics/latest')
def latest_metrics():
    """Получение последних метрик"""
    if processor is None or not processor.drift_history:
        return jsonify({'error': 'No metrics available'}), 404
    
    latest = processor.drift_history[-1] if processor.drift_history else {}
    return jsonify(latest)

@app.route('/metrics/prometheus')
def prometheus_metrics():
    """Эндпоинт метрик для Prometheus"""
    if processor is None:
        return '', 204
    
    return generate_latest(processor.registry)

@app.route('/start', methods=['POST'])
def start_processing():
    """Запуск обработки данных"""
    global processor, simulator
    
    if processor is None:
        data_path = os.getenv('DATA_PATH', '/app/local_data/2019-Oct.csv')
        processor = StreamingDataProcessor(
            data_path=data_path,
            reference_size=int(os.getenv('REFERENCE_SAMPLE_SIZE', 10000)),
            batch_size=int(os.getenv('BATCH_SIZE', 1000))
        )
    
    if simulator is None:
        simulator = DataStreamSimulator(
            data_path=os.getenv('DATA_PATH', '/app/local_data/2019-Oct.csv'),
            batch_size=int(os.getenv('BATCH_SIZE', 1000))
        )
    
    # Запуск фоновой обработки
    import threading
    thread = threading.Thread(target=process_stream, daemon=True)
    thread.start()
    
    return jsonify({'status': 'started'})

@app.route('/stop', methods=['POST'])
def stop_processing():
    """Остановка обработки данных"""
    # В реальной системе здесь должна быть логика остановки
    return jsonify({'status': 'stopped'})

@app.route('/check-drift', methods=['POST'])
def check_drift():
    """Принудительная проверка дрейфа"""
    if processor is None or simulator is None:
        return jsonify({'error': 'System not initialized'}), 400
    
    batch = simulator.get_next_batch()
    if batch is not None:
        results = processor.process_batch(batch)
        return jsonify(results)
    
    return jsonify({'error': 'No data available'}), 400

def process_stream():
    """Фоновая обработка потока данных"""
    global processor, simulator
    
    if processor is None or simulator is None:
        logger.error("Processor or simulator not initialized")
        return
    
    logger.info("Запуск фоновой обработки потока данных")
    
    while True:
        try:
            # Получение следующего батча
            batch = simulator.get_next_batch()
            
            if batch is not None:
                # Обработка батча
                processor.process_batch(batch)
            
            # Пауза между батчами
            time.sleep(5)  # 5 секунд между батчами
            
        except KeyboardInterrupt:
            logger.info("Обработка остановлена пользователем")
            break
        except Exception as e:
            logger.error(f"Ошибка в потоке обработки: {e}")
            time.sleep(10)

if __name__ == "__main__":
    # Создание директории для данных
    os.makedirs('/app/data', exist_ok=True)
    
    # Запуск Flask приложения
    app.run(host='0.0.0.0', port=5000, debug=False)