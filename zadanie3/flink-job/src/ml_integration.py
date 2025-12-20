import json
import asyncio
import aiohttp
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import time

from pyflink.datastream.functions import RuntimeContext, ProcessFunction
from pyflink.datastream.state import (
    ValueStateDescriptor, 
    ValueState,
    MapStateDescriptor,
    MapState
)
from pyflink.common.typeinfo import Types

logger = logging.getLogger(__name__)

@dataclass
class MLPrediction:
    user_id: str
    product_id: str
    score: float
    latency_ms: float
    cached: bool
    timestamp: int

class MLInferenceFunction(ProcessFunction):
    """
    ProcessFunction для интеграции с ML-сервисом
    Реализует кэширование, батчинг и обработку ошибок
    """
    
    def __init__(self, ml_service_url: str, cache_ttl_ms: int = 300000):
        self.ml_service_url = ml_service_url
        self.cache_ttl_ms = cache_ttl_ms
        self.prediction_cache = None
        self.http_session = None
        self.batch_queue = []
        self.batch_size = 32
        self.max_batch_wait_ms = 10
        
    def open(self, runtime_context: RuntimeContext):
        # Инициализация state для кэша предсказаний
        cache_descriptor = MapStateDescriptor(
            "prediction_cache",
            Types.STRING(),  # Ключ: user_id:product_id
            Types.PICKLED_BYTE_ARRAY()  # Значение: (score, timestamp)
        )
        self.prediction_cache = runtime_context.get_map_state(cache_descriptor)
        
        # Инициализация HTTP клиента
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=2.0)
        )
        
        # Инициализация метрик
        self.metrics = {
            "total_predictions": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "batch_predictions": 0,
            "errors": 0
        }
        
        logger.info(f"ML Inference Function initialized with service: {self.ml_service_url}")
    
    def close(self):
        if self.http_session:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.http_session.close())
        
        logger.info(f"ML Inference metrics: {self.metrics}")
    
    def process_element(self, value, ctx, out):
        user_id = value["user_id"]
        product_id = value["product_id"]
        current_time = ctx.timestamp()
        
        # Проверка кэша
        cache_key = f"{user_id}:{product_id}"
        cached_value = self.prediction_cache.get(cache_key)
        
        if cached_value:
            score, timestamp = cached_value
            if current_time - timestamp < self.cache_ttl_ms:
                # Используем кэшированное значение
                self.metrics["cache_hits"] += 1
                prediction = MLPrediction(
                    user_id=user_id,
                    product_id=product_id,
                    score=score,
                    latency_ms=0.1,  # Минимальная задержка для кэша
                    cached=True,
                    timestamp=current_time
                )
                out.collect(prediction)
                return
        
        self.metrics["cache_misses"] += 1
        
        # Добавляем в batch очередь
        self.batch_queue.append({
            "user_id": user_id,
            "product_id": product_id,
            "context": value.get("context", {}),
            "out": out,
            "timestamp": current_time,
            "cache_key": cache_key
        })
        
        # Если набрался batch или прошло много времени, делаем запрос
        if len(self.batch_queue) >= self.batch_size:
            self._process_batch()
    
    def _process_batch(self):
        """Обработка batch запроса"""
        if not self.batch_queue:
            return
        
        # Формируем batch запрос
        batch_data = []
        batch_items = []
        
        for item in self.batch_queue:
            batch_data.append({
                "user_id": item["user_id"],
                "product_id": item["product_id"],
                "context": item["context"]
            })
            batch_items.append(item)
        
        # Очищаем очередь
        self.batch_queue = []
        
        # Выполняем асинхронный запрос
        loop = asyncio.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._async_batch_predict(batch_data, batch_items), 
            loop
        )
        
        try:
            # Ждем результат с таймаутом
            future.result(timeout=3.0)
        except Exception as e:
            logger.error(f"Batch prediction failed: {e}")
            self.metrics["errors"] += 1
            # Fallback: используем дефолтные значения
            for item in batch_items:
                prediction = MLPrediction(
                    user_id=item["user_id"],
                    product_id=item["product_id"],
                    score=0.5,  # Дефолтный score
                    latency_ms=3000,  # Большая задержка для ошибки
                    cached=False,
                    timestamp=item["timestamp"]
                )
                item["out"].collect(prediction)
    
    async def _async_batch_predict(self, batch_data: List[Dict], batch_items: List[Dict]):
        """Асинхронный batch prediction"""
        start_time = time.time()
        
        try:
            async with self.http_session.post(
                f"{self.ml_service_url}/predict",
                json={
                    "user_ids": [item["user_id"] for item in batch_data],
                    "product_ids": [item["product_id"] for item in batch_data],
                    "context": batch_data[0]["context"] if batch_data else {}
                }
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    predictions = result["predictions"]
                    latency_ms = result["metadata"]["latency_ms"]
                    
                    # Обрабатываем результаты
                    for i, (item, score) in enumerate(zip(batch_items, predictions)):
                        # Обновляем кэш
                        self.prediction_cache.put(
                            item["cache_key"],
                            (float(score), item["timestamp"])
                        )
                        
                        # Создаем prediction
                        prediction = MLPrediction(
                            user_id=item["user_id"],
                            product_id=item["product_id"],
                            score=float(score),
                            latency_ms=latency_ms,
                            cached=False,
                            timestamp=item["timestamp"]
                        )
                        
                        item["out"].collect(prediction)
                        self.metrics["total_predictions"] += 1
                        self.metrics["batch_predictions"] += 1
                
                else:
                    error_text = await response.text()
                    raise Exception(f"ML service error: {error_text}")
                    
        except Exception as e:
            logger.error(f"Async prediction error: {e}")
            raise