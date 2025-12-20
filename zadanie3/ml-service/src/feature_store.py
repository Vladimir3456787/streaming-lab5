import redis.asyncio as redis
import json
import asyncio
import numpy as np
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class FeatureStore:
    """Feature Store на основе Redis для online инференса"""
    
    def __init__(self):
        self.redis_client = None
        self.cache_stats = {"hits": 0, "misses": 0}
        
    async def connect(self):
        """Подключение к Redis"""
        self.redis_client = redis.Redis(
            host="redis",
            port=6379,
            decode_responses=False,
            max_connections=50
        )
        logger.info("Connected to Redis feature store")
    
    async def get_user_features(self, user_id: str) -> Dict[str, Any]:
        """
        Получение фич пользователя из Feature Store
        """
        cache_key = f"user:{user_id}:features"
        
        # Проверка кэша
        cached = await self.redis_client.get(cache_key)
        if cached:
            self.cache_stats["hits"] += 1
            return json.loads(cached)
        
        self.cache_stats["misses"] += 1
        
        # Если нет в кэше, вычисляем фичи
        features = await self._compute_user_features(user_id)
        
        # Сохраняем в кэш (TTL 5 минут)
        await self.redis_client.setex(
            cache_key,
            timedelta(minutes=5),
            json.dumps(features)
        )
        
        return features
    
    async def get_user_features_batch(self, user_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Batch получение фич пользователей
        """
        # Используем pipeline для оптимизации
        pipe = self.redis_client.pipeline()
        cache_keys = [f"user:{uid}:features" for uid in user_ids]
        
        for key in cache_keys:
            pipe.get(key)
        
        cached_results = await pipe.execute()
        
        results = []
        compute_tasks = []
        
        for i, (user_id, cached) in enumerate(zip(user_ids, cached_results)):
            if cached:
                self.cache_stats["hits"] += 1
                results.append(json.loads(cached))
            else:
                self.cache_stats["misses"] += 1
                results.append(None)
                compute_tasks.append(self._compute_user_features(user_id))
        
        # Параллельно вычисляем недостающие фичи
        if compute_tasks:
            computed_features = await asyncio.gather(*compute_tasks)
            
            # Заполняем результаты и обновляем кэш
            update_pipe = self.redis_client.pipeline()
            computed_idx = 0
            
            for i, result in enumerate(results):
                if result is None:
                    features = computed_features[computed_idx]
                    results[i] = features
                    computed_idx += 1
                    
                    # Сохраняем в кэш
                    cache_key = cache_keys[i]
                    update_pipe.setex(
                        cache_key,
                        timedelta(minutes=5),
                        json.dumps(features)
                    )
            
            await update_pipe.execute()
        
        return results
    
    async def get_product_features(self, product_id: str) -> Dict[str, Any]:
        """Получение фич товара"""
        cache_key = f"product:{product_id}:features"
        
        cached = await self.redis_client.get(cache_key)
        if cached:
            return json.loads(cached)
        
        # Статические фичи товара (в реальности из БД)
        features = {
            "product_id": product_id,
            "price": 99.99,
            "category_id": 1,
            "popularity_score": 0.85,
            "rating": 4.5,
            "click_rate": 0.12
        }
        
        await self.redis_client.setex(
            cache_key,
            timedelta(hours=1),  # Долгий TTL, так как фичи товара редко меняются
            json.dumps(features)
        )
        
        return features
    
    async def get_product_features_batch(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        """Batch получение фич товаров"""
        return await asyncio.gather(
            *[self.get_product_features(pid) for pid in product_ids]
        )
    
    async def _compute_user_features(self, user_id: str) -> Dict[str, Any]:
        """
        Вычисление фич пользователя в реальном времени
        """
        # Получаем историю транзакций
        transactions_key = f"user:{user_id}:transactions"
        transactions = await self.redis_client.lrange(transactions_key, 0, 99)
        
        # Вычисляем статистики
        if transactions:
            amounts = [json.loads(t)["amount"] for t in transactions]
            timestamps = [json.loads(t)["timestamp"] for t in transactions]
            
            # Временные характеристики
            recent_transactions = [
                t for t in transactions 
                if time.time() - json.loads(t)["timestamp"] < 3600  # Последний час
            ]
            
            features = {
                "user_id": user_id,
                "avg_transaction_amount": float(np.mean(amounts)),
                "total_transactions": len(transactions),
                "recent_transaction_count": len(recent_transactions),
                "transaction_velocity": len(recent_transactions) / 3600,  # транзакций в секунду
                "preferred_category": self._get_preferred_category(transactions),
                "session_duration_avg": 300,  # Пример
                "days_since_last_purchase": self._days_since_last(timestamps)
            }
        else:
            # Фичи по умолчанию для нового пользователя
            features = {
                "user_id": user_id,
                "avg_transaction_amount": 0.0,
                "total_transactions": 0,
                "recent_transaction_count": 0,
                "transaction_velocity": 0.0,
                "preferred_category": "unknown",
                "session_duration_avg": 0,
                "days_since_last_purchase": 999
            }
        
        return features
    
    def _get_preferred_category(self, transactions) -> str:
        """Определение предпочитаемой категории"""
        if not transactions:
            return "unknown"
        
        categories = [json.loads(t).get("category", "unknown") for t in transactions]
        from collections import Counter
        return Counter(categories).most_common(1)[0][0]
    
    def _days_since_last(self, timestamps) -> float:
        """Дней с последней активности"""
        if not timestamps:
            return 999.0
        latest = max(timestamps)
        return (time.time() - latest) / 86400
    
    async def cache_prediction(self, key: str, prediction: float, ttl: int = 300):
        """Кэширование предсказания"""
        await self.redis_client.setex(key, ttl, str(prediction))
    
    async def get_cached_prediction(self, key: str) -> Optional[float]:
        """Получение кэшированного предсказания"""
        cached = await self.redis_client.get(key)
        if cached:
            self.cache_stats["hits"] += 1
            return float(cached)
        self.cache_stats["misses"] += 1
        return None
    
    def get_cache_stats(self) -> Dict[str, float]:
        """Статистика кэша"""
        total = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_ratio = self.cache_stats["hits"] / total if total > 0 else 0
        return {
            "hit_ratio": hit_ratio,
            "hits": self.cache_stats["hits"],
            "misses": self.cache_stats["misses"]
        }