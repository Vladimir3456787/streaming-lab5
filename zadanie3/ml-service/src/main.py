from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import redis
import numpy as np
import time
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Модель для запроса
class PredictionRequest(BaseModel):
    user_ids: List[str]
    product_ids: List[str]

class SinglePredictionRequest(BaseModel):
    user_id: str
    product_id: str

# Инициализация Redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

app = FastAPI()

# Простая модель для предсказания (заглушка)
class SimpleModel:
    def predict(self, user_id: str, product_id: str) -> float:
        # В реальной системе здесь бы была сложная модель
        # Сейчас используем хэш для генерации "случайного" но детерминированного числа
        combined = f"{user_id}_{product_id}"
        hash_val = hash(combined) % 1000
        return float(hash_val / 1000.0)

model = SimpleModel()

@app.get("/")
async def root():
    return {"message": "ML Service for Assignment 3"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/predict")
async def predict_batch(request: PredictionRequest):
    start_time = time.time()
    
    predictions = []
    for user_id, product_id in zip(request.user_ids, request.product_ids):
        # Проверка кэша
        cache_key = f"prediction:{user_id}:{product_id}"
        cached = redis_client.get(cache_key)
        
        if cached is not None:
            predictions.append(float(cached))
        else:
            # Предсказание
            prediction = model.predict(user_id, product_id)
            predictions.append(prediction)
            
            # Сохранение в кэш на 5 минут
            redis_client.setex(cache_key, 300, str(prediction))
    
    latency_ms = (time.time() - start_time) * 1000
    
    return {
        "predictions": predictions,
        "metadata": {
            "batch_size": len(predictions),
            "latency_ms": latency_ms
        }
    }

@app.post("/predict/single")
async def predict_single(request: SinglePredictionRequest):
    start_time = time.time()
    
    cache_key = f"prediction:{request.user_id}:{request.product_id}"
    cached = redis_client.get(cache_key)
    
    if cached is not None:
        prediction = float(cached)
        cached_flag = True
    else:
        prediction = model.predict(request.user_id, request.product_id)
        cached_flag = False
        redis_client.setex(cache_key, 300, str(prediction))
    
    latency_ms = (time.time() - start_time) * 1000
    
    return {
        "prediction": prediction,
        "cached": cached_flag,
        "latency_ms": latency_ms
    }