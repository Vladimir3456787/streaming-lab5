#!/bin/bash

echo "Запуск системы рекомендаций на Python..."

# 1. Запуск инфраструктуры
echo "1. Запуск инфраструктуры (Kafka, Flink, Redis)..."
docker-compose -f docker-compose-infra.yml up -d

# 2. Ожидание инициализации
echo "2. Ожидание инициализации сервисов..."
sleep 30

# 3. Создание Kafka topics
echo "3. Создание Kafka topics..."
docker exec -it pyflink-recommendation-job python3 scripts/kafka_topics.py create

# 4. Загрузка данных в Redis
echo "4. Загрузка популярных товаров в Redis..."
docker exec -it pyflink-recommendation-job python3 scripts/load_redis.py

# 5. Запуск Flink Job
echo "5. Запуск основного PyFlink Job..."
docker-compose up -d flink-job

# 6. Запуск генератора данных
echo "6. Запуск генератора тестовых данных..."
docker-compose up -d data-generator

# 7. Запуск мониторинга
echo "7. Запуск системы мониторинга..."
docker-compose up -d latency-monitor

# 8. Проверка статуса
echo "8. Проверка статуса системы..."
sleep 10
docker-compose ps

echo "Система запущена!"
echo "Flink UI: http://localhost:8081"
echo "Grafana: http://localhost:3000 (admin/admin)"
echo "API: http://localhost:8000/docs"