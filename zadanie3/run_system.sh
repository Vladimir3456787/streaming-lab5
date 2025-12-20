#!/bin/bash

echo "Запуск системы ML-интеграции..."

# 1. Остановка предыдущих контейнеров
docker-compose -f docker-compose-infra.yml down

# 2. Запуск инфраструктуры
echo "Запуск инфраструктуры..."
docker-compose -f docker-compose-infra.yml up -d

# 3. Ожидание запуска сервисов
echo "Ожидание запуска сервисов..."
sleep 30

# 4. Создание Kafka topics
echo "Создание Kafka topics..."
docker exec kafka kafka-topics --create --topic user-behavior-events --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
docker exec kafka kafka-topics --create --topic recommendations --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

# 5. Заполнение Redis тестовыми данными
echo "Заполнение Redis тестовыми данными..."
python feature-generation/populate_features.py

# 6. Проверка сервисов
echo "Проверка сервисов..."
curl -f http://localhost:8000/health || echo "ML service not ready"
curl -f http://localhost:8081/overview || echo "Flink not ready"

echo "Система запущена!"
echo "Доступные сервисы:"
echo "  - Flink UI: http://localhost:8081"
echo "  - ML Service: http://localhost:8000/docs"
echo "  - Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "Для запуска Flink job выполните:"
echo "  docker exec jobmanager python3 /opt/flink-job/src/main.py"