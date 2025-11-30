#!/bin/bash

# Ждем пока Kafka будет готова
echo "Waiting for Kafka to be ready..."
sleep 30

# Создаем топики
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic user-events --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 --topic fraud-alerts --partitions 1 --replication-factor 1

# Выводим информацию о топиках
echo "Topics created:"
kafka-topics --list --bootstrap-server kafka:9092