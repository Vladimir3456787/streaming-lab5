#!/bin/bash

# Ждем пока Redis будет готов
echo "Waiting for Redis to be ready..."
sleep 10

# Тестируем подключение
redis-cli -h redis ping

echo "Redis is ready for fraud detection system"