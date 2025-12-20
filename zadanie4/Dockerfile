FROM python:3.11-slim
WORKDIR /app

# Копируем рабочие файлы
COPY streaming_processor.py /app/streaming_processor.py
COPY generate_sample.py /app/generate_sample.py
COPY .env.example /app/.env.example
COPY requirements.txt /app/requirements.txt

# Устанавливаем зависимости (если requirements.txt пустой — этот шаг ничего не установит)
RUN pip install --no-cache-dir -r requirements.txt

VOLUME ["/data"]
CMD ["python", "streaming_processor.py"]
