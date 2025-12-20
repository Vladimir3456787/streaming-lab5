FROM flink:1.18.1-scala_2.12-java11

# Устанавливаем Python и pip
RUN apt-get update && apt-get install -y python3 python3-pip && apt-get clean

WORKDIR /opt/flink-job

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем только requests (PyFlink уже есть в образе Flink)
RUN pip3 install --no-cache-dir -r requirements.txt

# Копируем исходный код
COPY src/ ./src/

# Создаем симлинк для Python в Flink
RUN ln -sf /usr/bin/python3 /opt/flink/bin/python

CMD ["python3", "src/main.py"]