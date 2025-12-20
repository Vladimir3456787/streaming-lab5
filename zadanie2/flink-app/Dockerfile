FROM flink:1.18.1-scala_2.12-java11

# Устанавливаем Python и зависимости
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && apt-get clean

# Устанавливаем PyFlink
RUN pip3 install apache-flink==1.18.1

# Копируем весь проект
COPY . /opt/flink-app/

# Создаем симлинк для Python
RUN ln -sf /usr/bin/python3 /opt/flink/bin/python

WORKDIR /opt/flink-app
CMD ["python3", "src/main.py"]