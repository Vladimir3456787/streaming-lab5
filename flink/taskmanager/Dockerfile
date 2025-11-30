FROM flink:1.15-scala_2.12

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Сначала устанавливаем numpy отдельно с совместимой версией
RUN pip3 install numpy==1.21.6

# Затем устанавливаем apache-flink
RUN pip3 install apache-flink==1.15.0

CMD ["taskmanager.sh", "start-foreground"]