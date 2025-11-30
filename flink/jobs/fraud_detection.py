from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import json
import time

def main():
    """Упрощенная версия Flink job для тестирования"""
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Настройка Kafka consumer
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'fraud-detection-group'
    }
    
    kafka_consumer = FlinkKafkaConsumer(
        'user-events',
        SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Создаем поток данных
    input_stream = env.add_source(kafka_consumer)
    
    # Простая обработка - выводим полученные события
    processed_stream = input_stream.map(lambda x: f"Processed event: {x}")
    
    # Выводим результат
    processed_stream.print()
    
    print("Starting Simple Fraud Detection Job...")
    env.execute("Simple Fraud Detection Job")

if __name__ == '__main__':
    main()