import sys
import os

# Добавляем путь к модулям
sys.path.append('/opt/flink-app/src')

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Duration

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Простой тестовый пайплайн
    data = ["Hello", "Flink", "Python", "Docker"]
    stream = env.from_collection(data)
    stream.print()
    
    env.execute("Test Job")
    print("Job executed successfully!")

if __name__ == "__main__":
    main()