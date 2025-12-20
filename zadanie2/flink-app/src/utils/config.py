import yaml
import os

def load_config(config_path: str = None) -> dict:
    """
    Загрузка конфигурации из YAML файла
    """
    if config_path is None:
        config_path = os.getenv('CONFIG_PATH', 'config.yaml')
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Добавляем дефолтные значения
        config.setdefault('environment', 'development')
        config.setdefault('flink', {})
        config['flink'].setdefault('parallelism', 4)
        
        return config
        
    except FileNotFoundError:
        # Возвращаем конфигурацию по умолчанию
        return {
            'environment': 'development',
            'flink': {
                'parallelism': 4,
                'checkpoint_interval': 1000,
                'checkpoint_mode': 'EXACTLY_ONCE'
            },
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'consumer_group': 'flink-recommendation-group'
            }
        }