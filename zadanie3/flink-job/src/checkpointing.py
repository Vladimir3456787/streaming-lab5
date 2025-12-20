from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import CheckpointConfig, ExternalizedCheckpointCleanup
from pyflink.datastream.state_backend import RocksDBStateBackend
import logging

logger = logging.getLogger(__name__)

def configure_exactly_once(env: StreamExecutionEnvironment, checkpoint_dir: str = None):
    """
    Настройка checkpointing для exactly-once семантики
    
    Exactly-once гарантии обеспечиваются через:
    1. Checkpointing состояния каждую секунду
    2. Distributed snapshots через Chandy-Lamport алгоритм
    3. Transactional sinks (Kafka)
    4. Idempotent operations
    """
    
    # Включаем checkpointing каждую секунду
    env.enable_checkpointing(1000)  # 1000 ms = 1 second
    
    # Настройка exactly-once режима
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    
    # Минимальная пауза между checkpoint (500ms)
    checkpoint_config.set_min_pause_between_checkpoints(500)
    
    # Таймаут checkpoint (60 секунд)
    checkpoint_config.set_checkpoint_timeout(60000)
    
    # Максимальное количество concurrent checkpoint (1)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    
    # Внешние checkpoint для восстановления
    checkpoint_config.enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    
    # Tolerable checkpoint failures
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)
    
    # Настройка state backend (RocksDB для больших состояний)
    if checkpoint_dir:
        state_backend = RocksDBStateBackend(
            checkpoint_dir,
            True  # incremental checkpoints
        )
        env.set_state_backend(state_backend)
    
    # Настройка восстановления при перезапуске
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            3,  # Количество попыток
            10000  # Задержка между попытками (ms)
        )
    )
    
    logger.info("Exactly-once checkpointing configured")
    logger.info(f"Checkpoint interval: 1000ms")
    logger.info(f"Checkpoint mode: EXACTLY_ONCE")
    logger.info(f"State backend: RocksDB")
    
    return env

class ExactlyOnceGuarantees:
    """
    Класс для обеспечения exactly-once семантики в пользовательских функциях
    """
    
    def __init__(self):
        self.exactly_once_mechanisms = []
    
    def add_idempotent_operation(self, operation_name: str, idempotent_key: str):
        """
        Добавление идемпотентной операции
        
        Идемпотентность обеспечивает, что повторное выполнение операции
        не изменит результат (ключевое для exactly-once)
        """
        self.exactly_once_mechanisms.append({
            "type": "idempotent",
            "operation": operation_name,
            "key": idempotent_key
        })
    
    def add_transactional_sink(self, sink_name: str, transaction_id: str):
        """
        Добавление transactional sink
        
        Transactional sinks гарантируют атомарность записи
        """
        self.exactly_once_mechanisms.append({
            "type": "transactional",
            "sink": sink_name,
            "transaction_id": transaction_id
        })
    
    def add_state_snapshot(self, state_name: str):
        """
        Добавление state snapshotting
        
        State snapshotting позволяет восстанавливать состояние
        после сбоя точно с того же места
        """
        self.exactly_once_mechanisms.append({
            "type": "state_snapshot",
            "state": state_name
        })
    
    def explain_exactly_once(self) -> str:
        """
        Объяснение exactly-once гарантий системы
        """
        explanation = """
        Exactly-once semantics гарантирует, что каждое событие будет обработано
        ровно один раз, даже при сбоях в системе.
        
        Механизмы реализации:
        1. Checkpointing: Состояние периодически сохраняется в распределенное хранилище
        2. State Snapshotting: Моментальные снимки всего состояния системы
        3. Idempotent Operations: Повторное выполнение не меняет результат
        4. Transactional Output: Атомарная запись в выходные системы
        
        Как это работает в нашем пайплайне:
        """
        
        for mechanism in self.exactly_once_mechanisms:
            if mechanism["type"] == "idempotent":
                explanation += f"\n- Идемпотентность: {mechanism['operation']} (ключ: {mechanism['key']})"
            elif mechanism["type"] == "transactional":
                explanation += f"\n- Transactional sink: {mechanism['sink']} (ID: {mechanism['transaction_id']})"
            elif mechanism["type"] == "state_snapshot":
                explanation += f"\n- State snapshot: {mechanism['state']}"
        
        explanation += """
        
        Влияние на ML-интеграцию:
        - ML запросы должны быть идемпотентными
        - Результаты кэшируются для избежания повторных запросов
        - Состояние предсказаний сохраняется в checkpoint
        - При восстановлении повторные ML запросы не выполняются
        
        Проверка exactly-once:
        1. Все операции идемпотентны
        2. Состояние сохраняется в checkpoint
        3. Выходные данные пишутся транзакционно
        4. При сбое восстановление происходит с последнего checkpoint
        """
        
        return explanation

# Пример использования в main.py Flink job
def setup_exactly_once_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Настройка checkpointing
    env = configure_exactly_once(
        env, 
        checkpoint_dir="file:///tmp/flink/checkpoints"
    )
    
    # Создание объяснения exactly-once
    guarantees = ExactlyOnceGuarantees()
    guarantees.add_idempotent_operation("ML Prediction", "user_id:product_id:timestamp")
    guarantees.add_transactional_sink("Kafka Sink", "flink-kafka-transaction")
    guarantees.add_state_snapshot("UserSessionState")
    guarantees.add_state_snapshot("PredictionCache")
    
    print(guarantees.explain_exactly_once())
    
    return env