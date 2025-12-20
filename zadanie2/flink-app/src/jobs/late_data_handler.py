"""
Обработка late-arriving данных и out-of-order событий
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, List
from dataclasses import dataclass

from pyflink.datastream import (
    DataStream, 
    ProcessFunction, 
    OutputTag,
    SideOutputProcessFunction
)
from pyflink.datastream.state import (
    ValueStateDescriptor, 
    ValueState,
    MapStateDescriptor,
    MapState
)
from pyflink.common import Time
from pyflink.common.typeinfo import Types

from models.user_behavior import UserBehavior

logger = logging.getLogger(__name__)

@dataclass
class LateEventInfo:
    """Информация о позднем событии"""
    event: UserBehavior
    arrival_time: datetime
    processing_time: datetime
    delay_seconds: float
    window_start: Optional[datetime]
    window_end: Optional[datetime]

class LateDataHandler:
    """Обработчик late-arriving данных"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.allowed_lateness = config['processing']['allowed_lateness_seconds']
        self.max_delay = config['processing']['max_delay_seconds']
        
        # Output tags для разных типов late данных
        self.moderate_late_tag = OutputTag("moderate_late_data", Types.PICKLED_BYTE_ARRAY())
        self.very_late_tag = OutputTag("very_late_data", Types.PICKLED_BYTE_ARRAY())
        self.discarded_tag = OutputTag("discarded_late_data", Types.PICKLED_BYTE_ARRAY())
    
    def execute(self, stream: DataStream) -> DataStream:
        """Основной пайплайн обработки late данных"""
        
        # 1. Обработка late данных в основном потоке
        processed_stream = stream \
            .process(self.LateDataProcessor(
                self.allowed_lateness,
                self.max_delay,
                self.moderate_late_tag,
                self.very_late_tag,
                self.discarded_tag
            )) \
            .name("Late Data Processor")
        
        # 2. Извлечение moderate late данных (задержка < allowed_lateness)
        moderate_late_stream = processed_stream \
            .get_side_output(self.moderate_late_tag) \
            .map(self._process_moderate_late) \
            .name("Process Moderate Late Data")
        
        # 3. Извлечение very late данных (задержка > allowed_lateness)
        very_late_stream = processed_stream \
            .get_side_output(self.very_late_tag) \
            .map(self._process_very_late) \
            .name("Process Very Late Data")
        
        # 4. Извлечение discarded данных
        discarded_stream = processed_stream \
            .get_side_output(self.discarded_tag) \
            .map(self._log_discarded) \
            .name("Log Discarded Data")
        
        # 5. Объединение обработанных late данных с основным потоком
        main_stream = processed_stream \
            .union(moderate_late_stream) \
            .name("Merge with Late Data")
        
        return main_stream
    
    def _process_moderate_late(self, late_info: LateEventInfo) -> UserBehavior:
        """Обработка умеренно поздних данных"""
        event = late_info.event
        
        # Можно обновить метаданные события
        event.metadata['late_processed'] = True
        event.metadata['delay_seconds'] = late_info.delay_seconds
        event.metadata['original_timestamp'] = event.timestamp
        event.metadata['processed_timestamp'] = int(datetime.now().timestamp() * 1000)
        
        logger.info(
            f"Processed moderate late event: user={event.user_id}, "
            f"delay={late_info.delay_seconds:.2f}s"
        )
        
        return event
    
    def _process_very_late(self, late_info: LateEventInfo) -> UserBehavior:
        """Обработка очень поздних данных"""
        event = late_info.event
        
        # Альтернативная обработка для очень поздних данных
        event.metadata['very_late'] = True
        event.metadata['discarded_from_windows'] = True
        event.metadata['delay_seconds'] = late_info.delay_seconds
        
        # Можно использовать для обновления моделей или аналитики
        logger.warning(
            f"Processing very late event: user={event.user_id}, "
            f"delay={late_info.delay_seconds:.2f}s"
        )
        
        return event
    
    def _log_discarded(self, late_info: LateEventInfo):
        """Логирование отброшенных данных"""
        logger.error(
            f"Discarded too late event: user={late_info.event.user_id}, "
            f"delay={late_info.delay_seconds:.2f}s, "
            f"max_allowed={self.max_delay}s"
        )
        return None
    
    class LateDataProcessor(ProcessFunction):
        """Процессор для классификации late данных"""
        
        def __init__(self, allowed_lateness, max_delay, 
                    moderate_tag, very_tag, discarded_tag):
            self.allowed_lateness = allowed_lateness
            self.max_delay = max_delay
            self.moderate_tag = moderate_tag
            self.very_tag = very_tag
            self.discarded_tag = discarded_tag
            
            # State для отслеживания последних окон
            self.window_state_desc = MapStateDescriptor(
                "window_timestamps",
                Types.STRING(),  # user_id
                Types.LONG()     # last_window_end
            )
            self.window_state = None
        
        def open(self, runtime_context):
            self.window_state = runtime_context.get_map_state(
                self.window_state_desc
            )
        
        def process_element(self, event: UserBehavior, ctx, out):
            current_watermark = ctx.timer_service().current_watermark()
            event_timestamp = event.timestamp
            
            # Рассчитываем задержку
            delay_ms = current_watermark - event_timestamp
            delay_seconds = delay_ms / 1000.0
            
            # Создаем информацию о late событии
            late_info = LateEventInfo(
                event=event,
                arrival_time=datetime.now(),
                processing_time=datetime.fromtimestamp(current_watermark / 1000),
                delay_seconds=delay_seconds,
                window_start=None,
                window_end=None
            )
            
            # Классификация late данных
            if delay_seconds < 0:
                # Обычное событие (не late)
                out.collect(event)
                
            elif delay_seconds <= self.allowed_lateness:
                # Умеренно поздние данные
                late_info.window_start, late_info.window_end = \
                    self._calculate_window(event_timestamp)
                ctx.output(self.moderate_tag, late_info)
                
            elif delay_seconds <= self.max_delay:
                # Очень поздние данные
                late_info.window_start, late_info.window_end = \
                    self._calculate_window(event_timestamp)
                ctx.output(self.very_tag, late_info)
                
            else:
                # Слишком поздние данные - отбрасываем
                ctx.output(self.discarded_tag, late_info)
            
            # Обновление state с последним временем окна
            if event_timestamp > self.window_state.get(event.user_id) or \
               not self.window_state.contains(event.user_id):
                self.window_state.put(event.user_id, event_timestamp)
        
        def _calculate_window(self, timestamp: int) -> tuple:
            """Вычисление временного окна для события"""
            window_size = 60000  # 1 минута в миллисекундах
            
            window_start = (timestamp // window_size) * window_size
            window_end = window_start + window_size
            
            return (
                datetime.fromtimestamp(window_start / 1000),
                datetime.fromtimestamp(window_end / 1000)
            )