import json
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

from pyflink.datastream.state import (
    ValueStateDescriptor,
    ValueState,
    ListStateDescriptor,
    ListState,
    MapStateDescriptor,
    MapState,
    ReducingStateDescriptor,
    ReducingState,
    AggregatingStateDescriptor,
    AggregatingState
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.common import Time

logger = logging.getLogger(__name__)

class UserSessionState:
    """Управление состоянием сессии пользователя"""
    
    def __init__(self, runtime_context):
        # Состояние текущей сессии
        self.session_state_desc = ValueStateDescriptor(
            "user_session_state",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.session_state = runtime_context.get_state(self.session_state_desc)
        
        # История сессий (последние 10)
        self.session_history_desc = ListStateDescriptor(
            "user_session_history",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.session_history = runtime_context.get_list_state(self.session_history_desc)
        
        # Счетчик событий в сессии
        self.event_count_desc = ValueStateDescriptor(
            "session_event_count",
            Types.INT()
        )
        self.event_count_state = runtime_context.get_state(self.event_count_desc)
        
        # Время последнего события
        self.last_event_time_desc = ValueStateDescriptor(
            "last_event_time",
            Types.LONG()
        )
        self.last_event_time_state = runtime_context.get_state(self.last_event_time_desc)
    
    def update_session(self, user_id: str, event: Dict[str, Any], timestamp: int):
        """Обновление состояния сессии"""
        # Получаем текущее состояние
        current_session = self.session_state.value() or {
            "user_id": user_id,
            "start_time": timestamp,
            "last_update": timestamp,
            "events": [],
            "product_views": set(),
            "categories_viewed": set()
        }
        
        # Обновляем сессию
        current_session["last_update"] = timestamp
        current_session["events"].append({
            "event": event["event_type"],
            "product_id": event["product_id"],
            "timestamp": timestamp
        })
        
        # Ограничиваем размер истории событий
        if len(current_session["events"]) > 100:
            current_session["events"] = current_session["events"][-100:]
        
        # Обновляем просмотренные товары и категории
        current_session["product_views"].add(event["product_id"])
        if "category_id" in event:
            current_session["categories_viewed"].add(event["category_id"])
        
        # Сохраняем состояние
        self.session_state.update(current_session)
        
        # Обновляем счетчик
        event_count = self.event_count_state.value() or 0
        self.event_count_state.update(event_count + 1)
        
        # Обновляем время последнего события
        self.last_event_time_state.update(timestamp)
        
        # Если сессия длится больше 30 минут, сохраняем в историю и начинаем новую
        if timestamp - current_session["start_time"] > 30 * 60 * 1000:
            self._finalize_session(current_session)
    
    def _finalize_session(self, session: Dict[str, Any]):
        """Финализация сессии и сохранение в историю"""
        # Рассчитываем метрики сессии
        session["duration_ms"] = session["last_update"] - session["start_time"]
        session["event_count"] = len(session["events"])
        session["unique_products"] = len(session["product_views"])
        session["unique_categories"] = len(session["categories_viewed"])
        
        # Добавляем в историю
        self.session_history.add(session)
        
        # Ограничиваем историю (последние 10 сессий)
        history = list(self.session_history.get())
        if len(history) > 10:
            # Удаляем самую старую сессию
            sorted_history = sorted(history, key=lambda x: x["start_time"])
            self.session_history.clear()
            for sess in sorted_history[-10:]:
                self.session_history.add(sess)
        
        # Сбрасываем текущую сессию
        self.session_state.clear()
        self.event_count_state.clear()
    
    def get_session_features(self) -> Dict[str, Any]:
        """Получение фич из состояния сессии"""
        session = self.session_state.value()
        if not session:
            return {}
        
        events = session.get("events", [])
        recent_events = [e for e in events 
                        if datetime.now().timestamp() * 1000 - e["timestamp"] < 5 * 60 * 1000]
        
        return {
            "session_duration_ms": session.get("last_update", 0) - session.get("start_time", 0),
            "total_events": len(events),
            "recent_events": len(recent_events),
            "unique_products": len(session.get("product_views", set())),
            "unique_categories": len(session.get("categories_viewed", set())),
            "event_frequency": len(events) / max((session["last_update"] - session["start_time"]) / 1000, 1)
        }

class WindowedAggregationState:
    """Состояние для оконных агрегаций"""
    
    def __init__(self, runtime_context, window_size_ms: int = 60000):
        self.window_size_ms = window_size_ms
        
        # Состояние для скользящего окна
        self.window_state_desc = ListStateDescriptor(
            "window_events",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.window_state = runtime_context.get_list_state(self.window_state_desc)
    
    def add_event(self, event: Dict[str, Any], timestamp: int):
        """Добавление события в окно"""
        window_event = {
            "data": event,
            "timestamp": timestamp
        }
        self.window_state.add(window_event)
        
        # Очищаем старые события
        self._cleanup_old_events(timestamp)
    
    def _cleanup_old_events(self, current_time: int):
        """Очистка событий старше окна"""
        events = list(self.window_state.get())
        recent_events = [
            e for e in events 
            if current_time - e["timestamp"] <= self.window_size_ms
        ]
        
        if len(recent_events) < len(events):
            self.window_state.clear()
            for event in recent_events:
                self.window_state.add(event)
    
    def get_window_aggregates(self) -> Dict[str, Any]:
        """Получение агрегатов по окну"""
        events = list(self.window_state.get())
        
        if not events:
            return {}
        
        # Пример агрегатов
        event_types = [e["data"].get("event_type") for e in events]
        from collections import Counter
        type_counts = Counter(event_types)
        
        return {
            "total_events": len(events),
            "event_type_counts": dict(type_counts),
            "events_per_second": len(events) / (self.window_size_ms / 1000)
        }

class StateTTLManager:
    """Управление TTL для state"""
    
    def __init__(self, ttl_config: Dict[str, int]):
        """
        ttl_config: {
            "session_state_ttl_ms": 3600000,  # 1 час
            "window_state_ttl_ms": 300000,    # 5 минут
            "cache_ttl_ms": 300000            # 5 минут
        }
        """
        self.ttl_config = ttl_config
        
    def configure_state_ttl(self, state_descriptor, state_name: str):
        """Настройка TTL для state descriptor"""
        if state_name in self.ttl_config:
            ttl_ms = self.ttl_config[state_name]
            # В PyFlink TTL настраивается через configuration
            # Здесь должна быть реализация в зависимости от версии Flink
            pass