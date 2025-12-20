"""
Рекомендательная система в реальном времени
Генерация персональных рекомендаций на основе поведения пользователя
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass

from pyflink.datastream import (
    DataStream, 
    KeyedStream, 
    ProcessFunction, 
    AggregateFunction,
    ProcessWindowFunction,
    WindowFunction
)
from pyflink.datastream.window import (
    TumblingEventTimeWindows, 
    Time, 
    TimeWindow
)
from pyflink.datastream.state import (
    ValueStateDescriptor, 
    ValueState, 
    ListStateDescriptor,
    ListState,
    MapStateDescriptor,
    MapState
)
from pyflink.common import Row
from pyflink.common.typeinfo import Types

from models.user_behavior import UserBehavior, EventType
from models.recommendation import Recommendation, RecommendationEngine
from connectors.redis_client import RedisClient

logger = logging.getLogger(__name__)

@dataclass
class UserProfile:
    """Профиль пользователя для рекомендаций"""
    user_id: str
    viewed_products: List[str]
    purchased_products: List[str]
    categories_preferred: List[str]
    last_activity: datetime
    session_count: int
    avg_session_duration: float

class UserProfileAggregator(AggregateFunction):
    """Агрегатор для построения профиля пользователя"""
    
    def create_accumulator(self) -> Dict:
        return {
            'user_id': None,
            'viewed_products': set(),
            'purchased_products': set(),
            'categories': set(),
            'session_start': None,
            'session_end': None,
            'event_count': 0
        }
    
    def add(self, value: UserBehavior, accumulator: Dict) -> Dict:
        accumulator['user_id'] = value.user_id
        
        if value.event_type == EventType.VIEW:
            accumulator['viewed_products'].add(value.product_id)
            if value.category_code:
                accumulator['categories'].add(value.category_code)
        
        elif value.event_type == EventType.PURCHASE:
            accumulator['purchased_products'].add(value.product_id)
            if value.category_code:
                accumulator['categories'].add(value.category_code)
        
        # Обновление времени сессии
        if not accumulator['session_start'] or \
           value.timestamp < accumulator['session_start']:
            accumulator['session_start'] = value.timestamp
        
        if not accumulator['session_end'] or \
           value.timestamp > accumulator['session_end']:
            accumulator['session_end'] = value.timestamp
        
        accumulator['event_count'] += 1
        
        return accumulator
    
    def get_result(self, accumulator: Dict) -> UserProfile:
        duration = 0
        if accumulator['session_start'] and accumulator['session_end']:
            duration = accumulator['session_end'] - accumulator['session_start']
        
        return UserProfile(
            user_id=accumulator['user_id'],
            viewed_products=list(accumulator['viewed_products']),
            purchased_products=list(accumulator['purchased_products']),
            categories_preferred=list(accumulator['categories']),
            last_activity=datetime.fromtimestamp(
                accumulator['session_end'] / 1000
            ) if accumulator['session_end'] else datetime.now(),
            session_count=1,
            avg_session_duration=duration
        )
    
    def merge(self, a: Dict, b: Dict) -> Dict:
        a['viewed_products'].update(b['viewed_products'])
        a['purchased_products'].update(b['purchased_products'])
        a['categories'].update(b['categories'])
        a['event_count'] += b['event_count']
        
        if not a['session_start'] or \
           (b['session_start'] and b['session_start'] < a['session_start']):
            a['session_start'] = b['session_start']
        
        if not a['session_end'] or \
           (b['session_end'] and b['session_end'] > a['session_end']):
            a['session_end'] = b['session_end']
        
        return a

class ColdStartHandler:
    """Обработчик холодного старта для новых пользователей"""
    
    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client
        self.popular_products_key = "popular_products"
        self.trending_products_key = "trending_products"
    
    def get_recommendations(self, user_id: str, limit: int = 10) -> List[str]:
        """Получение рекомендаций для холодного старта"""
        
        # Попробуем получить из кеша
        cached = self.redis.get(f"cold_start:{user_id}")
        if cached:
            return json.loads(cached)
        
        # 1. Самые популярные товары
        popular = self.redis.zrevrange(
            self.popular_products_key, 
            0, limit-1, 
            withscores=True
        )
        
        # 2. Трендовые товары (последние 24 часа)
        trending = self.redis.zrevrange(
            self.trending_products_key,
            0, limit-1,
            withscores=True
        )
        
        # 3. Товары из той же категории, что и популярные
        recommendations = []
        seen = set()
        
        # Смешиваем популярные и трендовые
        for source in [popular, trending]:
            for product_id, score in source:
                if product_id not in seen:
                    recommendations.append(product_id)
                    seen.add(product_id)
                    if len(recommendations) >= limit:
                        break
        
        # Сохраняем в кеш на 1 час
        self.redis.setex(
            f"cold_start:{user_id}",
            3600,
            json.dumps(recommendations)
        )
        
        return recommendations

class RecommendationJob:
    """Основной Job для рекомендательной системы"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.redis_client = RedisClient(config['redis'])
        self.recommendation_engine = RecommendationEngine(config)
        self.cold_start_handler = ColdStartHandler(self.redis_client)
        
        # Состояния
        self.user_profile_state_desc = MapStateDescriptor(
            "user_profile_state",
            Types.STRING(),  # user_id
            Types.PICKLED_BYTE_ARRAY()  # UserProfile
        )
        
        self.user_recommendations_desc = MapStateDescriptor(
            "user_recommendations",
            Types.STRING(),  # user_id
            Types.PICKLED_BYTE_ARRAY()  # List[Recommendation]
        )
    
    def execute(self, stream: DataStream) -> DataStream:
        """Выполнение рекомендательного пайплайна"""
        
        # 1. Преобразование в UserBehavior
        user_behavior_stream = stream \
            .map(self._parse_user_behavior) \
            .name("Parse User Behavior") \
            .filter(lambda x: x is not None) \
            .name("Filter Valid Events")
        
        # 2. Окно для агрегации поведения (окно 10 минут)
        windowed_stream = user_behavior_stream \
            .key_by(lambda x: x.user_id) \
            .window(TumblingEventTimeWindows.of(Time.minutes(10))) \
            .aggregate(
                UserProfileAggregator(),
                self._process_user_window
            ) \
            .name("User Behavior Aggregation")
        
        # 3. Генерация рекомендаций на основе профиля
        recommendations_stream = windowed_stream \
            .key_by(lambda x: x.user_id) \
            .process(
                self.RecommendationGenerator(
                    self.recommendation_engine,
                    self.cold_start_handler
                )
            ) \
            .name("Generate Recommendations")
        
        # 4. Фильтрация и ранжирование рекомендаций
        filtered_stream = recommendations_stream \
            .flat_map(self._filter_and_rank) \
            .name("Filter and Rank Recommendations")
        
        # 5. Обогащение рекомендаций дополнительной информацией
        enriched_stream = filtered_stream \
            .map(self._enrich_recommendation) \
            .name("Enrich Recommendations")
        
        # 6. Форматирование для вывода
        output_stream = enriched_stream \
            .map(lambda rec: json.dumps(rec.to_dict())) \
            .name("Format Output")
        
        return output_stream
    
    def _parse_user_behavior(self, row: Row) -> Optional[UserBehavior]:
        """Парсинг Row в UserBehavior"""
        try:
            return UserBehavior.from_row(row)
        except Exception as e:
            logger.warning(f"Failed to parse row: {e}")
            return None
    
    def _process_user_window(self, key, window, profiles, out):
        """Обработка оконного агрегата"""
        for profile in profiles:
            # Можно добавить дополнительную логику агрегации
            out.collect(profile)
    
    def _filter_and_rank(self, recommendations: List[Recommendation]):
        """Фильтрация и ранжирование рекомендаций"""
        # Фильтрация уже просмотренных товаров
        seen_products = set()
        filtered = []
        
        for rec in recommendations:
            if rec.product_id not in seen_products:
                # Применяем ранжирование на основе скоринга
                if rec.score >= self.config['recommendation']['min_score']:
                    filtered.append(rec)
                    seen_products.add(rec.product_id)
        
        # Сортировка по score
        filtered.sort(key=lambda x: x.score, reverse=True)
        
        # Ограничение по количеству
        max_recommendations = self.config['recommendation']['max_count']
        for rec in filtered[:max_recommendations]:
            yield rec
    
    def _enrich_recommendation(self, recommendation: Recommendation) -> Recommendation:
        """Обогащение рекомендации дополнительной информацией"""
        try:
            # Получение информации о товаре из Redis
            product_info = self.redis_client.hgetall(
                f"product:{recommendation.product_id}"
            )
            
            if product_info:
                recommendation.product_name = product_info.get('name', '')
                recommendation.category = product_info.get('category', '')
                recommendation.price = float(product_info.get('price', 0))
                recommendation.image_url = product_info.get('image_url', '')
            
            # Добавление времени генерации
            recommendation.generated_at = datetime.now()
            
            return recommendation
            
        except Exception as e:
            logger.error(f"Failed to enrich recommendation: {e}")
            return recommendation
    
    class RecommendationGenerator(ProcessFunction):
        """Генератор рекомендаций"""
        
        def __init__(self, engine, cold_start_handler):
            self.engine = engine
            self.cold_start_handler = cold_start_handler
            self.user_profile_state = None
            self.recommendation_cache = None
        
        def open(self, runtime_context):
            self.user_profile_state = runtime_context.get_map_state(
                MapStateDescriptor(
                    "user_profiles",
                    Types.STRING(),
                    Types.PICKLED_BYTE_ARRAY()
                )
            )
            
            self.recommendation_cache = runtime_context.get_map_state(
                MapStateDescriptor(
                    "recommendation_cache",
                    Types.STRING(),
                    Types.PICKLED_BYTE_ARRAY()
                )
            )
        
        def process_element(self, profile: UserProfile, ctx, out):
            user_id = profile.user_id
            
            # Проверка кеша рекомендаций
            cached_recommendations = self.recommendation_cache.get(user_id)
            cache_ttl = 300  # 5 минут
            
            if cached_recommendations:
                # Проверяем актуальность кеша
                if datetime.now().timestamp() - \
                   cached_recommendations[-1].generated_at.timestamp() < cache_ttl:
                    for rec in cached_recommendations:
                        out.collect(rec)
                    return
            
            # Генерация новых рекомендаций
            try:
                # Получаем предыдущий профиль
                previous_profile = self.user_profile_state.get(user_id)
                
                if previous_profile:
                    # Обновляем профиль
                    updated_profile = self._merge_profiles(
                        previous_profile, 
                        profile
                    )
                    self.user_profile_state.put(user_id, updated_profile)
                    
                    # Генерируем рекомендации на основе обновленного профиля
                    recommendations = self.engine.generate(
                        updated_profile,
                        limit=20
                    )
                else:
                    # Новый пользователь - холодный старт
                    self.user_profile_state.put(user_id, profile)
                    
                    # Проверяем, достаточно ли данных для персонализации
                    if len(profile.viewed_products) >= 3:
                        recommendations = self.engine.generate(
                            profile,
                            limit=20
                        )
                    else:
                        # Используем холодный старт
                        product_ids = self.cold_start_handler.get_recommendations(
                            user_id,
                            limit=20
                        )
                        recommendations = [
                            Recommendation(
                                user_id=user_id,
                                product_id=pid,
                                score=0.7,  # Базовая оценка для холодного старта
                                reason="cold_start",
                                generated_at=datetime.now()
                            )
                            for pid in product_ids
                        ]
                
                # Кешируем рекомендации
                self.recommendation_cache.put(user_id, recommendations)
                
                # Отправляем рекомендации
                for rec in recommendations:
                    out.collect(rec)
                    
            except Exception as e:
                logger.error(f"Error generating recommendations: {e}")
                # Fallback: холодный старт
                product_ids = self.cold_start_handler.get_recommendations(
                    user_id,
                    limit=10
                )
                for pid in product_ids:
                    out.collect(Recommendation(
                        user_id=user_id,
                        product_id=pid,
                        score=0.7,
                        reason="fallback",
                        generated_at=datetime.now()
                    ))
        
        def _merge_profiles(self, old: UserProfile, new: UserProfile) -> UserProfile:
            """Объединение профилей пользователя"""
            # Объединяем просмотренные товары
            viewed = list(set(old.viewed_products) | set(new.viewed_products))
            
            # Объединяем купленные товары
            purchased = list(set(old.purchased_products) | set(new.purchased_products))
            
            # Объединяем категории
            categories = list(set(old.categories_preferred) | set(new.categories_preferred))
            
            # Обновляем время последней активности
            last_activity = max(old.last_activity, new.last_activity)
            
            # Обновляем статистику сессий
            session_count = old.session_count + 1
            avg_duration = (
                (old.avg_session_duration * old.session_count) + 
                new.avg_session_duration
            ) / session_count
            
            return UserProfile(
                user_id=old.user_id,
                viewed_products=viewed,
                purchased_products=purchased,
                categories_preferred=categories,
                last_activity=last_activity,
                session_count=session_count,
                avg_session_duration=avg_duration
            )