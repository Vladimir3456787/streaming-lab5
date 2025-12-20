import asyncio
import aiohttp
import time
import random
import statistics
from typing import List, Dict
import numpy as np

class MLServiceTester:
    def __init__(self, base_url: str, num_users: int = 1000, num_products: int = 100):
        self.base_url = base_url
        self.num_users = num_users
        self.num_products = num_products
        self.latencies = []
        self.session = None
    
    async def test_single_predictions(self, num_requests: int = 1000):
        """Тестирование одиночных предсказаний"""
        print(f"Testing {num_requests} single predictions...")
        
        latencies = []
        errors = 0
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(num_requests):
                user_id = f"user_{random.randint(1, self.num_users)}"
                product_id = f"product_{random.randint(1, self.num_products)}"
                
                task = self._make_single_request(session, user_id, product_id)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    errors += 1
                else:
                    latencies.append(result)
        
        return self._calculate_metrics(latencies, errors, num_requests)
    
    async def test_batch_predictions(self, batch_size: int = 32, num_batches: int = 100):
        """Тестирование batch предсказаний"""
        print(f"Testing {num_batches} batches of size {batch_size}...")
        
        latencies = []
        errors = 0
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(num_batches):
                user_ids = [f"user_{random.randint(1, self.num_users)}" 
                           for _ in range(batch_size)]
                product_ids = [f"product_{random.randint(1, self.num_products)}" 
                              for _ in range(batch_size)]
                
                task = self._make_batch_request(session, user_ids, product_ids)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    errors += 1
                else:
                    latencies.append(result)
        
        return self._calculate_metrics(latencies, errors, num_batches)
    
    async def _make_single_request(self, session, user_id: str, product_id: str):
        """Отправка одиночного запроса"""
        start_time = time.time()
        
        try:
            async with session.post(
                f"{self.base_url}/predict/single",
                json={
                    "user_id": user_id,
                    "product_id": product_id,
                    "context": {"time_of_day": "afternoon"}
                },
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    latency = (time.time() - start_time) * 1000
                    return latency
                else:
                    raise Exception(f"HTTP {response.status}")
                    
        except Exception as e:
            raise Exception(f"Request failed: {e}")
    
    async def _make_batch_request(self, session, user_ids: List[str], product_ids: List[str]):
        """Отправка batch запроса"""
        start_time = time.time()
        
        try:
            async with session.post(
                f"{self.base_url}/predict",
                json={
                    "user_ids": user_ids,
                    "product_ids": product_ids,
                    "context": {"time_of_day": "afternoon"}
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    latency = (time.time() - start_time) * 1000
                    return latency
                else:
                    raise Exception(f"HTTP {response.status}")
                    
        except Exception as e:
            raise Exception(f"Batch request failed: {e}")
    
    def _calculate_metrics(self, latencies: List[float], errors: int, total: int) -> Dict:
        """Расчет метрик производительности"""
        if not latencies:
            return {}
        
        latencies_array = np.array(latencies)
        
        # Убираем выбросы (более 3 стандартных отклонений)
        mean = np.mean(latencies_array)
        std = np.std(latencies_array)
        filtered = latencies_array[latencies_array < mean + 3 * std]
        
        if len(filtered) == 0:
            filtered = latencies_array
        
        return {
            "p50_ms": float(np.percentile(filtered, 50)),
            "p95_ms": float(np.percentile(filtered, 95)),
            "p99_ms": float(np.percentile(filtered, 99)),
            "p999_ms": float(np.percentile(filtered, 99.9)),
            "mean_ms": float(np.mean(filtered)),
            "min_ms": float(np.min(filtered)),
            "max_ms": float(np.max(filtered)),
            "std_ms": float(np.std(filtered)),
            "throughput_rps": len(latencies) / (sum(latencies) / 1000) if sum(latencies) > 0 else 0,
            "error_rate": errors / total,
            "total_requests": total,
            "successful_requests": len(latencies)
        }
    
    def run_comprehensive_test(self):
        """Комплексное тестирование производительности"""
        print("=" * 60)
        print("COMPREHENSIVE ML SERVICE PERFORMANCE TEST")
        print("=" * 60)
        
        loop = asyncio.get_event_loop()
        
        # Тест 1: Одиночные запросы
        print("\n1. Single Prediction Test")
        single_metrics = loop.run_until_complete(
            self.test_single_predictions(1000)
        )
        self._print_metrics(single_metrics)
        
        # Тест 2: Batch запросы (разные размеры)
        print("\n2. Batch Prediction Tests")
        batch_sizes = [1, 4, 8, 16, 32, 64]
        
        for batch_size in batch_sizes:
            print(f"\n  Batch size: {batch_size}")
            batch_metrics = loop.run_until_complete(
                self.test_batch_predictions(batch_size, 100)
            )
            self._print_metrics(batch_metrics)
            
            # Расчет эффективности batch
            if batch_size > 1:
                efficiency = single_metrics["mean_ms"] / (batch_metrics["mean_ms"] / batch_size)
                print(f"  Batch efficiency: {efficiency:.2f}x")
        
        # Тест 3: Long-running test
        print("\n3. Long-running Stability Test")
        stability_metrics = loop.run_until_complete(
            self.test_single_predictions(5000)
        )
        self._print_metrics(stability_metrics)
        
        # Вывод рекомендаций
        self._print_recommendations(single_metrics, batch_metrics)
    
    def _print_metrics(self, metrics: Dict):
        """Вывод метрик"""
        print(f"    Latency p50: {metrics.get('p50_ms', 0):.2f} ms")
        print(f"    Latency p95: {metrics.get('p95_ms', 0):.2f} ms")
        print(f"    Latency p99: {metrics.get('p99_ms', 0):.2f} ms")
        print(f"    Mean latency: {metrics.get('mean_ms', 0):.2f} ms")
        print(f"    Throughput: {metrics.get('throughput_rps', 0):.2f} RPS")
        print(f"    Error rate: {metrics.get('error_rate', 0):.4f}")
    
    def _print_recommendations(self, single_metrics: Dict, batch_metrics: Dict):
        """Вывод рекомендаций по оптимизации"""
        print("\n" + "=" * 60)
        print("OPTIMIZATION RECOMMENDATIONS")
        print("=" * 60)
        
        # Анализ latency
        p95 = single_metrics.get("p95_ms", 0)
        
        if p95 < 75:
            print("✅ SLA выполнено: p95 latency < 75 ms")
        elif p95 < 100:
            print("⚠️  SLA близко к нарушению: p95 latency < 100 ms")
            print("   Рекомендации:")
            print("   - Увеличить размер batch (сейчас 32)")
            print("   - Увеличить кэш Redis")
            print("   - Оптимизировать модель (квантование)")
        else:
            print("❌ SLA нарушено: p95 latency > 100 ms")
            print("   Критические рекомендации:")
            print("   - Увеличить ресурсы ML-сервиса")
            print("   - Включить GPU инференс")
            print("   - Реализовать model ensemble")
        
        # Анализ throughput
        throughput = single_metrics.get("throughput_rps", 0)
        if throughput < 100:
            print(f"\n⚠️  Низкий throughput: {throughput:.2f} RPS")
            print("   Рекомендации:")
            print("   - Увеличить параллелизм Flink job")
            print("   - Использовать connection pooling")
            print("   - Оптимизировать Feature Store queries")
        
        # Анализ batch efficiency
        if batch_metrics:
            batch_size = 32  # пример
            single_mean = single_metrics.get("mean_ms", 0)
            batch_mean = batch_metrics.get("mean_ms", 0)
            
            if batch_mean > 0:
                efficiency = (single_mean * batch_size) / batch_mean
                print(f"\n📊 Batch efficiency ({batch_size}): {efficiency:.2f}x")
                
                if efficiency < 1.5:
                    print("   Рекомендации:")
                    print("   - Уменьшить overhead batch обработки")
                    print("   - Оптимизировать сериализацию")
                    print("   - Использовать более эффективный формат (Protocol Buffers)")

async def main():
    tester = MLServiceTester("http://localhost:8000")
    tester.run_comprehensive_test()

if __name__ == "__main__":
    asyncio.run(main())