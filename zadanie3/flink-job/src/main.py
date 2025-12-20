import requests
import time
import random
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_ml_service(max_attempts=30, delay=2):
    """Ждем запуска ML-сервиса"""
    print("Waiting for ML service to start...")
    for attempt in range(max_attempts):
        try:
            response = requests.get("http://ml-service:8000/health", timeout=2)
            if response.status_code == 200:
                print(f"✓ ML service is ready (attempt {attempt+1}/{max_attempts})")
                return True
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}/{max_attempts}: {e}")
            time.sleep(delay)
    return False

class MLIntegrationTest:
    def __init__(self):
        self.base_url = "http://ml-service:8000"
    
    def test_health(self):
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            print(f"Health check: {response.status_code}")
            return response.status_code == 200
        except Exception as e:
            print(f"Health check failed: {e}")
            return False

def main():
    """Основная функция"""
    print("=" * 60)
    print("ML INTEGRATION TEST")
    print("=" * 60)
    
    # Ждем ML-сервис
    if not wait_for_ml_service():
        print("❌ ML service not available")
        return
    
    tester = MLIntegrationTest()
    
    if tester.test_health():
        print("\n" + "=" * 60)
        print("✅ ML SERVICE IS WORKING!")
        print("=" * 60)
        
        # Простой тест
        try:
            print("\nTesting single prediction...")
            response = requests.post(
                "http://ml-service:8000/predict/single",
                json={"user_id": "test_user", "product_id": "test_product"},
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✓ Success: {result}")
            else:
                print(f"✗ Error: {response.status_code}")
                
        except Exception as e:
            print(f"✗ Exception: {e}")
    else:
        print("❌ ML service health check failed")

if __name__ == "__main__":
    main()