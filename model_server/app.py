from fastapi import FastAPI
import redis

app = FastAPI()
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

@app.get("/")
def read_root():
    return {"message": "Model Server is running", "status": "ok"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}