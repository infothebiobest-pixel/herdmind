import os
import time
import httpx
import redis
from fastapi import FastAPI, Request, Response, HTTPException, status
from contextlib import asynccontextmanager

# Network bindings avoiding port 8000 internal network collision
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://ai_service:8001")
REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
async_http_client = httpx.AsyncClient()

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await async_http_client.aclose()

app = FastAPI(title="HerdMind-X API Gateway", lifespan=lifespan)

@app.middleware("http")
async def rate_limiting_middleware(request: Request, call_next):
    client_ip = request.client.host
    bucket = f"rate:{client_ip}:{int(time.time()) // 60}"
    try:
        count = redis_client.incr(bucket)
        if count == 1:
            redis_client.expire(bucket, 60)
        if count > 60:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded.")
    except redis.RedisError:
        pass  # Fail-open if Redis encounters connection blocks
    return await call_next(request)

async def reverse_proxy_handler(target_base_url: str, path: str, request: Request) -> Response:
    url = f"{target_base_url}/{path}"
    headers = dict(request.headers)
    headers.pop("host", None)
    try:
        proxy_response = await async_http_client.request(
            method=request.method,
            url=url,
            params=dict(request.query_params),
            headers=headers,
            content=await request.body(),
            timeout=10.0
        )
        return Response(
            content=proxy_response.content,
            status_code=proxy_response.status_code,
            headers=dict(proxy_response.headers)
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Downstream unreachable: {e}")

@app.api_route("/ai/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def route_to_ai(path: str, request: Request):
    return await reverse_proxy_handler(AI_SERVICE_URL, path, request)

@app.get("/gateway/health")
def gateway_health():
    return {"status": "gateway_operational"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
