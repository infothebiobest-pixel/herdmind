import os
import time
import jwt
import httpx
import redis
from fastapi import FastAPI, Request, Response, HTTPException, status
from contextlib import asynccontextmanager

# Network configurations
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://ai_service:8001")
REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))

# Enterprise Secret Keys (Override via env variables in production)
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "herdmind_super_secret_enterprise_key_2026")
JWT_ALGORITHM  = "HS256"

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
async_http_client = httpx.AsyncClient()

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await async_http_client.aclose()

app = FastAPI(title="HerdMind-X API Gateway (Secured)", lifespan=lifespan)

# Rate limiting middleware layer
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
        pass
    return await call_next(request)

# Reusable core reverse proxy mapping logic
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

# SECURED ROUTE: Intercepts and parses authorization signatures before proxy dispatch
@app.api_route("/ai/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def route_to_ai(path: str, request: Request):
    auth_header = request.headers.get("Authorization")
    
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED, 
            detail="Missing or malformed Authorization Bearer header token."
        )
        
    parts = auth_header.split(" ")
    if len(parts) != 2:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token structure.")
        
    token = parts[1] # Extract just the raw token string safely
    try:
        # Cryptographically parse and validate token properties
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        request.state.user = payload.get("sub")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication token has expired.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid cryptographic signature credentials.")

    return await reverse_proxy_handler(AI_SERVICE_URL, path, request)

# Open Health Route
@app.get("/gateway/health")
def gateway_health():
    return {"status": "gateway_operational", "security": "jwt_active"}

# Development Utility Route: Generates a test token for the Farmer Mobile Application
@app.get("/gateway/dev/token")
def generate_dev_token(farmer_id: str = "farmer_001"):
    payload = {
        "sub": farmer_id,
        "exp": time.time() + 3600, # Valid for 1 Hour
        "role": "administrator"
    }
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return {"access_token": token, "token_type": "bearer"}

if __name__ == "__main__":
    import uvicorn
    # Pass the actual application object directly instead of a file lookup string path
    uvicorn.run(app, host="0.0.0.0", port=8000)
