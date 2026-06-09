import os
import time
import jwt
import httpx
import redis
from fastapi import FastAPI, Request, Response, HTTPException, status
from pydantic import BaseModel
from contextlib import asynccontextmanager

# 1. Native Explicit Relative Submodule Imports
from .auth_db import init_auth_tables, create_user, verify_user_credentials

# Network configurations
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://ai_service:8001")
REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "herdmind_super_secret_enterprise_key_2026")
JWT_ALGORITHM  = "HS256"

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
async_http_client = httpx.AsyncClient()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🛠️ [Gateway] Initializing user database infrastructure...")
    init_auth_tables()
    yield
    await async_http_client.aclose()

app = FastAPI(title="HerdMind-X API Gateway (Secured)", lifespan=lifespan)

class UserAuthSchema(BaseModel):
    username: str
    password: str

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

# AUTH ENDPOINT 1: Account Creation Path
@app.post("/api/auth/register", status_code=201)
async def register_farmer_account(user: UserAuthSchema):
    new_user = create_user(user.username, user.password)
    if not new_user:
        raise HTTPException(status_code=400, detail="Username already exists in system databases.")
    return {"status": "success", "message": "Farmer registration completed.", "user_id": str(new_user)}

# AUTH ENDPOINT 2: Authentication Login Path returning active JWTs
@app.post("/api/auth/login")
async def login_farmer_account(user: UserAuthSchema):
    verified_profile = verify_user_credentials(user.username, user.password)
    if not verified_profile:
        raise HTTPException(status_code=401, detail="Invalid username or password credentials.")
        
    payload = {
        "sub": verified_profile["username"],
        "exp": time.time() + 86400,
        "role": verified_profile["role"]
    }
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return {"access_token": token, "token_type": "bearer", "farmer": verified_profile["username"]}

# SECURED ROUTE: Validates authentication token + checks RBAC roles before proxying
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
        
    token = parts[1]
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        request.state.user = payload.get("sub")
        request.state.role = payload.get("role", "guest")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication token has expired.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid cryptographic signature credentials.")

    # Explicit RBAC Privilege Verification Check
    allowed_roles = ["administrator", "farm_manager", "vet"]
    if request.state.role not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied. Role '{request.state.role}' has insufficient security privileges."
        )

    return await reverse_proxy_handler(AI_SERVICE_URL, path, request)

@app.get("/gateway/health")
def gateway_health():
    return {"status": "gateway_operational", "security": "database_jwt_active"}

@app.get("/gateway/dev/token")
def generate_dev_token(farmer_id: str = "farmer_001"):
    payload = {"sub": farmer_id, "exp": time.time() + 3600, "role": "administrator"}
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return {"dev_access_token": token, "token_type": "bearer"}
