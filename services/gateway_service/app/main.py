import os
import time
import asyncio
import json
import socket
import jwt
import httpx
import redis
import redis.asyncio as aioredis
from fastapi import FastAPI, Request, Response, HTTPException, status, WebSocket, WebSocketDisconnect, Query
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import List

# 1. Native Explicit Relative Submodule Imports
from .auth_db import init_auth_tables, create_user, verify_user_credentials

# Network configurations
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://ai_service:8001")
REDIS_HOST     = os.getenv("REDIS_HOST", "herd_redis") # Corrected underscore alignment
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "herdmind_super_secret_enterprise_key_2026")
JWT_ALGORITHM  = "HS256"

# Sync client for legacy HTTP middleware rate limiting
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
async_http_client = httpx.AsyncClient()

# ================= WEBSOCKET MANAGEMENT CONNECTOR =================

class WebSocketConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"📡 [Gateway] New dashboard socket opened. Total active paths: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"🛑 [Gateway] Dashboard socket closed. Remaining active paths: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

manager = WebSocketConnectionManager()

# ================= BACKGROUND DYNAMIC REDIS STREAM BRIDGE =================

async def redis_alerts_stream_listener():
    """
    Dynamically tracks unique Blue/Green container IDs using socket hostnames
    to listen to the shared Redis Stream matrix safely.
    """
    STREAM_KEY = "herd:alerts:stream"
    GROUP_NAME = "gateway_group"
    CONSUMER_NAME = socket.gethostname() 

    print(f"🚀 [Gateway] Redis Stream Listener started for instance: {CONSUMER_NAME}")
    
    while True:
        try:
            async_redis = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}/0", decode_responses=True)
            
            try:
                await async_redis.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
                print(f"✅ [Gateway] Consumer group '{GROUP_NAME}' initialized.")
            except Exception as e:
                if "BUSYGROUP" not in str(e):
                    raise

            while True:
                response = await async_redis.xreadgroup(
                    groupname=GROUP_NAME,
                    consumername=CONSUMER_NAME,
                    streams={STREAM_KEY: ">"},
                    count=10,
                    block=2000
                )

                if not response:
                    continue

                for stream, messages in response:
                    for message_id, payload in messages:
                        raw = payload.get("data")
                        if not raw:
                            continue

                        try:
                            event = json.loads(raw)
                        except Exception:
                            continue

                        if "cow_id" not in event and "animal_id" not in event:
                            continue

                        await manager.broadcast(raw)
                        await async_redis.xack(STREAM_KEY, GROUP_NAME, message_id)

        except asyncio.CancelledError:
            print(f"🛑 [Gateway] Stream Listener instance {CONSUMER_NAME} cleanly shut down.")
            break
        except Exception as e:
            print(f"❌ [Gateway] Stream processing error: {e}. Re-establishing pool in 3s...")
            await asyncio.sleep(3)

# ================= LIFECYCLE MANAGEMENT =================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🛠️ [Gateway] Initializing user database infrastructure...")
    init_auth_tables()
    pubsub_task = asyncio.create_task(redis_alerts_stream_listener())
    yield
    pubsub_task.cancel()
    await async_http_client.aclose()

app = FastAPI(title="HerdMind-X API Gateway (Secured)", lifespan=lifespan)

class UserAuthSchema(BaseModel):
    username: str
    password: str

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

@app.post("/api/auth/register", status_code=201)
async def register_farmer_account(user: UserAuthSchema):
    new_user = create_user(user.username, user.password)
    if not new_user:
        raise HTTPException(status_code=400, detail="Username already exists in system databases.")
    return {"status": "success", "message": "Farmer registration completed.", "user_id": str(new_user)}

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

@app.websocket("/ws/alerts")
async def websocket_alerts_endpoint(websocket: WebSocket, token: str = Query(None)):
    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        role = payload.get("role", "guest")
        allowed_roles = ["administrator", "farm_manager", "vet"]
        if role not in allowed_roles:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
    except jwt.PyJWTError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.api_route("/ai/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def route_to_ai(path: str, request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED, 
            detail="Missing or malformed Authorization Bearer header token."
        )
    return await reverse_proxy_handler(AI_SERVICE_URL, path, request)
