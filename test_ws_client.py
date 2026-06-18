import asyncio
import websockets
import json

async def test_listen():
    # Pass your live generated JWT token directly into the query string matrix
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbl9mYXJtZXIiLCJleHAiOjE3ODE4NTk5OTMuODExODU3Miwicm9sZSI6ImZhcm1fbWFuYWdlciJ9.vwjDIouzz9psMWeV1onsIeB0DU1xrO7VuKIZZlzmhQA"
    
    # Target port 80 through the Nginx proxy layer subpath mapping
    url = f"ws://localhost/ws/alerts?token={token}"
    
    print(f"📡 Attempting WebSocket connection to: {url}")
    try:
        async with websockets.connect(url) as websocket:
            print("✅ Connected successfully to HerdMind-X Live Proxy Stream!")
            print("👀 Waiting for live alert messages... (Press CTRL+C to stop)")
            
            while True:
                message = await websocket.recv()
                print(f"📥 [LIVE EVENT RECEIVE]: {message}")
                
    except Exception as e:
        print(f"❌ WebSocket connection failure: {e}")

if __name__ == "__main__":
    asyncio.run(test_listen())
