# Total successful requests: 27
# Total time taken: 3.43s

# Client a:
# Successful requests: 15
# Time taken: 2.47s

# Client b:
# Successful requests: 12
# Time taken: 3.40s




import time
import random
import uuid
import asyncio
from collections import deque
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

CLASSIFICATION_SERVER_URL = "http://localhost:8001/classify"

BATCH_SIZE = 5           # Max batch size allowed by backend
SMALL_STRING_THRESHOLD = 30  # Strings ≤ this length go to small queue
SMALL_BATCH_TIMEOUT = 0.07  # 100 ms max wait for small batches
LARGE_BATCH_TIMEOUT = 0.4  # 500 ms max wait for large batches
REQUEST_TIMEOUT = 30       # Timeout per client request in seconds
MAX_RETRIES = 4            # Max retries on 429 or transient errors

app = FastAPI(
    title="Classification Proxy with Size-Aware Batching",
    description="Async batch proxy with separate small/large queues and caching"
)


class ProxyRequest(BaseModel):
    sequence: str


class ProxyResponse(BaseModel):
    result: str


@dataclass
class PendingRequest:
    request_id: str
    sequence: str
    future: asyncio.Future


small_request_queue = deque()
large_request_queue = deque()
queue_lock = asyncio.Lock()

# Simple in-memory cache mapping sequence->result
cache = {}
cache_lock = asyncio.Lock()

# Single backend concurrency lock - only one request at a time allowed
backend_lock = asyncio.Lock()


@app.on_event("startup")
async def startup_event():
    # Start two batch workers: one for small, one for large
    asyncio.create_task(batch_worker(small_request_queue, SMALL_BATCH_TIMEOUT, "small"))
    asyncio.create_task(batch_worker(large_request_queue, LARGE_BATCH_TIMEOUT, "large"))


@app.on_event("shutdown")
async def shutdown_event():
    pass


@app.post("/proxy_classify")
async def proxy_classify(req: ProxyRequest) -> ProxyResponse:
    # Check cache first
    async with cache_lock:
        cached_result = cache.get(req.sequence)
        if cached_result is not None:
            return ProxyResponse(result=cached_result)

    # Choose queue based on string length
    future = asyncio.get_event_loop().create_future()
    request_id = str(uuid.uuid4())
    pending = PendingRequest(request_id, req.sequence, future)

    queue = small_request_queue if len(req.sequence) <= SMALL_STRING_THRESHOLD else large_request_queue

    async with queue_lock:
        queue.append(pending)

    try:
        result = await asyncio.wait_for(future, timeout=REQUEST_TIMEOUT)
    except asyncio.TimeoutError:
        # Remove request if still queued on timeout
        async with queue_lock:
            try:
                queue.remove(pending)
            except ValueError:
                pass
        raise HTTPException(status_code=504, detail="Request timed out")

    # Cache the result once received
    async with cache_lock:
        cache[req.sequence] = result

    return ProxyResponse(result=result)


async def batch_worker(queue: deque, batch_timeout: float, queue_name: str):
    async with httpx.AsyncClient() as client:
        while True:
            batch, futures = await collect_batch(queue, batch_timeout)
            if not batch:
                await asyncio.sleep(0.05)
                continue

            attempt = 0
            while attempt < MAX_RETRIES:
                try:
                    # Serialize calls to backend server
                    async with backend_lock:
                        response = await client.post(
                            CLASSIFICATION_SERVER_URL,
                            json={"sequences": batch},
                            timeout=REQUEST_TIMEOUT
                        )

                    if response.status_code == 200:
                        results = response.json().get("results", [])
                        for fut, res in zip(futures, results):
                            if not fut.done():
                                fut.set_result(res)
                        break

                    elif response.status_code == 429:
                        backoff = min(2 ** attempt * 0.2, 5) + random.uniform(0, 0.1)
                        print(f"[{queue_name}] Rate limited; backing off {backoff:.2f}s, attempt {attempt + 1}")
                        await asyncio.sleep(backoff)
                        attempt += 1

                    else:
                        error_msg = f"[{queue_name}] Classification failed with status {response.status_code}"
                        print(error_msg)
                        for fut in futures:
                            if not fut.done():
                                fut.set_exception(HTTPException(status_code=response.status_code, detail=error_msg))
                        break

                except (httpx.RequestError, httpx.TimeoutException) as exc:
                    print(f"[{queue_name}] Request error: {exc}; attempt {attempt + 1}")
                    attempt += 1
                    if attempt >= MAX_RETRIES:
                        for fut in futures:
                            if not fut.done():
                                fut.set_exception(HTTPException(status_code=500, detail=f"Request failed: {str(exc)}"))
                    else:
                        await asyncio.sleep(0.5 * attempt)

            await asyncio.sleep(0.01)


async def collect_batch(queue: deque, batch_timeout: float):
    batch = []
    futures = []
    start_time = time.time()

    # Wait up to batch_timeout for first request to appear if queue empty
    while True:
        async with queue_lock:
            if queue:
                break
        if time.time() - start_time > batch_timeout:
            return [], []
        await asyncio.sleep(0.01)

    async with queue_lock:
        while len(batch) < BATCH_SIZE and queue:
            pending = queue.popleft()
            batch.append(pending.sequence)
            futures.append(pending.future)

    # If batch smaller than max, optionally wait remainder of timeout to fill more
    if len(batch) < BATCH_SIZE:
        elapsed = time.time() - start_time
        remaining = batch_timeout - elapsed
        if remaining > 0:
            await asyncio.sleep(min(remaining, 0.01))

    return batch, futures

