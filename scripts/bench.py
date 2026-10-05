import asyncio
import time
import httpx
from statistics import median

async def bench_endpoint(client: httpx.AsyncClient, url: str, n: int = 200):
    # Cold request
    start = time.perf_counter()
    resp = await client.get(url)
    cold_time = (time.perf_counter() - start) * 1000
    assert resp.status_code == 200

    times = []
    for _ in range(n - 1):
        start = time.perf_counter()
        resp = await client.get(url)
        t = (time.perf_counter() - start) * 1000
        times.append(t)
        assert resp.status_code == 200

    times.sort()
    p50 = median(times)
    p95 = times[int(len(times) * 0.95)]
    return cold_time, p50, p95

async def main():
    endpoints = [
        "http://localhost:8000/metrics/revenue/daily",
        "http://localhost:8000/metrics/revenue/by-region",
        "http://localhost:8000/metrics/cohorts",
        "http://localhost:8000/metrics/top-customers"
    ]

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Check health first
        try:
            r = await client.get("http://localhost:8000/health")
            print("Health check:", r.json())
        except Exception as e:
            print("Server not running. Please start uvicorn first.", e)
            return

        print("\n--- BENCHMARK RESULTS (N=200 requests) ---")
        for ep in endpoints:
            cold, p50, p95 = await bench_endpoint(client, ep, n=200)
            print(f"Endpoint: {ep}")
            print(f"  Cold Cache: {cold:.2f} ms")
            print(f"  Warm Cache p50: {p50:.2f} ms")
            print(f"  Warm Cache p95: {p95:.2f} ms")

        # Get cache stats
        stats_resp = await client.get("http://localhost:8000/admin/cache/stats")
        print("\nCache Stats:", stats_resp.json())

if __name__ == "__main__":
    asyncio.run(main())
