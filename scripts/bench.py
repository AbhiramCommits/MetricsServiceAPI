from __future__ import annotations

import argparse
import asyncio
import os
import time
from statistics import median

import httpx

DEFAULT_BASE_URL = os.environ.get("METRICS_BASE_URL", "http://localhost:8000")

ENDPOINTS = [
    "/metrics/revenue/daily?limit=100",
    "/metrics/revenue/by-region",
    "/metrics/cohorts",
    "/metrics/top-customers?top_n=5",
]


async def _time_request(client: httpx.AsyncClient, url: str) -> float:
    start = time.perf_counter()
    resp = await client.get(url)
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert resp.status_code == 200, f"{url} -> {resp.status_code}"
    return elapsed_ms


async def bench_endpoint(
    client: httpx.AsyncClient, url: str, n: int = 200
) -> tuple[float, float, float]:
    # First request is served from a cold cache.
    cold_ms = await _time_request(client, url)

    warm = [await _time_request(client, url) for _ in range(n - 1)]
    warm.sort()
    p50 = median(warm)
    p95 = warm[min(int(len(warm) * 0.95), len(warm) - 1)]
    return cold_ms, p50, p95


async def main(base_url: str, n: int) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            health = await client.get(f"{base_url}/health")
        except Exception as exc:  # noqa: BLE001
            print(f"Server not reachable at {base_url}: {exc}")
            return
        print("Health:", health.json())

        # Reset cache stats so the reported hit rate reflects this run.
        await client.post(f"{base_url}/admin/cache/invalidate")

        print(f"\n--- BENCHMARK (N={n} requests/endpoint) ---")
        for path in ENDPOINTS:
            url = f"{base_url}{path}"
            cold, p50, p95 = await bench_endpoint(client, url, n=n)
            print(f"{path}")
            print(f"  cold: {cold:8.2f} ms")
            print(f"  warm p50: {p50:8.2f} ms")
            print(f"  warm p95: {p95:8.2f} ms")

        stats = (await client.get(f"{base_url}/admin/cache/stats")).json()
        print("\nCache stats:", stats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--n", type=int, default=200)
    args = parser.parse_args()
    asyncio.run(main(args.base_url, args.n))
