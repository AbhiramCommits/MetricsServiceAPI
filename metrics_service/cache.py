import time
from typing import Any, Dict, Optional, Tuple

class TTLCache:
    def __init__(self, ttl_seconds: int = 60):
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def _now(self) -> float:
        return time.monotonic()

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            self.misses += 1
            return None
        value, timestamp = self._cache[key]
        if self._now() - timestamp > self.ttl_seconds:
            # Expired
            del self._cache[key]
            self.evictions += 1
            self.misses += 1
            return None
        self.hits += 1
        return value

    def set(self, key: str, value: Any) -> None:
        self._cache[key] = (value, self._now())

    def invalidate(self, prefix: Optional[str] = None) -> int:
        if prefix is None:
            count = len(self._cache)
            self._cache.clear()
            return count
        keys_to_delete = [k for k in self._cache.keys() if k.startswith(prefix)]
        for k in keys_to_delete:
            del self._cache[k]
        return len(keys_to_delete)

    def stats(self) -> Dict[str, Any]:
        total_requests = self.hits + self.misses
        hit_rate = (self.hits / total_requests) if total_requests > 0 else 0.0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(hit_rate, 4),
            "size": len(self._cache),
            "evictions": self.evictions,
        }
