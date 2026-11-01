"""Bounded in-memory JSON values with monotonic TTL expiry."""

import json
import math
import time
from collections import OrderedDict

from dht.ids import normalize_id


class ValueStore:
    def __init__(self, capacity=1024, max_value_bytes=4096):
        if capacity < 1 or max_value_bytes < 1:
            raise ValueError("store limits must be positive")
        self.capacity = capacity
        self.max_value_bytes = max_value_bytes
        self._items = OrderedDict()

    def put(self, key, value, ttl=300):
        key = normalize_id(key)
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
            raise TypeError("ttl must be numeric")
        if not math.isfinite(ttl) or not 0 < ttl <= 86400:
            raise ValueError("ttl must be in (0, 86400]")
        raw = json.dumps(value, allow_nan=False).encode()
        if len(raw) > self.max_value_bytes:
            raise ValueError("value exceeds maximum encoded size")
        self._items[key] = (raw, time.monotonic() + ttl)
        self._items.move_to_end(key)
        while len(self._items) > self.capacity:
            self._items.popitem(last=False)

    def get(self, key):
        """Return (found, value); JSON null is a valid stored value."""
        key = normalize_id(key)
        item = self._items.get(key)
        if item is None:
            return False, None
        raw, expires = item
        if expires <= time.monotonic():
            del self._items[key]
            return False, None
        return True, json.loads(raw)
