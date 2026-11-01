"""Bounded LRU k-buckets; a full bucket keeps its live oldest contact."""

import ipaddress
import time
from collections import OrderedDict
from dataclasses import dataclass

from dht.ids import BITS, bucket_index, distance, normalize_id


@dataclass(frozen=True)
class Contact:
    node_id: str
    host: str
    port: int

    def __post_init__(self):
        object.__setattr__(self, "node_id", normalize_id(self.node_id))
        ipaddress.IPv4Address(self.host)
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")

    @property
    def addr(self):
        return self.host, self.port

    def to_dict(self):
        return {"node_id": self.node_id, "host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, data):
        return cls(data["node_id"], data["host"], data["port"])


class RoutingTable:
    def __init__(self, my_id, k=20):
        if type(k) is not int or not 1 <= k <= 20:
            raise ValueError("k must be between 1 and 20")
        self.my_id = normalize_id(my_id)
        self.k = k
        self.buckets = [OrderedDict() for _ in range(BITS)]
        self.updated = [time.monotonic()] * BITS

    def add(self, contact):
        """Touch/insert a contact, or return the oldest contact to probe."""
        index = bucket_index(self.my_id, contact.node_id)
        if index < 0:
            return None
        bucket = self.buckets[index]
        if contact.node_id not in bucket and len(bucket) >= self.k:
            return next(iter(bucket.values()))
        bucket[contact.node_id] = contact
        bucket.move_to_end(contact.node_id)
        self.updated[index] = time.monotonic()
        return None

    def remove(self, node_id):
        index = bucket_index(self.my_id, node_id)
        if index >= 0:
            return self.buckets[index].pop(normalize_id(node_id), None)
        return None

    def closest(self, target_id, n=None):
        target_id = normalize_id(target_id)
        if n is None:
            n = self.k
        if type(n) is not int or n < 0:
            raise ValueError("n must be non-negative")
        return sorted(self.contacts(), key=lambda c: distance(c.node_id, target_id))[:n]

    def contacts(self):
        return [contact for bucket in self.buckets for contact in bucket.values()]

    def __len__(self):
        return sum(map(len, self.buckets))
