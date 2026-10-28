"""Resource governor and adaptive concurrency slot manager for Flowguard."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class ResourceSnapshot:
    cpu_load_1m: float
    cpu_count: int
    ram_total_mb: float
    ram_available_mb: float
    timestamp: float

    @property
    def load_ratio(self) -> float:
        if self.cpu_count <= 0:
            return 1.0
        return self.cpu_load_1m / float(self.cpu_count)

    @property
    def ram_free_gb(self) -> float:
        return self.ram_available_mb / 1024.0


@dataclass(frozen=True)
class ResourceLimits:
    min_free_ram_gb: float = 2.0
    max_cpu_load_ratio: float = 0.90
    max_concurrent_slots: int = 8
    min_concurrent_slots: int = 1
    llm_token_bucket_rpm: int = 120
    resource_capacity: Mapping[str, int] = field(default_factory=dict)
    exclusive_prefixes: tuple[str, ...] = ("writer:",)
    max_rate_limit_retries: int = 3
    default_retry_after_seconds: float = 15.0
    max_io_slots: int = 8

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "ResourceLimits":
        """Build limits from a policy document; unknown keys fail closed."""
        known = {f.name for f in fields(cls)}
        unknown = sorted(set(data) - known)
        if unknown:
            raise ValueError(f"Unknown flowguard limit keys: {', '.join(unknown)}")
        values = dict(data)
        if "exclusive_prefixes" in values:
            values["exclusive_prefixes"] = tuple(values["exclusive_prefixes"])
        if "resource_capacity" in values:
            values["resource_capacity"] = {str(k): int(v) for k, v in values["resource_capacity"].items()}
        return cls(**values)

    def capacity_of(self, resource: str) -> int | None:
        """Return the capacity of a named resource; ``None`` means unbounded."""
        if resource in self.resource_capacity:
            return self.resource_capacity[resource]
        if resource.startswith(self.exclusive_prefixes):
            return 1
        return None


class FlowGovernor:
    """Monitors host telemetry and dynamically calculates admissible execution concurrency."""

    def __init__(
        self,
        limits: ResourceLimits | None = None,
        snapshot_source: Callable[[], ResourceSnapshot] | None = None,
    ) -> None:
        self.limits = limits or ResourceLimits()
        self._snapshot_source = snapshot_source or self.read_host_snapshot
        self._provider_backoff_until: dict[str, float] = {}
        self._current_slots = max(1, self.limits.max_concurrent_slots // 2)
        self._last_adjustment_time = time.monotonic()
        self._rate_limit_backoff_until = 0.0

    @classmethod
    def read_host_snapshot(cls) -> ResourceSnapshot:
        cpu_count = os.cpu_count() or 1
        load_1m = 0.5
        try:
            loadavg_path = Path("/proc/loadavg")
            if loadavg_path.exists():
                parts = loadavg_path.read_text(encoding="utf-8").split()
                load_1m = float(parts[0])
        except Exception:
            load_1m = 0.5

        ram_total = 16384.0
        ram_avail = 8192.0
        try:
            meminfo_path = Path("/proc/meminfo")
            if meminfo_path.exists():
                info = {}
                for line in meminfo_path.read_text(encoding="utf-8").splitlines():
                    if ":" in line:
                        k, v = line.split(":", 1)
                        info[k.strip()] = v.strip().split()[0]
                if "MemTotal" in info:
                    ram_total = float(info["MemTotal"]) / 1024.0
                if "MemAvailable" in info:
                    ram_avail = float(info["MemAvailable"]) / 1024.0
        except Exception:
            pass

        return ResourceSnapshot(
            cpu_load_1m=load_1m,
            cpu_count=cpu_count,
            ram_total_mb=ram_total,
            ram_available_mb=ram_avail,
            timestamp=time.time(),
        )

    def signal_pressure(self, reason: str = "host_pressure") -> None:
        """Multiplicative decrease upon resource or rate-limit pressure."""
        self._current_slots = max(self.limits.min_concurrent_slots, self._current_slots // 2)
        self._last_adjustment_time = time.monotonic()

    def signal_rate_limit(self, retry_after_seconds: float = 30.0, provider: str | None = None) -> None:
        """Register provider HTTP 429 / RPM depletion and trigger backoff.

        A named provider backs off alone so other providers keep running;
        without a provider the backoff applies to every ``llm*`` task.
        """
        until = time.monotonic() + retry_after_seconds
        if provider is None:
            self._rate_limit_backoff_until = until
            self.signal_pressure("rate_limit")
        else:
            self._provider_backoff_until[provider] = max(until, self._provider_backoff_until.get(provider, 0.0))

    def is_provider_limited(self, provider: str) -> bool:
        return time.monotonic() < self._provider_backoff_until.get(provider, 0.0)

    def snapshot(self) -> ResourceSnapshot:
        return self._snapshot_source()

    def evaluate_io_capacity(self, snapshot: ResourceSnapshot | None = None) -> int:
        """Slots for I/O-bound tasks (remote calls): CPU load does not throttle them, RAM does."""
        snap = snapshot or self._snapshot_source()
        if snap.ram_free_gb < self.limits.min_free_ram_gb:
            return self.limits.min_concurrent_slots
        return self.limits.max_io_slots

    @property
    def is_rate_limited(self) -> bool:
        return time.monotonic() < self._rate_limit_backoff_until

    def evaluate_capacity(self, snapshot: ResourceSnapshot | None = None) -> int:
        """Calculate currently admissible concurrency slot count via AIMD."""
        snap = snapshot or self._snapshot_source()
        now = time.monotonic()

        # Hard guard checks
        is_ram_critical = snap.ram_free_gb < self.limits.min_free_ram_gb
        is_cpu_saturated = snap.load_ratio > self.limits.max_cpu_load_ratio

        if is_ram_critical or is_cpu_saturated:
            self.signal_pressure("threshold_breached")
            return self.limits.min_concurrent_slots

        # Additive increase every 5 seconds if headroom allows
        if now - self._last_adjustment_time >= 5.0:
            if self._current_slots < self.limits.max_concurrent_slots:
                self._current_slots += 1
            self._last_adjustment_time = now

        return self._current_slots
