"""Node registry: persistence, health polling, and last-write-wins merge (peer gossip)."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import time
from dataclasses import dataclass, field
from urllib.parse import quote

import httpx

from .config import settings
from .models import norm, required_models


def parse_address(text: str, default_port: int | None = None) -> tuple[str, int]:
    """Accept '192.168.1.5', '192.168.1.5:8188' or 'http://host:8188/' -> (host, port)."""
    t = text.strip()
    for prefix in ("http://", "https://"):
        if t.lower().startswith(prefix):
            t = t[len(prefix):]
    t = t.split("/")[0]
    if not t:
        raise ValueError("Address is empty")
    host, _, port = t.partition(":")
    if port:
        if not port.isdigit() or not (0 < int(port) < 65536):
            raise ValueError(f"Invalid port: {port}")
        return host, int(port)
    return host, default_port or settings.comfy_port


@dataclass
class Node:
    host: str
    port: int = 8188
    name: str = ""
    source: str = "manual"          # manual | discovered | gossip
    added: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)
    removed: bool = False           # tombstone so removals propagate to peers

    # runtime state (never persisted)
    status: str = "unknown"         # unknown | idle | busy | offline
    running: int = 0
    pending: int = 0
    running_ids: set[str] = field(default_factory=set)   # prompt ids in the node's queue at last poll
    pending_ids: set[str] = field(default_factory=set)
    inflight: int = 0               # jobs we forwarded since the last poll
    latency_ms: float | None = None
    last_seen: float | None = None
    error: str = ""

    # model inventory (runtime only; re-read on startup, first contact, recovery and manual checks)
    models: dict[str, list[str]] | None = None   # folder -> files, as the node reports them
    model_set: set[str] | None = None            # flat set of normalised names; None = unknown
    models_at: float | None = None
    models_error: str = ""
    models_tried: bool = False

    @property
    def id(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def load(self) -> int:
        return self.running + self.pending + self.inflight

    @property
    def online(self) -> bool:
        return self.status in ("idle", "busy")

    def record(self) -> dict:
        return {"host": self.host, "port": self.port, "name": self.name, "source": self.source,
                "added": self.added, "updated": self.updated, "removed": self.removed}

    def view(self) -> dict:
        return {"id": self.id, "host": self.host, "port": self.port, "name": self.name,
                "source": self.source, "status": self.status, "running": self.running,
                "pending": self.pending, "inflight": self.inflight, "load": self.load,
                "latency_ms": self.latency_ms, "last_seen": self.last_seen, "error": self.error,
                "model_count": None if self.model_set is None else len(self.model_set),
                "models_at": self.models_at, "models_error": self.models_error}


class Registry:
    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}
        self.client: httpx.AsyncClient | None = None
        self._poll_lock = asyncio.Lock()
        self._last_poll = float("-inf")   # monotonic time of the last full poll
        self.polled_at: float | None = None  # wall-clock time of the last full poll (for the dashboard)

    # ---- persistence -------------------------------------------------
    def load(self) -> None:
        path = settings.data_file
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            # Unreadable file: keep a copy instead of silently overwriting it on the next save.
            with contextlib.suppress(OSError):
                os.replace(path, path.with_suffix(".bad"))
            return
        except OSError:
            return
        now = time.time()
        for r in data.get("nodes", []):
            try:
                node = Node(host=r["host"], port=int(r.get("port", 8188)), name=r.get("name", ""),
                            source=r.get("source", "manual"), added=r.get("added", now),
                            updated=r.get("updated", now), removed=bool(r.get("removed", False)))
            except (KeyError, ValueError, TypeError):
                continue
            if node.removed and now - node.updated > settings.tombstone_ttl:
                continue
            self.nodes[node.id] = node

    def save(self) -> None:
        path = settings.data_file
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"nodes": self.records()}, indent=2), encoding="utf-8")
        os.replace(tmp, path)  # atomic: a crash never leaves a half-written file

    def records(self) -> list[dict]:
        return [n.record() for n in self.nodes.values()]

    # ---- membership --------------------------------------------------
    def add(self, address: str, name: str = "", source: str = "manual") -> Node:
        host, port = parse_address(address)
        nid = f"{host}:{port}"
        node = self.nodes.get(nid)
        if node is None:
            node = Node(host=host, port=port, name=name, source=source)
            self.nodes[nid] = node
        else:
            if node.removed:
                node.removed, node.source = False, source
                node.updated = time.time()
            if name and name != node.name:
                node.name, node.updated = name, time.time()
        self.save()
        return node

    def remove(self, node_id: str) -> bool:
        node = self.nodes.get(node_id)
        if not node or node.removed:
            return False
        node.removed, node.updated = True, time.time()
        node.status = "unknown"
        self.save()
        return True

    def merge(self, records: list[dict]) -> int:
        """Last-write-wins merge of a peer's node list. Returns number of changes."""
        changed = 0
        for r in records:
            try:
                host, port = r["host"], int(r.get("port", 8188))
                updated = float(r.get("updated", 0))
            except (KeyError, ValueError, TypeError):
                continue
            nid = f"{host}:{port}"
            existing = self.nodes.get(nid)
            if existing is not None and updated <= existing.updated:
                continue
            if existing is None and r.get("removed"):
                continue  # don't import tombstones for nodes we never knew
            node = existing or Node(host=host, port=port)
            node.name = r.get("name", node.name)
            node.source = "gossip" if existing is None else node.source
            node.added = r.get("added", node.added)
            node.updated = updated
            node.removed = bool(r.get("removed", False))
            self.nodes[nid] = node
            changed += 1
        if changed:
            self.save()
        return changed

    def live(self) -> list[Node]:
        return [n for n in self.nodes.values() if not n.removed]

    def online(self) -> list[Node]:
        return [n for n in self.live() if n.online]

    def known_models(self) -> set[str]:
        """Every model file any live node has reported."""
        known: set[str] = set()
        for n in self.live():
            if n.model_set:
                known |= n.model_set
        return known

    def required_for(self, body: bytes) -> set[str]:
        return required_models(body, self.known_models())

    def ranked(self, required: set[str] | frozenset[str] = frozenset()) -> list[Node]:
        """Online nodes that can run the workflow, least loaded first.

        Nodes known to have every required model come first; nodes whose inventory is unknown
        (older ComfyUI, fetch failed) come after them; nodes known to lack a model are excluded.
        Random tie-break spreads work across equals.
        """
        scored = []
        for n in self.online():
            if not required or (n.model_set is not None and required <= n.model_set):
                tier = 0
            elif n.model_set is None:
                tier = 1
            else:
                continue
            scored.append((tier, n.load, random.random(), n))
        scored.sort(key=lambda t: t[:3])
        return [t[3] for t in scored]

    def missing_by_node(self, required: set[str]) -> dict[str, list[str]]:
        return {n.id: sorted(required - n.model_set)[:20]
                for n in self.live() if n.model_set is not None and not required <= n.model_set}

    def capable_offline(self, required: set[str]) -> list[str]:
        return [n.id for n in self.live()
                if not n.online and n.model_set is not None and required <= n.model_set]

    def mark_offline(self, node: Node) -> None:
        node.status = "offline"
        node.running = node.pending = node.inflight = 0
        node.running_ids = set()
        node.pending_ids = set()

    async def refresh_models(self, node_id: str | None = None) -> list[dict]:
        """Re-read model files from one node (or all). Reports what changed on each."""
        targets = [n for n in self.live() if node_id in (None, n.id)]

        async def one(n: Node) -> dict:
            before = set(n.model_set) if n.model_set is not None else None
            await self.probe(n, 3.0, models=True)   # also updates the node's status
            name = n.name or n.host
            if not n.online:
                return {"id": n.id, "name": name, "skipped": "offline"}
            after = n.model_set
            if n.models_error:
                return {"id": n.id, "name": name, "error": n.models_error}
            added = len(after - before) if before is not None and after is not None else 0
            removed = len(before - after) if before is not None and after is not None else 0
            return {"id": n.id, "name": name, "added": added, "removed": removed,
                    "total": len(after or ())}

        return list(await asyncio.gather(*(one(n) for n in targets)))

    # ---- health polling ---------------------------------------------
    async def refresh(self, max_age: float = 0.0, timeout: float = 3.0, models: bool = False) -> None:
        """Poll every node once, concurrently. Simultaneous callers share a single poll, and a poll
        newer than max_age seconds is reused, so a burst of requests causes one round of checks."""
        async with self._poll_lock:
            if time.monotonic() - self._last_poll < max_age:
                return
            nodes = self.live()
            if nodes:
                await asyncio.gather(*(self.probe(n, timeout, models) for n in nodes))
            self._last_poll = time.monotonic()
            self.polled_at = time.time()

    async def startup(self) -> None:
        """One-off check when the service starts: pull peers' node lists, then poll everything."""
        try:
            await self.gossip_once()
        except Exception:
            pass
        await self.refresh()

    async def probe(self, node: Node, timeout: float = 3.0, models: bool = False) -> None:
        assert self.client is not None
        started = time.perf_counter()
        try:
            r = await self.client.get(f"{node.url}/queue", timeout=timeout)
            r.raise_for_status()
            q = r.json()
            node.running_ids = {i[1] for i in q.get("queue_running", []) if len(i) > 1}
            node.pending_ids = {i[1] for i in q.get("queue_pending", []) if len(i) > 1}
            node.running = len(q.get("queue_running", []))
            node.pending = len(q.get("queue_pending", []))
            node.inflight = 0  # anything we forwarded is now visible in /queue
            node.latency_ms = round((time.perf_counter() - started) * 1000, 1)
            node.last_seen = time.time()
            node.error = ""
            node.status = "busy" if node.running + node.pending > 0 else "idle"
        except Exception as exc:  # any failure = unreachable/unhealthy
            self.mark_offline(node)
            node.error = type(exc).__name__
            node.models_tried = False  # re-read its models when it comes back
            return
        if models or not node.models_tried:
            await self.fetch_models(node)

    async def fetch_models(self, node: Node) -> None:
        """Read the node's model files via /models and /models/{folder}. Keeps any earlier
        inventory if this attempt fails, and never leaves a half-read inventory behind."""
        assert self.client is not None
        node.models_tried = True
        try:
            r = await self.client.get(f"{node.url}/models", timeout=5)
            r.raise_for_status()
            folders = [f for f in r.json() if isinstance(f, str) and f != "custom_nodes"][:40]

            async def one(folder: str) -> tuple[str, list[str]]:
                rr = await self.client.get(f"{node.url}/models/{quote(folder)}", timeout=10)
                rr.raise_for_status()
                return folder, [x for x in rr.json() if isinstance(x, str)]

            found = await asyncio.gather(*(one(f) for f in folders))  # any failure aborts the whole read
            node.models = {f: sorted(files) for f, files in found if files}
            node.model_set = {norm(x) for files in node.models.values() for x in files}
            node.models_at = time.time()
            node.models_error = ""
        except Exception as exc:
            node.models_error = type(exc).__name__

    async def poll_loop(self) -> None:
        """Optional background timer; only started when CLUSTER_POLL > 0."""
        while True:
            await self.refresh()
            await asyncio.sleep(settings.poll_interval)

    # ---- gossip ------------------------------------------------------
    async def gossip_once(self) -> None:
        assert self.client is not None
        hosts = {n.host for n in self.live()}

        async def pull(host: str) -> list[dict]:
            try:
                r = await self.client.get(f"http://{host}:{settings.port}/cluster/api/gossip", timeout=2)
                if r.status_code == 200:
                    return r.json().get("nodes", [])
            except Exception:
                pass
            return []

        for records in await asyncio.gather(*(pull(h) for h in hosts)):
            self.merge(records)

    async def gossip_loop(self) -> None:
        await asyncio.sleep(3)
        while True:
            try:
                await self.gossip_once()
            except Exception:
                pass
            await asyncio.sleep(settings.gossip_interval)


registry = Registry()
