"""Request monitor: the last few job submissions, which node got them, and whether they finished.

Kept in memory only (cleared on restart). Completion is learned from the node itself:
a job that is in the node's /queue is queued or running; once it leaves the queue, its
/history entry says whether it succeeded or failed. Nothing runs on a timer: checks happen only
while at least one job is unfinished (every few seconds, stopping when the last one ends), or on a manual "Check nodes now".
"""
from __future__ import annotations

import asyncio
import itertools
import time
from collections import deque
from dataclasses import dataclass, field

import httpx

from .config import settings
from .registry import Registry

ACTIVE = ("queued", "running")


@dataclass
class Job:
    seq: int
    received: float
    client: str = ""
    models: list[str] = field(default_factory=list)   # model files the workflow uses (first few)
    node: str | None = None                            # node id that accepted the job
    tried: list[str] = field(default_factory=list)     # nodes that were unreachable or declined it first
    prompt_id: str | None = None
    status: str = "routing"   # routing | queued | running | completed | failed | rejected | lost | unknown
    detail: str = ""
    finished: float | None = None
    run_seconds: float | None = None
    misses: int = 0           # consecutive checks where the node didn't answer


def _run_info(messages: list) -> tuple[float | None, str]:
    """Pull run time and a short error text out of ComfyUI's history status messages."""
    start = end = None
    error = ""
    for m in messages or []:
        if not (isinstance(m, (list, tuple)) and len(m) > 1 and isinstance(m[1], dict)):
            continue
        kind, data = m[0], m[1]
        ts = data.get("timestamp")
        if kind == "execution_start" and ts:
            start = ts
        elif kind in ("execution_success", "execution_error", "execution_interrupted") and ts:
            end = ts
        if kind == "execution_error" and not error:
            text = str(data.get("exception_message") or data.get("exception_type") or "Execution error").strip()
            where = data.get("node_type")
            error = (f"{where}: " if where else "") + text[:200]
        elif kind == "execution_interrupted" and not error:
            error = "Interrupted"
    seconds = round((end - start) / 1000, 1) if start and end and end >= start else None
    return seconds, error


class Monitor:
    def __init__(self, registry: Registry) -> None:
        self.registry = registry
        self.client: httpx.AsyncClient | None = None
        self.jobs: deque[Job] = deque(maxlen=settings.monitor_size)
        self._seq = itertools.count(1)
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    # ---- recording (called by the router) ----------------------------
    def start(self, client: str, models: set[str]) -> Job:
        job = Job(seq=next(self._seq), received=time.time(), client=client, models=sorted(models)[:5])
        self.jobs.append(job)
        return job

    def accepted(self, job: Job, node_id: str, prompt_id: str | None) -> None:
        job.node, job.prompt_id, job.status = node_id, prompt_id, "queued"
        if not prompt_id:  # node answered 200 but gave us nothing to track
            job.status, job.detail = "unknown", "The node accepted the job but returned no prompt id"

    def rejected(self, job: Job, detail: str) -> None:
        job.status, job.detail, job.finished = "rejected", detail, time.time()

    # ---- checking -----------------------------------------------------
    def _unfinished(self) -> list[Job]:
        return [j for j in self.jobs if j.status in ACTIVE and j.node and j.prompt_id]

    def ensure_tracking(self) -> None:
        """Start the tracking loop if it isn't already running (called when a job is accepted)."""
        if settings.track_interval <= 0:
            return
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._track_loop())

    async def _track_loop(self) -> None:
        """While any job is unfinished: re-check every node and job each track_interval seconds.
        Ends by itself as soon as nothing is unfinished, so an idle cluster generates no traffic."""
        while self._unfinished():
            try:
                await self.registry.refresh()
                await self.check()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # a bad cycle must never kill tracking while jobs are still running
            if not self._unfinished():
                break
            await asyncio.sleep(settings.track_interval)

    async def check(self) -> None:
        """Update unfinished jobs from the queue data the last node poll collected."""
        async with self._lock:
            by_node: dict[str, list[Job]] = {}
            for j in self._unfinished():
                by_node.setdefault(j.node, []).append(j)
            if by_node:
                await asyncio.gather(*(self._check_node(nid, jobs) for nid, jobs in by_node.items()))

    async def _check_node(self, node_id: str, jobs: list[Job]) -> None:
        node = self.registry.nodes.get(node_id)
        if node is None or node.status == "offline":
            for j in jobs:
                j.misses += 1
                if j.misses >= 3:  # a single slow answer isn't enough to call a job lost
                    j.status, j.finished = "lost", time.time()
                    j.detail = "The node stopped responding before the job finished"
            return
        if node.status == "unknown":
            return  # not polled yet; try again next cycle
        for j in jobs:
            j.misses = 0
            if j.prompt_id in node.running_ids:
                j.status = "running"
            elif j.prompt_id in node.pending_ids:
                j.status = "queued"
            else:
                await self._resolve(node, j)

    async def _resolve(self, node, job: Job) -> None:
        """The job left the node's queue: its history entry says how it ended."""
        assert self.client is not None
        try:
            r = await self.client.get(f"{node.url}/history/{job.prompt_id}", timeout=5)
            entry = r.json().get(job.prompt_id) if r.status_code == 200 else None
        except Exception:
            return  # try again at the next check
        job.finished = time.time()
        if not entry:
            job.status = "unknown"
            job.detail = "No longer in the node's queue and no result found (cancelled, or the node restarted)"
            return
        status = entry.get("status") or {}
        job.run_seconds, error = _run_info(status.get("messages"))
        if status.get("status_str") == "error":
            job.status, job.detail = "failed", error or "Execution failed"
        else:
            job.status = "completed"

    # ---- view ---------------------------------------------------------
    def _name(self, node_id: str | None) -> str | None:
        node = self.registry.nodes.get(node_id) if node_id else None
        return (node.name or node.host) if node else node_id

    def view(self) -> list[dict]:
        return [{"seq": j.seq, "received": j.received, "client": j.client, "models": j.models,
                 "node": j.node, "node_name": self._name(j.node),
                 "tried": [self._name(t) for t in j.tried], "prompt_id": j.prompt_id,
                 "status": j.status, "detail": j.detail, "finished": j.finished,
                 "run_seconds": j.run_seconds}
                for j in reversed(self.jobs)]  # newest first
