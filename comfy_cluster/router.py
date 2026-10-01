"""Smart routing: picks the least busy node and relays ComfyUI API calls."""
from __future__ import annotations

import asyncio
from collections import OrderedDict

import httpx
from fastapi.responses import JSONResponse, Response

from .config import settings
from .monitor import Monitor
from .registry import Node, Registry

NODE_HEADER = "X-ComfyUI-Node"
_DROP_REQUEST_HEADERS = {"host", "content-length", "connection", "transfer-encoding",
                         "accept-encoding", "upgrade", "expect"}


def _wrap(r: httpx.Response, node: Node) -> Response:
    headers = {NODE_HEADER: node.id}
    if "content-type" in r.headers:
        headers["content-type"] = r.headers["content-type"]
    return Response(r.content, status_code=r.status_code, headers=headers)


def _no_nodes() -> JSONResponse:
    return JSONResponse({"error": {"type": "no_nodes_available",
                                   "message": "No online ComfyUI nodes in the cluster"}}, status_code=503)


def _error_text(r: httpx.Response) -> str:
    """Short human-readable reason from a ComfyUI error response."""
    try:
        err = r.json().get("error", {})
        text = err.get("message", "") if isinstance(err, dict) else str(err)
        details = err.get("details", "") if isinstance(err, dict) else ""
        return (f"{text}: {details}" if details else text)[:240] or f"Node returned HTTP {r.status_code}"
    except Exception:
        return f"Node returned HTTP {r.status_code}"


class Router:
    def __init__(self, registry: Registry) -> None:
        self.registry = registry
        self.client: httpx.AsyncClient | None = None
        self.monitor = Monitor(registry)
        self.owners: OrderedDict[str, str] = OrderedDict()  # prompt_id -> node id (fast path only)

    async def _fresh(self) -> None:
        """Called when a request needs a routing decision: poll now (bursts share one poll)."""
        await self.registry.refresh(settings.request_poll_max_age, settings.request_poll_timeout)

    async def _known_online(self) -> list[Node]:
        """For lookups (history, view, queue): use the last known state; poll only if nothing looks online."""
        if not self.registry.online():
            await self._fresh()
        return self.registry.online()

    def _remember(self, prompt_id: str, node_id: str) -> None:
        self.owners[prompt_id] = node_id
        while len(self.owners) > 5000:
            self.owners.popitem(last=False)

    # ---- POST /prompt ------------------------------------------------
    async def submit(self, body: bytes, content_type: str, client: str = "") -> Response:
        """Forward to the least busy capable node; fail over to the next one on errors."""
        assert self.client is not None
        first_error: tuple[httpx.Response, Node] | None = None
        await self._fresh()
        required = self.registry.required_for(body)
        candidates = self.registry.ranked(required)
        if required and not candidates:
            # Inventories may be stale (a model was just installed): re-read them once and retry.
            await self.registry.refresh(0, settings.request_poll_timeout, models=True)
            required = self.registry.required_for(body)
            candidates = self.registry.ranked(required)
        job = self.monitor.start(client, required)
        if not candidates:
            response, message = self._unroutable(required)
            self.monitor.rejected(job, message)
            return response
        for node in candidates:
            try:
                r = await self.client.post(f"{node.url}/prompt", content=body,
                                           headers={"content-type": content_type}, timeout=60)
            except httpx.HTTPError:
                self.registry.mark_offline(node)
                job.tried.append(node.id)
                continue
            if r.status_code == 200:
                node.inflight += 1  # keep ranking honest until the next poll
                try:
                    pid = r.json().get("prompt_id")
                except ValueError:
                    pid = None
                if pid:
                    self._remember(pid, node.id)
                self.monitor.accepted(job, node.id, pid)
                self.monitor.ensure_tracking()
                return _wrap(r, node)
            # 4xx/5xx: another node may accept it (different models/custom nodes installed)
            job.tried.append(node.id)
            if first_error is None:
                first_error = (r, node)
        if first_error:
            self.monitor.rejected(job, _error_text(first_error[0]))
            return _wrap(*first_error)
        self.monitor.rejected(job, "Every candidate node was unreachable")
        return _no_nodes()

    def _unroutable(self, required: set[str]) -> tuple[Response, str]:
        if not required:
            return _no_nodes(), "No online ComfyUI nodes in the cluster"
        offline = self.registry.capable_offline(required)
        if offline:
            message = "Nodes that have the required models are offline: " + ", ".join(offline)
        else:
            message = "No node in the cluster has all the models this workflow needs"
        return JSONResponse(
            {"error": {"type": "no_capable_nodes", "message": message,
                       "details": f"{len(required)} model file(s) referenced by the workflow"},
             "node_errors": {},
             "required_models": sorted(required)[:20],
             "missing_by_node": self.registry.missing_by_node(required),
             "capable_but_offline": offline},
            status_code=503 if offline else 400), message

    # ---- GET /history/{prompt_id} -----------------------------------
    async def history(self, prompt_id: str) -> Response:
        assert self.client is not None

        async def fetch(node: Node) -> tuple[Node, httpx.Response] | None:
            try:
                r = await self.client.get(f"{node.url}/history/{prompt_id}", timeout=5)
                if r.status_code == 200 and r.json():
                    return node, r
            except Exception:
                pass
            return None

        online = await self._known_online()
        hint = self.owners.get(prompt_id)
        ordered = sorted(online, key=lambda n: n.id != hint)
        if ordered and ordered[0].id == hint:
            hit = await fetch(ordered[0])
            if hit:
                return _wrap(hit[1], hit[0])
            ordered = ordered[1:]
        for hit in await asyncio.gather(*(fetch(n) for n in ordered)):
            if hit:
                return _wrap(hit[1], hit[0])
        return JSONResponse({})  # same shape ComfyUI returns for an unknown id

    # ---- GET /queue (aggregated) ------------------------------------
    async def queue(self) -> Response:
        assert self.client is not None

        async def fetch(node: Node) -> dict:
            try:
                r = await self.client.get(f"{node.url}/queue", timeout=3)
                return r.json()
            except Exception:
                return {}

        merged: dict[str, list] = {"queue_running": [], "queue_pending": []}
        for q in await asyncio.gather(*(fetch(n) for n in await self._known_online())):
            merged["queue_running"] += q.get("queue_running", [])
            merged["queue_pending"] += q.get("queue_pending", [])
        return JSONResponse(merged)

    # ---- GET /view: find whichever node holds the output ------------
    async def view(self, query: str) -> Response:
        assert self.client is not None
        for node in await self._known_online():
            try:
                r = await self.client.get(f"{node.url}/view" + (f"?{query}" if query else ""), timeout=30)
            except httpx.HTTPError:
                continue
            if r.status_code == 200:
                return _wrap(r, node)
        return JSONResponse({"error": "file not found on any online node"}, status_code=404)

    # ---- POST /upload/*: broadcast so whichever node runs the job has the file
    async def upload(self, path: str, body: bytes, content_type: str) -> Response:
        assert self.client is not None
        await self._fresh()
        nodes = self.registry.online()
        if not nodes:
            return _no_nodes()

        async def send(node: Node) -> tuple[Node, httpx.Response] | None:
            try:
                return node, await self.client.post(f"{node.url}/{path}", content=body,
                                                    headers={"content-type": content_type}, timeout=120)
            except httpx.HTTPError:
                return None

        results = [r for r in await asyncio.gather(*(send(n) for n in nodes)) if r]
        for node, r in results:
            if r.status_code == 200:
                return _wrap(r, node)
        return _wrap(results[0][1], results[0][0]) if results else _no_nodes()

    # ---- everything else: relay to the least busy node --------------
    async def passthrough(self, method: str, path: str, query: str, body: bytes, headers: dict) -> Response:
        assert self.client is not None
        await self._fresh()
        ranked = self.registry.ranked()
        if not ranked:
            return _no_nodes()
        node = ranked[0]
        fwd = {k: v for k, v in headers.items() if k.lower() not in _DROP_REQUEST_HEADERS}
        url = f"{node.url}/{path}" + (f"?{query}" if query else "")
        try:
            r = await self.client.request(method, url, content=body, headers=fwd, timeout=60)
        except httpx.HTTPError:
            self.registry.mark_offline(node)
            return _no_nodes()
        return _wrap(r, node)
