"""FastAPI app: dashboard + cluster API under /cluster, ComfyUI-compatible API everywhere else."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from pathlib import Path

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel

from .config import settings
from .discovery import Scanner
from .models import norm
from .registry import registry
from .router import Router

class _QuietDashboard(logging.Filter):
    """Hide the dashboard's routine GET /cluster/api/... refreshes from the access log.
    Real traffic (POST /prompt, adding nodes, scans, ...) is still logged."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) >= 3:
            method, path = args[1], str(args[2])
            if method == "GET" and (path.startswith("/cluster/api/") or path.rstrip("/") == "/cluster"):
                return False
        return True


STATIC = Path(__file__).parent / "static"
scanner = Scanner(registry)
router = Router(registry)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    logging.getLogger("uvicorn.access").addFilter(_QuietDashboard())
    client = httpx.AsyncClient(limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
                               timeout=30)
    registry.client = router.client = scanner.client = router.monitor.client = client
    registry.load()
    # One check at startup (in the background so the service is usable immediately).
    tasks = [asyncio.create_task(registry.startup())]
    # Timers are opt-in (CLUSTER_POLL / CLUSTER_GOSSIP > 0); off by default.
    if settings.poll_interval > 0:
        tasks.append(asyncio.create_task(registry.poll_loop()))
    if settings.gossip_interval > 0:
        tasks.append(asyncio.create_task(registry.gossip_loop()))
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await client.aclose()


app = FastAPI(title="ComfyUI Cluster", lifespan=lifespan)


# ------------------------------------------------------------------ dashboard
@app.get("/cluster", include_in_schema=False)
@app.get("/cluster/", include_in_schema=False)
async def dashboard():
    return FileResponse(STATIC / "index.html")


# ------------------------------------------------------------------ cluster API
class NodeIn(BaseModel):
    address: str
    name: str = ""


class ScanIn(BaseModel):
    cidr: str | None = None


@app.get("/cluster/api/nodes")
async def list_nodes():
    nodes = sorted(registry.live(), key=lambda n: (n.host, n.port))
    return {"self": socket.gethostname(), "polled_at": registry.polled_at,
            "data_file": str(settings.data_file),
            "nodes": [n.view() for n in nodes]}


@app.post("/cluster/api/poll")
async def poll_now():
    """Manual check: sync node lists with peer managers, then poll every node."""
    await registry.gossip_once()
    await registry.refresh(models=True)   # a manual check also re-reads each node's model files
    await router.monitor.check()          # ...and updates the status of unfinished requests
    return await list_nodes()


@app.get("/cluster/api/requests")
async def list_requests():
    """The last few job submissions (updated in the background while jobs are unfinished)."""
    return {"tracking": settings.track_interval, "size": settings.monitor_size,
            "requests": router.monitor.view()}


class ModelRefreshIn(BaseModel):
    node: str | None = None   # a node id like "192.168.1.5:8188"; omit to refresh every node


@app.post("/cluster/api/models/refresh")
async def refresh_models(body: ModelRefreshIn | None = None):
    """Re-read the model files on one node or all nodes (use after installing models)."""
    node_id = body.node if body else None
    if node_id and (node_id not in registry.nodes or registry.nodes[node_id].removed):
        raise HTTPException(404, "Node not found")
    return {"results": await registry.refresh_models(node_id)}


@app.get("/cluster/api/models")
async def models_matrix():
    """Which model files each node has, for the dashboard's model table."""
    nodes = sorted(registry.live(), key=lambda n: (n.host, n.port))
    rows: dict[tuple[str, str], list[str]] = {}
    for n in nodes:
        for folder, files in (n.models or {}).items():
            for f in files:
                rows.setdefault((folder, norm(f)), []).append(n.id)
    return {"nodes": [{"id": n.id, "name": n.name or n.host, "known": n.models is not None,
                       "online": n.online} for n in nodes],
            "models": [{"folder": f, "name": name, "on": ids} for (f, name), ids in sorted(rows.items())]}


@app.post("/cluster/api/nodes")
async def add_node(body: NodeIn):
    try:
        node = registry.add(body.address, body.name)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    await registry.probe(node)
    return node.view()


@app.delete("/cluster/api/nodes/{node_id}")
async def remove_node(node_id: str):
    if not registry.remove(node_id):
        raise HTTPException(404, "Node not found")
    return {"ok": True}


@app.get("/cluster/api/gossip")
async def gossip():
    return {"nodes": registry.records()}


@app.post("/cluster/api/discover")
async def start_discovery(body: ScanIn | None = None):
    try:
        scanner.start(body.cidr if body else None)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return scanner.state


@app.get("/cluster/api/discover")
async def discovery_status():
    return scanner.state


# ------------------------------------------------------------------ ComfyUI-compatible API
@app.post("/prompt")
async def prompt(request: Request):
    return await router.submit(await request.body(), request.headers.get("content-type", "application/json"),
                               request.client.host if request.client else "")


@app.get("/history/{prompt_id}")
async def history(prompt_id: str):
    return await router.history(prompt_id)


@app.get("/queue")
async def queue():
    return await router.queue()


@app.get("/view")
async def view(request: Request):
    return await router.view(request.url.query)


@app.post("/upload/{kind}")
async def upload(kind: str, request: Request):
    return await router.upload(f"upload/{kind}", await request.body(),
                               request.headers.get("content-type", "application/octet-stream"))


@app.websocket("/ws")
async def ws_fan_in(ws: WebSocket):
    """Clients connect before their job is routed, so we relay events from every online node."""
    await ws.accept()
    query = ws.url.query
    lock = asyncio.Lock()

    async def pump(node):
        url = f"ws://{node.host}:{node.port}/ws" + (f"?{query}" if query else "")
        try:
            async with websockets.connect(url, max_size=None) as upstream:
                async for msg in upstream:
                    async with lock:
                        if isinstance(msg, bytes):
                            await ws.send_bytes(msg)
                        else:
                            await ws.send_text(msg)
        except Exception:
            pass

    if not registry.online():
        await registry.refresh(settings.request_poll_max_age, settings.request_poll_timeout)
    tasks = [asyncio.create_task(pump(n)) for n in registry.online()]
    try:
        while True:
            if (await ws.receive())["type"] == "websocket.disconnect":
                break
    except Exception:
        pass
    finally:
        for t in tasks:
            t.cancel()


# Everything else (/object_info, /system_stats, /interrupt, ...) goes to the least busy node.
# Must stay last so it never shadows the routes above.
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"], include_in_schema=False)
async def passthrough(path: str, request: Request):
    if path == "" and request.method == "GET":
        return RedirectResponse("/cluster")
    return await router.passthrough(request.method, path, request.url.query,
                                    await request.body(), dict(request.headers))
