# ComfyUI cluster manager

A small service you run on **every** machine that hosts ComfyUI. Each copy is a full peer:
it keeps its own node list, checks node health on demand, serves a dashboard, and routes
ComfyUI API calls to the least busy machine. No coordinator, no shared database.

```
 your app ──POST /prompt──► manager (any machine, :8189)
                               │  checks GET /queue on every node when a request arrives
                               │  picks lowest (running + pending + in-flight)
                               ▼
                     ComfyUI node A / B / C  (:8188)

 manager A ◄──gossip (30 s)──► manager B ◄──► manager C     (node lists converge)
```

## Run

```bash
pip install -r requirements.txt     # Python 3.10+
python run.py                       # dashboard: http://localhost:8189/cluster
```

On each machine, add one node (or click **Scan network**). Managers share their node
lists with each other, so the rest fills in automatically.

Point your apps at `http://<any-machine>:8189` instead of `:8188`. Nothing else changes.

Settings (environment variables): `CLUSTER_PORT` (8189), `CLUSTER_HOST` (0.0.0.0),
`CLUSTER_DATA` (`~/.comfy_cluster/nodes.json`), `COMFY_PORT` (8188).
Request monitor: `CLUSTER_TRACK` (2, seconds; 0 = off), `CLUSTER_MONITOR_SIZE` (10).
Optional background timers, both off by default: `CLUSTER_POLL` and `CLUSTER_GOSSIP` (seconds, 0 = off).

### Windows launcher

`start_cluster.bat` (keep it next to `run.py`) starts the app only if it isn't already running: it checks the port, activates `.venv` (or `venv`), and opens the app in its own window. Add `open` to also open the dashboard, and `silent` to skip the final pause (for Task Scheduler or the Startup folder): `start_cluster.bat open silent`.

## How it behaves

| Concern | Behaviour |
|---|---|
| When nodes are checked | No constant background polling. Nodes are checked (1) once at startup, (2) when a request needs a routing decision (`/prompt`, uploads, other passthrough calls), (3) when you press **Check nodes now** in the dashboard (which also syncs node lists with peers), and (4) every 2 s **while any request is unfinished**, stopping automatically when the last one ends (see Request monitor). Requests arriving within 1 s of a check reuse it, so a burst causes one round of checks. Lookups such as `/history` and `/view` use the last known state and only check if nothing looks online. |
| Fault tolerance | Any manager can serve any request. A dead node is detected at the next check and skipped; if a forward fails mid-request the next best node is tried. |
| Routing | Least `running + pending + in-flight` wins; ties are broken randomly. In-flight counts jobs forwarded since the last check, so bursts don't pile onto one node. |
| Model awareness | Each node's model files are read from ComfyUI's `/models` endpoints: on first contact, when a node comes back online, and on **Check nodes now** (not on every request). A workflow "requires" any model file its inputs name that the cluster knows about, and is only sent to nodes that have all of them. Nodes whose list couldn't be read are used only after nodes known to be capable. If nothing qualifies, inventories are re-read once in case a model was just installed, then you get a clear error: `400 no_capable_nodes` (with `missing_by_node`), or `503` if a capable node is offline (with `capable_but_offline`). Windows and Linux path separators are treated as equal. |
| Request monitor | The dashboard lists the last 10 `POST /prompt` requests (kept in memory, cleared on restart): when it arrived, which node got it, and its status: queued, running, completed (with run time when the node reports it), failed (with the node's error), rejected (no node accepted it, with the reason), lost (the node stopped responding), or unknown (left the node's queue with no result, e.g. cancelled or the node restarted). While at least one request is unfinished, the service re-checks every node and request every 2 s (whether or not the dashboard is open) so the node cards and request list update live; when all have finished it stops and the cluster goes quiet again. Set `CLUSTER_TRACK` to change the interval in seconds, or `0` to turn it off (Check nodes now still updates). `CLUSTER_MONITOR_SIZE` changes how many requests are kept. |
| Failover on rejection | If a node rejects a workflow (e.g. missing model), the next node is tried before an error is returned. |
| Refreshing models | After installing models on a machine, press **Refresh model lists** (all nodes) or **Refresh models** on that node's row. It re-reads the files and reports what changed (e.g. `B: +2 files`). **Check nodes now** does the same for every node. |
| Persistence | `nodes.json`, written atomically. Removals are stored as tombstones (kept 7 days) so they propagate to peers instead of being re-added. |
| Sync | Last-write-wins merge of node lists between managers, using the same port on every machine. Runs at startup and on **Check nodes now**. |
| Discovery | Probes port 8188 across the local /24 (or a CIDR you enter, up to 4096 addresses) and confirms `/system_stats` looks like ComfyUI. |

## API

Cluster API: `GET/POST /cluster/api/nodes`, `DELETE /cluster/api/nodes/{host:port}`,
`GET/POST /cluster/api/discover`, `POST /cluster/api/poll`, `GET /cluster/api/models`, `GET /cluster/api/requests`, `GET /cluster/api/gossip`.

ComfyUI-compatible: `POST /prompt` (routed; response carries an `X-ComfyUI-Node` header),
`GET /history/{id}` (finds the owning node), `GET /queue` (merged across nodes),
`GET /view` (searches nodes), `POST /upload/*` (sent to all online nodes),
`WS /ws` (events from all online nodes), and everything else goes to the least busy node.

## Limitations to know about

- **Model matching is by file name.** Requirements are detected by comparing workflow values to file names in each node's model folders, without checking which folder or loader node uses them. A model that no node has is not detected as a requirement; the node that receives the workflow rejects it as usual. Requires a ComfyUI version with the `/models` API; otherwise that node is treated as "unknown" and used last for workflows that need models. Custom-node requirements aren't tracked.
- **WebSocket** clients connect before a job is routed, so `/ws` relays events from every online node that was up at connect time. Clients that pass `client_id` and filter on `prompt_id` (most do) work unchanged.
- **Uploads** are copied to every online node so the chosen node has the input image.
- **No authentication.** Anyone on the LAN can use the dashboard and API. Keep it on a trusted network, or bind to `127.0.0.1` with `CLUSTER_HOST` and put it behind a reverse proxy.
