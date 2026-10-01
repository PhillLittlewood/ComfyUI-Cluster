"""Runtime settings. Everything can be overridden with environment variables."""
import os
from dataclasses import dataclass, field
from pathlib import Path


def _default_data_file() -> Path:
    return Path(os.getenv("CLUSTER_DATA", Path.home() / ".comfy_cluster" / "nodes.json"))


@dataclass
class Settings:
    host: str = os.getenv("CLUSTER_HOST", "0.0.0.0")
    port: int = int(os.getenv("CLUSTER_PORT", "8189"))      # this manager's port (same on every machine)
    comfy_port: int = int(os.getenv("COMFY_PORT", "8188"))  # default ComfyUI port
    data_file: Path = field(default_factory=_default_data_file)
    # Nodes are checked on startup, when a request arrives, and on demand from the dashboard.
    # Set these >0 (seconds) only if you also want a background timer.
    poll_interval: float = float(os.getenv("CLUSTER_POLL", "0"))
    gossip_interval: float = float(os.getenv("CLUSTER_GOSSIP", "0"))
    request_poll_max_age: float = float(os.getenv("CLUSTER_REQ_POLL_AGE", "1"))  # reuse a poll newer than this
    request_poll_timeout: float = float(os.getenv("CLUSTER_REQ_POLL_TIMEOUT", "1.5"))
    # Request monitor: how many recent jobs to keep (in memory only), and how often (seconds) all nodes and
    # unfinished jobs are re-checked WHILE any job is unfinished. Polling stops when the last job ends.
    # 0 = never automatic (use "Check nodes now").
    monitor_size: int = int(os.getenv("CLUSTER_MONITOR_SIZE", "10"))
    track_interval: float = float(os.getenv("CLUSTER_TRACK", "2"))
    tombstone_ttl: float = 7 * 86400                                   # how long "removed" markers are kept
    scan_max_hosts: int = 4096                                         # largest subnet a scan may cover


settings = Settings()
