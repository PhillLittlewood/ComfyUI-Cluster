"""Subnet scanner: finds ComfyUI instances by probing port 8188 and verifying /system_stats."""
from __future__ import annotations

import asyncio
import ipaddress
import socket
import time

import httpx

from .config import settings
from .registry import Registry


def local_subnets() -> list[ipaddress.IPv4Network]:
    """Best-effort: assume a /24 around each non-loopback IPv4 address of this machine."""
    ips: set[str] = set()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))  # no packet is sent; just selects the outbound interface
        ips.add(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    try:
        ips.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        pass
    return [ipaddress.ip_network(f"{ip}/24", strict=False) for ip in sorted(ips) if not ip.startswith("127.")]


class Scanner:
    def __init__(self, registry: Registry) -> None:
        self.registry = registry
        self.client: httpx.AsyncClient | None = None
        self.task: asyncio.Task | None = None
        self.state: dict = self._fresh()

    @staticmethod
    def _fresh() -> dict:
        return {"running": False, "total": 0, "done": 0, "found": [], "subnets": [],
                "started": None, "finished": None, "error": ""}

    def start(self, cidr: str | None = None) -> None:
        if self.task and not self.task.done():
            return
        if cidr:
            nets = [ipaddress.ip_network(cidr.strip(), strict=False)]
            if nets[0].version != 4:
                raise ValueError("Only IPv4 subnets are supported")
            if nets[0].num_addresses > settings.scan_max_hosts:
                raise ValueError(f"Subnet too large (max {settings.scan_max_hosts} addresses, e.g. /20)")
        else:
            nets = local_subnets()
            if not nets:
                raise ValueError("Could not detect a local subnet; enter one in CIDR form (e.g. 192.168.1.0/24)")
        self.state = self._fresh()
        self.task = asyncio.create_task(self._run(nets))

    async def _run(self, nets: list[ipaddress.IPv4Network]) -> None:
        hosts = [str(h) for net in nets for h in net.hosts()]
        if "127.0.0.1" not in hosts:
            hosts.append("127.0.0.1")
        st = self.state
        st.update(running=True, total=len(hosts), subnets=[str(n) for n in nets], started=time.time())
        sem = asyncio.Semaphore(256)

        async def check(ip: str) -> None:
            async with sem:
                try:
                    if await self._port_open(ip) and await self._is_comfy(ip):
                        node = self.registry.add(f"{ip}:{settings.comfy_port}", source="discovered")
                        await self.registry.probe(node)  # give it a status straight away
                        st["found"].append(node.id)
                except Exception:
                    pass
                finally:
                    st["done"] += 1

        try:
            await asyncio.gather(*(check(ip) for ip in hosts))
        except Exception as exc:
            st["error"] = str(exc)
        finally:
            st.update(running=False, finished=time.time())

    @staticmethod
    async def _port_open(ip: str) -> bool:
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection(ip, settings.comfy_port), 0.6)
            writer.close()
            return True
        except (OSError, asyncio.TimeoutError):
            return False

    async def _is_comfy(self, ip: str) -> bool:
        assert self.client is not None
        try:
            r = await self.client.get(f"http://{ip}:{settings.comfy_port}/system_stats", timeout=2)
            data = r.json()
            return r.status_code == 200 and "system" in data and "devices" in data
        except Exception:
            return False
