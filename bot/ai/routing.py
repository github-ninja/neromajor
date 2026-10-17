"""
Gemini is reached through DNS-based routing: its hostname resolves to several
exit nodes, and some of them sit where Google refuses service ("User location
is not supported"). Every request opens a fresh connection to a random node,
and a node caught refusing is skipped for a while, so a retry lands elsewhere
instead of reusing the kept-alive connection to the same bad node.
"""

import asyncio
import contextvars
import logging
import random
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult

logger = logging.getLogger(__name__)

REFUSING_NODE_PAUSE = 60 * 60

# Nodes the current request connected to; set per attempt by track().
_used: contextvars.ContextVar[list[str]] = contextvars.ContextVar("gemini_nodes")


class NodeResolver(AbstractResolver):
    def __init__(self) -> None:
        self._refusing: dict[str, float] = {}  # node -> skip until (monotonic)

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        infos = await asyncio.get_running_loop().getaddrinfo(
            host, port, family=family, type=socket.SOCK_STREAM
        )
        nodes = list(dict.fromkeys(info[4][0] for info in infos))
        now = time.monotonic()
        node = random.choice([n for n in nodes if self._refusing.get(n, 0.0) <= now] or nodes)
        used = _used.get(None)
        if used is not None:
            used.append(node)
        return [{"hostname": host, "host": node, "port": port, "family": family,
                 "proto": 0, "flags": socket.AI_NUMERICHOST}]

    async def close(self) -> None:
        pass

    def refused(self, nodes: list[str]) -> None:
        for node in nodes:
            logger.warning("Gemini: узел %s отказывает по региону, пропускаю его %d мин.",
                           node, REFUSING_NODE_PAUSE // 60)
            self._refusing[node] = time.monotonic() + REFUSING_NODE_PAUSE


@contextmanager
def track() -> Iterator[list[str]]:
    """Collects the nodes that requests made inside the block connected to."""
    used: list[str] = []
    token = _used.set(used)
    try:
        yield used
    finally:
        _used.reset(token)


def session(resolver: NodeResolver) -> aiohttp.ClientSession:
    """Needs a running event loop."""
    connector = aiohttp.TCPConnector(
        resolver=resolver, use_dns_cache=False, force_close=True, limit=0
    )
    return aiohttp.ClientSession(connector=connector, trust_env=True)
