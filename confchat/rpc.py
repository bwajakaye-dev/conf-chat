"""Minimal asynchronous JSON-over-TCP RPC used by every Conf-Chat peer.

Wire format: one JSON object per line.
    request : {"method": "<name>", "params": {...}}
    response: {"result": <any>}  or  {"error": "<message>"}

Every peer is both an RPC server and an RPC client - there is no central server.
Client connections are pooled and reused, so the periodic Chord maintenance
traffic does not exhaust the operating system's ephemeral ports.
"""
from __future__ import annotations

import asyncio
import json
import logging
import weakref

log = logging.getLogger("confchat.rpc")

# Large enough for bulk key transfers between nodes.
STREAM_LIMIT = 16 * 1024 * 1024
DEFAULT_TIMEOUT = 3.0
MAX_IDLE_PER_PEER = 4


class NodeUnreachable(Exception):
    """The remote peer could not be contacted (down, refused, or timed out)."""


class RPCError(Exception):
    """The remote peer was reached but its handler raised an error."""


def _split(addr: str) -> tuple[str, int]:
    host, port = addr.rsplit(":", 1)
    return host, int(port)


# event loop -> {addr: [idle (reader, writer), ...]}
_pools: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict]" = weakref.WeakKeyDictionary()


def _pool() -> dict:
    loop = asyncio.get_running_loop()
    pool = _pools.get(loop)
    if pool is None:
        pool = _pools[loop] = {}
    return pool


def _close(writer) -> None:
    try:
        writer.close()
    except Exception:  # noqa: BLE001
        pass


def drop_connections(addr: str | None = None) -> None:
    """Close pooled client connections (to one peer, or to all)."""
    pool = _pool()
    for a in [addr] if addr else list(pool):
        for _, w in pool.pop(a, []):
            _close(w)


async def _exchange(reader, writer, payload: bytes, timeout: float) -> bytes:
    writer.write(payload)
    await writer.drain()
    return await asyncio.wait_for(reader.readline(), timeout)


async def call(addr: str, method: str, timeout: float = DEFAULT_TIMEOUT, **params):
    """Send one RPC request to ``addr`` ("host:port") and return its result."""
    payload = json.dumps({"method": method, "params": params}).encode() + b"\n"
    idle = _pool().setdefault(addr, [])
    line = b""
    # first try a pooled connection (it may have gone stale), then a fresh one
    while idle:
        reader, writer = idle.pop()
        try:
            line = await _exchange(reader, writer, payload, timeout)
        except asyncio.TimeoutError as exc:
            _close(writer)
            raise NodeUnreachable(addr) from exc
        except (OSError, asyncio.IncompleteReadError, ValueError):
            line = b""
        if line:
            break
        _close(writer)
    if not line:
        host, port = _split(addr)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, limit=STREAM_LIMIT), timeout)
            line = await _exchange(reader, writer, payload, timeout)
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError) as exc:
            if "writer" in locals():
                _close(writer)
            raise NodeUnreachable(addr) from exc
        if not line:
            _close(writer)
            raise NodeUnreachable(addr)
    if len(idle) < MAX_IDLE_PER_PEER:
        idle.append((reader, writer))
    else:
        _close(writer)
    resp = json.loads(line)
    if "error" in resp:
        raise RPCError(resp["error"])
    return resp.get("result")


class RPCServer:
    """Dispatches incoming JSON requests to registered coroutine handlers."""

    def __init__(self) -> None:
        self.handlers: dict = {}
        self._server: asyncio.base_events.Server | None = None
        self._writers: set = set()

    def register(self, name: str, handler) -> None:
        self.handlers[name] = handler

    async def start(self, bind: str, port: int) -> int:
        self._server = await asyncio.start_server(self._handle, bind, port, limit=STREAM_LIMIT)
        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            server, self._server = self._server, None
            server.close()
            for w in list(self._writers):
                _close(w)
            try:
                await asyncio.wait_for(server.wait_closed(), 0.2)
            except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                pass

    async def dispatch(self, method: str, params: dict):
        handler = self.handlers.get(method)
        if handler is None:
            raise RPCError(f"unknown method {method!r}")
        return await handler(**params)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._server is None:          # connection raced with shutdown
            _close(writer)
            return
        self._writers.add(writer)
        try:
            while self._server is not None:
                line = await reader.readline()
                if not line:
                    break
                try:
                    req = json.loads(line)
                    result = await self.dispatch(req["method"], req.get("params") or {})
                    resp = {"result": result}
                except Exception as exc:  # noqa: BLE001 - report any handler error to caller
                    log.debug("handler error: %r", exc)
                    resp = {"error": f"{type(exc).__name__}: {exc}"}
                writer.write(json.dumps(resp).encode() + b"\n")
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            self._writers.discard(writer)
            _close(writer)
