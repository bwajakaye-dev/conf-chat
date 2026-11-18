"""A Chord distributed hash table (Stoica et al., SIGCOMM 2001).

Each Conf-Chat peer runs one ChordNode. Together the nodes form a ring on a
160-bit identifier space (SHA-1). A key is stored on its *successor*: the
first node whose id is equal to or follows hash(key) clockwise on the ring.

Implemented features:
  * consistent hashing of node addresses and keys (SHA-1, m = 160)
  * O(log N) lookups through finger tables (iterative routing)
  * join through any existing node, periodic stabilize / notify / fix_fingers
  * successor lists (r entries) to survive node failures
  * replication of every key onto the r successors of its owner
  * key hand-off when a node joins, graceful leave, and crash recovery
  * atomic storage operations executed on the owning node, so many peers
    can safely write to the same key (for example an offline inbox)

Nodes can join or leave at any time; no reconfiguration is needed.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass

from .rpc import NodeUnreachable, RPCError, RPCServer, call

log = logging.getLogger("confchat.chord")

M = 160                 # bits in the identifier space
RING = 2 ** M
DEAD_TTL = 5.0          # seconds to avoid a node after it fails to answer


def hash_key(text: str) -> int:
    return int.from_bytes(hashlib.sha1(text.encode()).digest(), "big")


def between(x: int, a: int, b: int) -> bool:
    """x in the open ring interval (a, b)."""
    if a < b:
        return a < x < b
    return x > a or x < b          # wraps around 0 (a == b -> all but a)


def between_right(x: int, a: int, b: int) -> bool:
    """x in the half-open ring interval (a, b]."""
    if a < b:
        return a < x <= b
    return x > a or x <= b         # wraps around 0 (a == b -> whole ring)


@dataclass(frozen=True)
class NodeRef:
    id: int
    addr: str

    def to_dict(self) -> dict:
        return {"id": self.id, "addr": self.addr}

    @staticmethod
    def from_dict(d) -> "NodeRef | None":
        return NodeRef(int(d["id"]), d["addr"]) if d else None

    @property
    def short(self) -> str:
        return f"{self.id:040x}"[:8]

    def __str__(self) -> str:
        return f"{self.short}@{self.addr}"


class DHTError(Exception):
    """A DHT operation could not complete (e.g. the ring is still repairing)."""


WRITE_OPS = {"put", "put_if_absent", "delete", "sadd", "srem"}


class ChordNode:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        bind: str | None = None,
        replicas: int = 3,
        interval: float = 1.0,
        rpc_timeout: float = 3.0,
    ) -> None:
        self.host = host
        self.port = port
        self.bind = bind or host
        self.r = replicas
        self.interval = interval
        self.rpc_timeout = rpc_timeout

        self.ref: NodeRef | None = None
        self.successors: list[NodeRef] = []
        self.predecessor: NodeRef | None = None
        self.fingers: list[NodeRef | None] = [None] * M
        # key -> {"h": hash, "data": value-or-None, "ver": int}
        self.store: dict[str, dict] = {}
        self.dead: dict[str, float] = {}

        self.rpc = RPCServer()
        self._tasks: list[asyncio.Task] = []
        self.running = False
        for name in (
            "ping", "step", "find_successor", "get_predecessor", "get_successors",
            "notify", "dht_op", "replica_put", "pred_leaving", "succ_leaving", "info",
        ):
            self.rpc.register(name, getattr(self, f"_h_{name}"))

    # ------------------------------------------------------------------ setup
    @property
    def addr(self) -> str:
        return self.ref.addr

    def register_handler(self, name: str, handler) -> None:
        """Let the application layer (chat) expose its own RPC methods."""
        self.rpc.register(name, handler)

    async def start(self, bootstrap: str | None = None) -> None:
        """Start listening; create a new ring, or join one through ``bootstrap``."""
        self.port = await self.rpc.start(self.bind, self.port)
        addr = f"{self.host}:{self.port}"
        self.ref = NodeRef(hash_key(addr), addr)
        self.successors = [self.ref]
        self.running = True
        if bootstrap:
            try:
                d = await call(bootstrap, "find_successor", timeout=10, id=self.ref.id)
            except (NodeUnreachable, RPCError):
                await self.rpc.stop()
                raise
            succ = NodeRef.from_dict(d)
            if succ.id == self.ref.id:
                await self.rpc.stop()
                raise DHTError("node id collision; choose another port")
            self.successors = [succ]
            await self._stabilize()          # announce ourselves immediately
            log.info("joined ring via %s; successor %s", bootstrap, succ)
        else:
            log.info("created new ring as %s", self.ref)
        self._tasks.append(asyncio.create_task(self._maintenance()))

    async def stop(self) -> None:
        """Stop abruptly (simulates a crash: no hand-off)."""
        self.running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks.clear()
        await self.rpc.stop()

    async def leave(self) -> None:
        """Leave gracefully: hand our keys to our successor and relink neighbours."""
        self.running = False
        succ = self.successors[0] if self.successors else self.ref
        pred = self.predecessor
        if succ != self.ref:
            try:
                # versions make this safe: the successor keeps whichever copy is newer
                if self.store:
                    await call(succ.addr, "replica_put", timeout=self.rpc_timeout,
                               entries=self.store)
                await call(succ.addr, "pred_leaving", timeout=self.rpc_timeout,
                           node=self.ref.to_dict(), new_pred=pred.to_dict() if pred else None)
            except (NodeUnreachable, RPCError):
                pass
            if pred and pred != self.ref:
                try:
                    await call(pred.addr, "succ_leaving", timeout=self.rpc_timeout,
                               node=self.ref.to_dict(),
                               successors=[s.to_dict() for s in self.successors])
                except (NodeUnreachable, RPCError):
                    pass
        await self.stop()

    # ---------------------------------------------------------------- helpers
    def _alive(self, n: NodeRef) -> bool:
        t = self.dead.get(n.addr)
        return t is None or time.monotonic() - t > DEAD_TTL

    def _mark_dead(self, n: NodeRef) -> None:
        if n == self.ref:
            return
        log.info("node %s is unreachable", n)
        self.dead[n.addr] = time.monotonic()
        self.successors = [s for s in self.successors if s.addr != n.addr]
        self.fingers = [None if (f and f.addr == n.addr) else f for f in self.fingers]
        if self.predecessor and self.predecessor.addr == n.addr:
            self.predecessor = None
        if not self.successors:
            alive = [f for f in self.fingers if f and f != self.ref]
            self.successors = alive[:1] or [self.ref]

    async def _call(self, n: NodeRef, method: str, **params):
        if n == self.ref:  # local short-cut; JSON round trip avoids aliasing the store
            res = await self.rpc.dispatch(method, json.loads(json.dumps(params)))
            return json.loads(json.dumps(res))
        try:
            return await call(n.addr, method, timeout=self.rpc_timeout, **params)
        except NodeUnreachable:
            self._mark_dead(n)
            raise

    def _owns(self, h: int) -> bool:
        p = self.predecessor
        if p is None:
            # Without a predecessor we cannot know our range. Only a lone node
            # owns everything; otherwise wait (callers retry) until notify().
            return self.successors[0] == self.ref
        return between_right(h, p.id, self.ref.id)

    # ----------------------------------------------------------------- lookup
    def _closest_preceding(self, kid: int) -> NodeRef:
        best, best_d = self.ref, -1
        for n in self.fingers + self.successors:
            if n is None or n == self.ref or not self._alive(n):
                continue
            if between(n.id, self.ref.id, kid):
                d = (n.id - self.ref.id) % RING
                if d > best_d:
                    best, best_d = n, d
        return best

    def _step(self, kid: int) -> tuple[bool, NodeRef]:
        """One routing decision: (True, owner) or (False, next hop)."""
        succ = self.successors[0]
        if between_right(kid, self.ref.id, succ.id):
            return True, succ
        nxt = self._closest_preceding(kid)
        if nxt == self.ref:
            return True, succ
        return False, nxt

    async def find_successor(self, kid: int) -> NodeRef:
        """Iterative Chord lookup: the node responsible for identifier ``kid``."""
        for _ in range(4):
            done, n = self._step(kid)
            try:
                for _hop in range(64):
                    if done:
                        return n
                    r = await self._call(n, "step", id=kid)
                    done, n = r["done"], NodeRef.from_dict(r["node"])
            except (NodeUnreachable, RPCError):
                await asyncio.sleep(self.interval / 2)
        raise DHTError(f"lookup for {kid:040x} failed")

    # ------------------------------------------------------- DHT client API
    async def dht(self, key: str, op: str, **args):
        """Run a storage operation on the node that owns ``key``.

        Operations: get, put(value), put_if_absent(value), delete,
                    sadd(member, value), srem(member), smembers.
        """
        h = hash_key(key)
        for _ in range(12):
            try:
                owner = await self.find_successor(h)
                r = await self._call(owner, "dht_op", key=key, op=op, args=args)
            except (NodeUnreachable, DHTError):
                await asyncio.sleep(self.interval / 2)
                continue
            if r.get("redirect"):          # ring changed under us; look up again
                await asyncio.sleep(self.interval / 2)
                continue
            return r["result"]
        raise DHTError(f"could not complete {op} on {key!r}; the ring may be repairing")

    # -------------------------------------------------------------- storage
    def _apply(self, key: str, h: int, op: str, args: dict):
        e = self.store.get(key)
        data = e["data"] if e else None
        if op == "get":
            return data
        if op == "smembers":
            return data or {}
        ret = True
        if op == "put":
            new = args["value"]
        elif op == "put_if_absent":
            if data is not None:
                return False
            new = args["value"]
        elif op == "delete":
            new = None
        elif op == "sadd":
            new = dict(data or {})
            new[args["member"]] = args.get("value", True)
        elif op == "srem":
            new = dict(data or {})
            ret = new.pop(args["member"], None) is not None
            new = new or None
        else:
            raise ValueError(f"unknown op {op!r}")
        self.store[key] = {"h": h, "data": new, "ver": (e["ver"] if e else 0) + 1}
        return ret

    async def _push(self, n: NodeRef, entries: dict) -> None:
        try:
            await self._call(n, "replica_put", entries=entries)
        except (NodeUnreachable, RPCError):
            pass

    def _replicate(self, entries: dict) -> None:
        for s in self.successors[: self.r]:
            if s != self.ref:
                asyncio.create_task(self._push(s, entries))

    # ------------------------------------------------------------ maintenance
    async def _maintenance(self) -> None:
        tick = 0
        while self.running:
            for job, every in ((self._stabilize, 1), (self._check_predecessor, 1),
                               (self._fix_fingers, 2), (self._replicate_owned, 3)):
                if tick % every == 0:
                    try:
                        await job()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - keep the node alive
                        log.debug("%s failed: %r", job.__name__, exc)
            tick += 1
            await asyncio.sleep(self.interval)

    async def _stabilize(self) -> None:
        for _ in range(8):
            succ = self.successors[0]
            if succ == self.ref:
                x, slist = self.predecessor, []
            else:
                try:
                    x = NodeRef.from_dict(await self._call(succ, "get_predecessor"))
                    slist = [NodeRef.from_dict(d) for d in await self._call(succ, "get_successors")]
                except (NodeUnreachable, RPCError):
                    continue                     # successor removed; try the next one
            if x and x != self.ref and self._alive(x) and between(x.id, self.ref.id, succ.id):
                self.successors = [x] + self.successors   # a closer successor joined
                continue
            new = [succ]
            for s in slist:
                if s != self.ref and s not in new:
                    new.append(s)
            self.successors = new[: self.r]
            if succ != self.ref:
                try:
                    await self._call(succ, "notify", node=self.ref.to_dict())
                except (NodeUnreachable, RPCError):
                    pass
            return

    async def _check_predecessor(self) -> None:
        p = self.predecessor
        if p and p != self.ref:
            try:
                await self._call(p, "ping")
            except (NodeUnreachable, RPCError):
                pass                              # _call already cleared it

    async def _fix_fingers(self) -> None:
        i = 0
        while i < M:
            f = await self.find_successor((self.ref.id + 2 ** i) % RING)
            self.fingers[i] = f
            i += 1
            # every following finger whose start lies in (self, f] is also f
            while i < M and between_right((self.ref.id + 2 ** i) % RING, self.ref.id, f.id):
                self.fingers[i] = f
                i += 1

    async def _replicate_owned(self) -> None:
        owned = {k: e for k, e in self.store.items() if self._owns(e["h"])}
        if owned:
            self._replicate(owned)

    # ----------------------------------------------------------- RPC handlers
    async def _h_ping(self):
        return True

    async def _h_step(self, id):
        done, n = self._step(int(id))
        return {"done": done, "node": n.to_dict()}

    async def _h_find_successor(self, id):
        return (await self.find_successor(int(id))).to_dict()

    async def _h_get_predecessor(self):
        return self.predecessor.to_dict() if self.predecessor else None

    async def _h_get_successors(self):
        return [s.to_dict() for s in self.successors]

    async def _h_notify(self, node):
        n = NodeRef.from_dict(node)
        if n == self.ref:
            return False
        p = self.predecessor
        if p is None or not self._alive(p) or between(n.id, p.id, self.ref.id):
            self.predecessor = n
            self.dead.pop(n.addr, None)
            # hand over the keys the new predecessor is now responsible for
            # (we keep copies: we are its successor, hence one of its replicas)
            moved = {k: e for k, e in self.store.items()
                     if not between_right(e["h"], n.id, self.ref.id)}
            if moved:
                asyncio.create_task(self._push(n, moved))
            log.info("new predecessor %s", n)
        return True

    async def _h_dht_op(self, key, op, args):
        h = hash_key(key)
        if not self._owns(h):
            return {"redirect": True}
        result = self._apply(key, h, op, args or {})
        if op in WRITE_OPS:
            self._replicate({key: self.store[key]})
        return {"result": result}

    async def _h_replica_put(self, entries):
        for k, e in entries.items():
            cur = self.store.get(k)
            if cur is None or e["ver"] >= cur["ver"]:
                self.store[k] = {"h": int(e["h"]), "data": e["data"], "ver": e["ver"]}
        return True

    async def _h_pred_leaving(self, node, new_pred):
        n = NodeRef.from_dict(node)
        if self.predecessor == n:
            np = NodeRef.from_dict(new_pred)
            self.predecessor = np if np != self.ref else None
        self.dead[n.addr] = time.monotonic()
        self.fingers = [None if f == n else f for f in self.fingers]
        return True

    async def _h_succ_leaving(self, node, successors):
        n = NodeRef.from_dict(node)
        self.dead[n.addr] = time.monotonic()
        rest = [NodeRef.from_dict(d) for d in successors]
        if self.successors and self.successors[0] == n:
            self.successors = [s for s in rest if s != n and s != self.ref] or [self.ref]
        else:
            self.successors = [s for s in self.successors if s != n] or [self.ref]
        self.fingers = [None if f == n else f for f in self.fingers]
        return True

    async def _h_info(self):
        return self.info()

    # ------------------------------------------------------------ inspection
    def info(self) -> dict:
        owned = sum(1 for e in self.store.values() if self._owns(e["h"]) and e["data"] is not None)
        total = sum(1 for e in self.store.values() if e["data"] is not None)
        distinct = []
        for f in self.fingers:
            if f and f not in distinct:
                distinct.append(f)
        return {
            "node": self.ref.to_dict(),
            "predecessor": self.predecessor.to_dict() if self.predecessor else None,
            "successors": [s.to_dict() for s in self.successors],
            "fingers": [f.to_dict() for f in distinct],
            "keys_owned": owned,
            "keys_replica": total - owned,
        }

    async def walk_ring(self, limit: int = 64) -> list[NodeRef]:
        """Follow successor pointers around the whole ring."""
        ring, cur = [self.ref], self.successors[0]
        while cur != self.ref and len(ring) < limit:
            ring.append(cur)
            try:
                succs = await self._call(cur, "get_successors")
            except (NodeUnreachable, RPCError):
                break
            cur = NodeRef.from_dict(succs[0])
        return ring
