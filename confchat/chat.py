"""Conf-Chat application logic, built on top of the Chord DHT.

All shared state lives in the DHT (spread over every peer and replicated);
messages to online users travel directly peer-to-peer.

DHT key layout
--------------
user:<username>          {"username", "full_name", "salt", "pw", "created"}
word:<word>              set of usernames whose full name contains <word>
presence:<username>      {"addr", "ts"}       (refreshed by a heartbeat)
friends:<username>       set of usernames
freq:<username>          set of pending incoming friend requests
inbox:<username>         set of undelivered messages (msg id -> message)
conf:<id>                {"id", "name", "creator", "created"}
confmembers:<id>         set of usernames currently in the conference
userconfs:<username>     set of conference ids the user belongs to
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import re
import secrets
import time
import uuid

from .chord import ChordNode
from .rpc import NodeUnreachable, RPCError, call

log = logging.getLogger("confchat.chat")

HEARTBEAT = 5.0       # seconds between presence refreshes
ONLINE_TTL = 15.0     # presence older than this means "offline"
USERNAME_RE = re.compile(r"^[a-z0-9_]{2,24}$")


class ChatError(Exception):
    """A user-facing error (bad password, not friends, ...)."""


def name_words(text: str) -> list[str]:
    return sorted(set(re.findall(r"[a-z0-9]+", text.lower())))


def _pw_hash(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 100_000).hex()


def _now() -> float:
    return time.time()


class ConfChatPeer:
    def __init__(self, node: ChordNode, on_event=None, *,
                 heartbeat: float = HEARTBEAT, online_ttl: float = ONLINE_TTL) -> None:
        self.node = node
        self.on_event = on_event or (lambda msg: None)
        self.heartbeat = heartbeat
        self.online_ttl = online_ttl
        self.user: str | None = None
        self.full_name: str | None = None
        self._seen: set[str] = set()
        self._hb_task: asyncio.Task | None = None
        node.register_handler("chat_deliver", self._h_deliver)

    # ------------------------------------------------------------- plumbing
    async def _dht(self, key: str, op: str, **args):
        return await self.node.dht(key, op, **args)

    def _require_login(self) -> str:
        if not self.user:
            raise ChatError("you are not logged in")
        return self.user

    def _emit(self, msg: dict) -> None:
        if msg["id"] in self._seen:
            return
        self._seen.add(msg["id"])
        try:
            self.on_event(msg)
        except Exception:  # noqa: BLE001 - UI errors must not break delivery
            log.exception("event handler failed")

    async def _h_deliver(self, user, msg):
        """RPC: another peer pushes a message to the user logged in here."""
        if self.user != user:
            return False
        self._emit(msg)
        return True

    def _fresh(self, presence) -> bool:
        return bool(presence) and _now() - presence["ts"] < self.online_ttl

    async def is_online(self, username: str) -> bool:
        return self._fresh(await self._dht(f"presence:{username}", "get"))

    def _msg(self, type_: str, **fields) -> dict:
        return {"id": uuid.uuid4().hex, "type": type_, "from": self.user, "ts": _now(), **fields}

    async def _deliver(self, to: str, msg: dict, store: bool = True) -> str:
        """Push directly if ``to`` is online, otherwise park it in their DHT inbox."""
        presence = await self._dht(f"presence:{to}", "get")
        if self._fresh(presence):
            try:
                if await call(presence["addr"], "chat_deliver", timeout=3.0, user=to, msg=msg):
                    return "delivered"
            except (NodeUnreachable, RPCError):
                pass
        if store:
            await self._dht(f"inbox:{to}", "sadd", member=msg["id"], value=msg)
            return "stored"
        return "dropped"

    async def _drain_inbox(self) -> list[dict]:
        me = self.user
        items = await self._dht(f"inbox:{me}", "smembers")
        msgs = sorted(items.values(), key=lambda m: m["ts"])
        for m in msgs:
            await self._dht(f"inbox:{me}", "srem", member=m["id"])
        return msgs

    async def _get_user(self, username: str) -> dict | None:
        return await self._dht(f"user:{username}", "get")

    # -------------------------------------------------------------- accounts
    async def register(self, username: str, full_name: str, password: str) -> None:
        u = username.strip().lower()
        if not USERNAME_RE.match(u):
            raise ChatError("username must be 2-24 chars of a-z, 0-9 or _")
        words = name_words(full_name)
        if not words:
            raise ChatError("full name is required")
        if len(password) < 4:
            raise ChatError("password must be at least 4 characters")
        salt = secrets.token_hex(16)
        rec = {"username": u, "full_name": " ".join(full_name.split()), "salt": salt,
               "pw": _pw_hash(password, salt), "created": _now()}
        if not await self._dht(f"user:{u}", "put_if_absent", value=rec):
            raise ChatError(f"username '{u}' is already taken")
        for w in words:
            await self._dht(f"word:{w}", "sadd", member=u)

    async def login(self, username: str, password: str) -> dict:
        if self.user:
            raise ChatError(f"already logged in as {self.user}; log out first")
        u = username.strip().lower()
        rec = await self._get_user(u)
        if not rec or not hmac.compare_digest(rec["pw"], _pw_hash(password, rec["salt"])):
            raise ChatError("wrong username or password")
        presence = await self._dht(f"presence:{u}", "get")
        if self._fresh(presence) and presence["addr"] != self.node.addr:
            raise ChatError(f"{u} is already logged in at {presence['addr']}")
        self.user, self.full_name = u, rec["full_name"]
        await self._put_presence()
        offline = await self._drain_inbox()
        for m in offline:
            self._seen.add(m["id"])
        friends = await self.friends()
        requests = await self.pending_requests()
        await self._notify_friends(online=True, friends=friends)
        self._hb_task = asyncio.create_task(self._heartbeat())
        return {"offline_messages": offline, "friends": friends, "requests": requests}

    async def logout(self) -> None:
        me = self._require_login()
        # leaving the system means leaving every conference
        for cid in list((await self._dht(f"userconfs:{me}", "smembers")).keys()):
            await self._leave_conf(cid, "logged out")
        if self._hb_task:
            self._hb_task.cancel()
            self._hb_task = None
        await self._dht(f"presence:{me}", "delete")
        await self._notify_friends(online=False)
        self.user = self.full_name = None

    async def change_password(self, old: str, new: str) -> None:
        me = self._require_login()
        rec = await self._get_user(me)
        if not hmac.compare_digest(rec["pw"], _pw_hash(old, rec["salt"])):
            raise ChatError("current password is wrong")
        if len(new) < 4:
            raise ChatError("password must be at least 4 characters")
        rec["salt"] = secrets.token_hex(16)
        rec["pw"] = _pw_hash(new, rec["salt"])
        await self._dht(f"user:{me}", "put", value=rec)

    async def _put_presence(self) -> None:
        await self._dht(f"presence:{self.user}", "put",
                        value={"addr": self.node.addr, "ts": _now()})

    async def _heartbeat(self) -> None:
        while self.user:
            await asyncio.sleep(self.heartbeat)
            try:
                await self._put_presence()
                # pick up anything that was parked while a direct push failed
                for m in await self._drain_inbox():
                    self._emit(m)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("heartbeat failed: %r", exc)

    async def _notify_friends(self, online: bool, friends=None) -> None:
        if friends is None:
            friends = await self.friends()
        msg = self._msg("presence", online=online)
        await asyncio.gather(*(self._deliver(f["username"], msg, store=False)
                               for f in friends if f["online"]), return_exceptions=True)

    # ---------------------------------------------------------------- friends
    async def search(self, query: str) -> list[dict]:
        """Users whose full name contains *every* word in ``query``."""
        me = self._require_login()
        words = name_words(query)
        if not words:
            return []
        sets = await asyncio.gather(*(self._dht(f"word:{w}", "smembers") for w in words))
        found = set(sets[0])
        for s in sets[1:]:
            found &= set(s)
        found.discard(me)
        friends = set(await self._dht(f"friends:{me}", "smembers"))
        recs = await asyncio.gather(*(self._get_user(u) for u in sorted(found)))
        return [{"username": r["username"], "full_name": r["full_name"],
                 "friend": r["username"] in friends} for r in recs if r]

    async def friends(self) -> list[dict]:
        me = self._require_login()
        names = sorted(await self._dht(f"friends:{me}", "smembers"))
        recs = await asyncio.gather(*(self._get_user(u) for u in names))
        online = await asyncio.gather(*(self.is_online(u) for u in names))
        return [{"username": u, "full_name": r["full_name"] if r else "?", "online": o}
                for u, r, o in zip(names, recs, online)]

    async def pending_requests(self) -> list[dict]:
        me = self._require_login()
        reqs = await self._dht(f"freq:{me}", "smembers")
        return sorted(reqs.values(), key=lambda r: r["ts"])

    async def request_friend(self, username: str) -> str:
        me = self._require_login()
        u = username.strip().lower()
        if u == me:
            raise ChatError("you cannot befriend yourself")
        if not await self._get_user(u):
            raise ChatError(f"no such user '{u}'")
        if u in await self._dht(f"friends:{me}", "smembers"):
            raise ChatError(f"{u} is already your friend")
        if u in await self._dht(f"freq:{me}", "smembers"):
            await self.accept_friend(u)        # they already asked us
            return "accepted"
        await self._dht(f"freq:{u}", "sadd", member=me,
                        value={"from": me, "full_name": self.full_name, "ts": _now()})
        await self._deliver(u, self._msg("friend_request", full_name=self.full_name), store=False)
        return "requested"

    async def accept_friend(self, username: str) -> None:
        me = self._require_login()
        u = username.strip().lower()
        if u not in await self._dht(f"freq:{me}", "smembers"):
            raise ChatError(f"no pending request from '{u}'")
        await self._dht(f"friends:{me}", "sadd", member=u)
        await self._dht(f"friends:{u}", "sadd", member=me)
        await self._dht(f"freq:{me}", "srem", member=u)
        await self._dht(f"freq:{u}", "srem", member=me)
        await self._deliver(u, self._msg("friend_accept", full_name=self.full_name))

    async def reject_friend(self, username: str) -> None:
        me = self._require_login()
        if not await self._dht(f"freq:{me}", "srem", member=username.strip().lower()):
            raise ChatError(f"no pending request from '{username}'")

    async def _require_friend(self, username: str) -> str:
        me = self._require_login()
        u = username.strip().lower()
        if u not in await self._dht(f"friends:{me}", "smembers"):
            raise ChatError(f"{u} is not your friend")
        return u

    async def send(self, username: str, text: str) -> str:
        """Direct message. Returns 'delivered' (live) or 'stored' (offline inbox)."""
        u = await self._require_friend(username)
        return await self._deliver(u, self._msg("dm", to=u, text=text))

    # ------------------------------------------------------------ conferences
    async def _conf_members(self, cid: str) -> dict:
        return await self._dht(f"confmembers:{cid}", "smembers")

    async def _require_member(self, cid: str) -> tuple[dict, dict]:
        me = self._require_login()
        meta = await self._dht(f"conf:{cid}", "get")
        members = await self._conf_members(cid)
        if not meta or me not in members:
            raise ChatError(f"you are not in a conference with id '{cid}'")
        return meta, members

    async def _broadcast(self, members, msg: dict) -> dict:
        targets = [m for m in members if m != self.user]
        results = await asyncio.gather(*(self._deliver(m, msg) for m in targets),
                                       return_exceptions=True)
        return {m: (r if isinstance(r, str) else "failed") for m, r in zip(targets, results)}

    async def create_conf(self, name: str, invitees: list[str]) -> str:
        me = self._require_login()
        users = [await self._require_friend(u) for u in invitees]
        cid = secrets.token_hex(3)
        await self._dht(f"conf:{cid}", "put",
                        value={"id": cid, "name": name, "creator": me, "created": _now()})
        for u in [me] + users:
            await self._dht(f"confmembers:{cid}", "sadd", member=u)
            await self._dht(f"userconfs:{u}", "sadd", member=cid)
        await self._broadcast(users, self._msg(
            "conf_event", conf=cid, conf_name=name,
            text=f"{me} added you to conference '{name}' [{cid}]"))
        return cid

    async def conf_add(self, cid: str, username: str) -> None:
        me = self._require_login()
        meta, members = await self._require_member(cid)
        u = await self._require_friend(username)
        if u in members:
            raise ChatError(f"{u} is already in the conference")
        await self._dht(f"confmembers:{cid}", "sadd", member=u)
        await self._dht(f"userconfs:{u}", "sadd", member=cid)
        await self._broadcast(list(members) + [u], self._msg(
            "conf_event", conf=cid, conf_name=meta["name"],
            text=f"{me} added {u} to the conference"))

    async def conf_send(self, cid: str, text: str) -> dict:
        meta, members = await self._require_member(cid)
        return await self._broadcast(members, self._msg(
            "conf", conf=cid, conf_name=meta["name"], text=text))

    async def conf_leave(self, cid: str) -> None:
        await self._require_member(cid)
        await self._leave_conf(cid, "left")

    async def _leave_conf(self, cid: str, why: str) -> None:
        me = self.user
        await self._dht(f"confmembers:{cid}", "srem", member=me)
        await self._dht(f"userconfs:{me}", "srem", member=cid)
        remaining = await self._conf_members(cid)
        meta = await self._dht(f"conf:{cid}", "get") or {"name": cid}
        if not remaining:
            await self._dht(f"conf:{cid}", "delete")
            return
        await self._broadcast(remaining, self._msg(
            "conf_event", conf=cid, conf_name=meta["name"], text=f"{me} {why}"))

    async def conferences(self) -> list[dict]:
        me = self._require_login()
        out = []
        for cid in sorted(await self._dht(f"userconfs:{me}", "smembers")):
            meta = await self._dht(f"conf:{cid}", "get")
            if meta:
                members = sorted(await self._conf_members(cid))
                out.append({**meta, "members": members})
        return out
