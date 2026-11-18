"""Command-line interface: start a peer, then chat from the terminal."""
from __future__ import annotations

import argparse
import asyncio
import getpass
import logging
import os
import queue
import shlex
import sys
import threading
from datetime import datetime

from .chat import ChatError, ConfChatPeer
from .chord import ChordNode, DHTError, NodeRef
from .rpc import NodeUnreachable, RPCError

HELP = """
Accounts
  register [username]            create an account (asks for full name + password)
  login [username]               log in on this peer
  passwd                         change your password
  logout                         log out (also leaves all your conferences)

Friends
  friends                        list friends and who is online
  search <words...>              find users whose full name contains all the words
  add <username>                 send a friend request
  requests                       show pending friend requests
  accept <username> / reject <username>

Messages
  msg <username> <text>          message a friend (delivered now, or when they log in)

Conferences
  conf new <name> [users...]     create a conference with some friends
  conf add <id> <username>       add a friend to a conference you are in
  conf leave <id>                leave a conference
  confs                          list your conferences and their participants
  c <id> <text>                  send a message to a conference

P2P / Chord internals
  ring                           this node's id, predecessor, successors, fingers
  nodes                          walk the whole ring
  keys                           DHT keys stored on this node (owned / replica)

  help                           show this help
  quit                           log out, leave the ring gracefully and exit
"""


class Style:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def _c(self, code: str, s: str) -> str:
        return f"\033[{code}m{s}\033[0m" if self.enabled else s

    def dim(self, s): return self._c("2", s)
    def bold(self, s): return self._c("1", s)
    def red(self, s): return self._c("31", s)
    def green(self, s): return self._c("32", s)
    def yellow(self, s): return self._c("33", s)
    def cyan(self, s): return self._c("36", s)
    def magenta(self, s): return self._c("35", s)


class Console:
    """Reads stdin on a daemon thread so incoming messages can print meanwhile."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.requests: queue.Queue = queue.Queue()
        self.prompt = ""
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        while True:
            prompt, secret, fut = self.requests.get()
            try:
                if secret and sys.stdin.isatty():
                    line = getpass.getpass(prompt)
                else:  # plain input also lets the demo be scripted through a pipe
                    line = input(prompt)
            except (EOFError, KeyboardInterrupt):
                line = None
            self.loop.call_soon_threadsafe(lambda f=fut, v=line: f.done() or f.set_result(v))

    async def ask(self, prompt: str, secret: bool = False) -> str | None:
        self.prompt = prompt
        fut = self.loop.create_future()
        self.requests.put((prompt, secret, fut))
        try:
            return await fut
        finally:
            self.prompt = ""


def _clock(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


class App:
    def __init__(self, node: ChordNode, style: Style) -> None:
        self.node = node
        self.s = style
        self.peer = ConfChatPeer(node, on_event=self.show_event)
        self.console: Console | None = None

    # -------------------------------------------------------------- output
    def out(self, text: str = "") -> None:
        print(text, flush=True)

    def format_msg(self, m: dict) -> str:
        s, t = self.s, self.s.dim(_clock(m["ts"]))
        kind = m["type"]
        if kind == "dm":
            return f"{t} {s.cyan(m['from'])} -> you: {m['text']}"
        if kind in ("conf", "conf_event"):
            tag = s.magenta("[" + m["conf_name"] + "#" + m["conf"] + "]")
            if kind == "conf":
                return f"{t} {tag} {s.cyan(m['from'])}: {m['text']}"
            return f"{t} {tag} {s.dim(m['text'])}"
        if kind == "friend_request":
            return (f"{t} {s.yellow('*')} {m['from']} ({m['full_name']}) wants to be your friend"
                    f" - type 'accept {m['from']}'")
        if kind == "friend_accept":
            return f"{t} {s.yellow('*')} {m['from']} accepted your friend request"
        if kind == "presence":
            state = s.green("online") if m["online"] else s.dim("offline")
            return f"{t} {s.yellow('*')} {m['from']} is now {state}"
        return f"{t} {m}"

    def show_event(self, m: dict) -> None:
        prompt = self.console.prompt if self.console else ""
        # erase the half-drawn prompt, print the message, redraw the prompt
        sys.stdout.write("\r" + " " * (len(prompt) + 2) + "\r")
        sys.stdout.write(self.format_msg(m) + "\n" + prompt)
        sys.stdout.flush()

    # ---------------------------------------------------------------- loop
    async def run(self) -> None:
        self.console = Console(asyncio.get_running_loop())
        s = self.s
        self.out(s.bold("Conf-Chat") + f"  peer {s.cyan(str(self.node.ref))}  (type 'help')")
        while True:
            who = self.peer.user or "guest"
            line = await self.console.ask(f"{who}> ")
            if line is None:
                break
            try:
                args = shlex.split(line)
            except ValueError:
                args = line.split()
            if not args:
                continue
            cmd, rest = args[0].lower(), args[1:]
            if cmd in ("quit", "exit"):
                break
            handler = getattr(self, f"cmd_{cmd}", None)
            if handler is None:
                self.out(s.red(f"unknown command '{cmd}' (type 'help')"))
                continue
            try:
                await handler(rest, line)
            except ChatError as e:
                self.out(s.red(f"error: {e}"))
            except (DHTError, NodeUnreachable, RPCError) as e:
                self.out(s.red(f"network error: {e} (try again in a moment)"))
            except (IndexError, ValueError):
                self.out(s.red(f"bad arguments for '{cmd}' (type 'help')"))

    async def shutdown(self) -> None:
        if self.peer.user:
            try:
                await self.peer.logout()
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------ commands
    async def cmd_help(self, a, line):
        self.out(HELP)

    async def _ask(self, prompt, secret=False):
        v = await self.console.ask(prompt, secret)
        if v is None:
            raise ChatError("cancelled")
        return v.strip() if not secret else v

    async def cmd_register(self, a, line):
        user = a[0] if a else await self._ask("username: ")
        name = await self._ask("full name: ")
        pw = await self._ask("password: ", secret=True)
        if pw != await self._ask("repeat password: ", secret=True):
            raise ChatError("passwords do not match")
        await self.peer.register(user, name, pw)
        self.out(self.s.green(f"registered {user.lower()} - now 'login {user.lower()}'"))

    async def cmd_login(self, a, line):
        user = a[0] if a else await self._ask("username: ")
        pw = await self._ask("password: ", secret=True)
        info = await self.peer.login(user, pw)
        s = self.s
        self.out(s.green(f"welcome, {self.peer.full_name}!"))
        online = [f["username"] for f in info["friends"] if f["online"]]
        self.out(f"friends online: {', '.join(online) if online else s.dim('none')}"
                 f"  ({len(info['friends'])} friend(s) total)")
        if info["requests"]:
            self.out(s.yellow(f"pending friend requests: "
                              f"{', '.join(r['from'] for r in info['requests'])}"))
        if info["offline_messages"]:
            self.out(s.bold(f"--- {len(info['offline_messages'])} message(s) while you were away ---"))
            for m in info["offline_messages"]:
                self.out(self.format_msg(m))
            self.out(s.bold("---"))

    async def cmd_logout(self, a, line):
        await self.peer.logout()
        self.out("logged out")

    async def cmd_passwd(self, a, line):
        old = await self._ask("current password: ", secret=True)
        new = await self._ask("new password: ", secret=True)
        if new != await self._ask("repeat new password: ", secret=True):
            raise ChatError("passwords do not match")
        await self.peer.change_password(old, new)
        self.out(self.s.green("password changed"))

    async def cmd_friends(self, a, line):
        fl = await self.peer.friends()
        if not fl:
            self.out(self.s.dim("no friends yet - try 'search <name>' and 'add <username>'"))
        for f in fl:
            dot = self.s.green("online ") if f["online"] else self.s.dim("offline")
            self.out(f"  {dot}  {f['username']:<16} {f['full_name']}")

    cmd_online = cmd_friends

    async def cmd_search(self, a, line):
        res = await self.peer.search(" ".join(a))
        if not res:
            self.out(self.s.dim("no matching users"))
        for r in res:
            tag = self.s.green(" (friend)") if r["friend"] else ""
            self.out(f"  {r['username']:<16} {r['full_name']}{tag}")

    async def cmd_add(self, a, line):
        r = await self.peer.request_friend(a[0])
        if r == "accepted":
            self.out(self.s.green(f"{a[0]} had already asked - you are now friends"))
        else:
            self.out(f"friend request sent to {a[0]}")

    async def cmd_requests(self, a, line):
        reqs = await self.peer.pending_requests()
        if not reqs:
            self.out(self.s.dim("no pending requests"))
        for r in reqs:
            self.out(f"  {r['from']:<16} {r['full_name']}   ({_clock(r['ts'])})")

    async def cmd_accept(self, a, line):
        await self.peer.accept_friend(a[0])
        self.out(self.s.green(f"you and {a[0]} are now friends"))

    async def cmd_reject(self, a, line):
        await self.peer.reject_friend(a[0])
        self.out(f"rejected {a[0]}")

    async def cmd_msg(self, a, line):
        _, to, text = line.strip().split(maxsplit=2)
        status = await self.peer.send(to, text)
        note = "delivered" if status == "delivered" else f"{to} is offline - stored in the DHT"
        self.out(self.s.dim(f"  ({note})"))

    async def cmd_c(self, a, line):
        _, cid, text = line.strip().split(maxsplit=2)
        res = await self.peer.conf_send(cid, text)
        stored = [u for u, st in res.items() if st != "delivered"]
        note = f"sent to {len(res)} participant(s)"
        if stored:
            note += f"; stored for offline: {', '.join(stored)}"
        self.out(self.s.dim(f"  ({note})"))

    async def cmd_conf(self, a, line):
        sub = a[0].lower()
        if sub == "new":
            cid = await self.peer.create_conf(a[1], a[2:])
            self.out(self.s.green(f"conference '{a[1]}' created with id {cid}")
                     + f" - talk with: c {cid} <text>")
        elif sub == "add":
            await self.peer.conf_add(a[1], a[2])
            self.out(f"added {a[2]} to {a[1]}")
        elif sub == "leave":
            await self.peer.conf_leave(a[1])
            self.out(f"left conference {a[1]}")
        else:
            raise ValueError(sub)

    async def cmd_confs(self, a, line):
        confs = await self.peer.conferences()
        if not confs:
            self.out(self.s.dim("you are not in any conference"))
        for c in confs:
            self.out(f"  {self.s.magenta(c['id'])}  {c['name']:<16} members: {', '.join(c['members'])}")

    async def cmd_ring(self, a, line):
        print_ring_info(self.node, self.s)

    async def cmd_nodes(self, a, line):
        ring = await self.node.walk_ring()
        self.out(f"{len(ring)} node(s) on the ring (clockwise from this one):")
        for n in ring:
            self.out(f"  {n.short}  {n.addr}")

    async def cmd_keys(self, a, line):
        rows = sorted(self.node.store.items())
        live = [(k, e) for k, e in rows if e["data"] is not None]
        if not live:
            self.out(self.s.dim("no keys stored on this node"))
        for k, e in live:
            role = self.s.green("owner  ") if self.node._owns(e["h"]) else self.s.dim("replica")
            self.out(f"  {role}  {format(e['h'], '040x')[:8]}  {k:<32} v{e['ver']}")


def print_ring_info(node: ChordNode, s: Style) -> None:
    i = node.info()
    fmt = lambda d: str(NodeRef.from_dict(d)) if d else "none"  # noqa: E731
    print(f"node        {s.cyan(fmt(i['node']))}")
    print(f"predecessor {fmt(i['predecessor'])}")
    print(f"successors  {', '.join(fmt(x) for x in i['successors'])}")
    print(f"fingers     {', '.join(fmt(x) for x in i['fingers'])}")
    print(f"keys        {i['keys_owned']} owned, {i['keys_replica']} replicas", flush=True)


async def run(args) -> int:
    node = ChordNode(args.host, args.port, bind=args.bind, interval=args.interval)
    try:
        await node.start(args.join)
    except (NodeUnreachable, RPCError, DHTError, OSError) as e:
        print(f"could not start peer: {e}", file=sys.stderr)
        return 1
    style = Style(not args.no_color)
    try:
        if args.headless:
            print(f"headless Chord node {node.ref} running"
                  f"{' (joined ' + args.join + ')' if args.join else ' (new ring)'}; Ctrl+C to leave",
                  flush=True)
            while True:
                await asyncio.sleep(args.status or 3600)
                if args.status:
                    print("-" * 60)
                    print_ring_info(node, style)
        else:
            app = App(node, style)
            try:
                await app.run()
            finally:
                await app.shutdown()
    finally:
        await node.leave()
        print("left the ring", flush=True)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="confchat", description="Conf-Chat: P2P chat on a Chord DHT")
    p.add_argument("--host", default="127.0.0.1",
                   help="address other peers use to reach this one (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=0, help="TCP port to listen on (default: random)")
    p.add_argument("--bind", default=None, help="interface to bind (default: same as --host; "
                   "use 0.0.0.0 for LAN demos)")
    p.add_argument("--join", metavar="HOST:PORT", help="any existing peer; omit to start a new ring")
    p.add_argument("--headless", action="store_true",
                   help="run only a DHT node (stores data and routes, no chat UI)")
    p.add_argument("--status", type=float, default=0,
                   help="headless: print ring status every N seconds")
    p.add_argument("--interval", type=float, default=1.0, help="stabilization period in seconds")
    p.add_argument("--no-color", action="store_true", help="disable ANSI colours")
    p.add_argument("-v", "--verbose", action="store_true", help="log Chord activity")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    if os.name == "nt":
        os.system("")  # enables ANSI escape codes in the Windows console
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0
