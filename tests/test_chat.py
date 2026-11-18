"""End-to-end tests of the Conf-Chat use case on a real (local) Chord ring."""
import asyncio
import unittest

from confchat.chat import ChatError, ConfChatPeer
from confchat.chord import ChordNode

from test_chord import RingTestCase, make_ring


class Client:
    """A peer process: one Chord node + one chat peer + an event log."""

    def __init__(self, node: ChordNode):
        self.node = node
        self.events: list[dict] = []
        self.peer = ConfChatPeer(node, self.events.append, heartbeat=0.5, online_ttl=2.0)

    def texts(self, type_=None):
        return [e.get("text") for e in self.events if type_ is None or e["type"] == type_]


async def eventually(cond, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond():
        if loop.time() > end:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.05)


class ChatTests(RingTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.nodes = await make_ring(5)
        self.alice, self.bob, self.carol, self.dave, self.erin = (Client(n) for n in self.nodes)

    async def asyncTearDown(self):
        for n in self.nodes:
            if n.running:
                await n.stop()

    async def register_all(self):
        await self.alice.peer.register("alice", "Alice Marie Smith", "pw-a")
        await self.bob.peer.register("bob", "Bob Smith", "pw-b")
        await self.carol.peer.register("carol", "Carol Jones", "pw-c")
        await self.dave.peer.register("dave", "Dave Marie Jones", "pw-d")

    async def befriend(self, a: Client, b: Client):
        await a.peer.request_friend(b.peer.user)
        await b.peer.accept_friend(a.peer.user)

    async def test_accounts_and_search(self):
        await self.register_all()
        with self.assertRaises(ChatError):
            await self.erin.peer.register("alice", "Someone Else", "xxxx")
        with self.assertRaises(ChatError):
            await self.erin.peer.login("alice", "wrong")
        await self.erin.peer.login("alice", "pw-a")
        found = {r["username"] for r in await self.erin.peer.search("smith")}
        self.assertEqual(found, {"bob"})                          # self excluded
        found = {r["username"] for r in await self.erin.peer.search("marie jones")}
        self.assertEqual(found, {"dave"})                         # all words must match
        await self.erin.peer.change_password("pw-a", "new-pw")
        await self.erin.peer.logout()
        with self.assertRaises(ChatError):
            await self.alice.peer.login("alice", "pw-a")
        await self.alice.peer.login("alice", "new-pw")

    async def test_friends_online_and_offline_messages(self):
        await self.register_all()
        a, b = self.alice, self.bob
        await a.peer.login("alice", "pw-a")
        await b.peer.login("bob", "pw-b")
        with self.assertRaises(ChatError):
            await a.peer.send("bob", "not friends yet")
        await a.peer.request_friend("bob")
        await eventually(lambda: any(e["type"] == "friend_request" for e in b.events))
        self.assertEqual([r["from"] for r in await b.peer.pending_requests()], ["alice"])
        await b.peer.accept_friend("alice")
        self.assertEqual([(f["username"], f["online"]) for f in await a.peer.friends()],
                         [("bob", True)])

        # online -> delivered immediately, peer to peer
        self.assertEqual(await a.peer.send("bob", "hi bob"), "delivered")
        self.assertIn("hi bob", b.texts("dm"))

        # bob logs out; alice writes, then alice logs out AND her node leaves the ring
        await b.peer.logout()
        self.assertEqual(await a.peer.send("bob", "while you were away"), "stored")
        await a.peer.logout()
        await a.node.leave()
        # bob logs back in from a different peer and still gets the message
        info = await self.carol.peer.login("bob", "pw-b")
        self.assertEqual([m["text"] for m in info["offline_messages"]], ["while you were away"])

    async def test_conference_lifecycle(self):
        await self.register_all()
        a, b, c, d = self.alice, self.bob, self.carol, self.dave
        for cl, u, pw in ((a, "alice", "pw-a"), (b, "bob", "pw-b"),
                          (c, "carol", "pw-c"), (d, "dave", "pw-d")):
            await cl.peer.login(u, pw)
        await self.befriend(a, b)
        await self.befriend(b, c)
        await self.befriend(c, d)

        with self.assertRaises(ChatError):          # carol is not alice's friend
            await a.peer.create_conf("proj", ["carol"])
        cid = await a.peer.create_conf("proj", ["bob"])
        with self.assertRaises(ChatError):          # carol cannot join herself
            await c.peer.conf_send(cid, "let me in")
        await b.peer.conf_add(cid, "carol")         # any participant may add people
        res = await a.peer.conf_send(cid, "hello all")
        self.assertEqual(res, {"bob": "delivered", "carol": "delivered"})
        await eventually(lambda: "hello all" in c.texts("conf"))
        await c.peer.conf_send(cid, "hi from carol")
        await eventually(lambda: "hi from carol" in a.texts("conf") and
                         "hi from carol" in b.texts("conf"))

        # carol leaves the system -> automatically leaves the conference
        await c.peer.logout()
        conf = (await a.peer.conferences())[0]
        self.assertEqual(conf["members"], ["alice", "bob"])
        await a.peer.conf_send(cid, "carol should not get this")
        info = await c.peer.login("carol", "pw-c")
        self.assertNotIn("carol should not get this",
                         [m.get("text") for m in info["offline_messages"]])
        self.assertEqual(await c.peer.conferences(), [])
        # ... until she is invited back
        await b.peer.conf_add(cid, "carol")
        await a.peer.conf_send(cid, "welcome back")
        await eventually(lambda: "welcome back" in c.texts("conf"))

    async def test_message_to_offline_conference_member(self):
        await self.register_all()
        a, b = self.alice, self.bob
        await a.peer.login("alice", "pw-a")
        await b.peer.login("bob", "pw-b")
        await self.befriend(a, b)
        await b.peer.logout()
        cid = await a.peer.create_conf("later", ["bob"])   # bob is offline
        self.assertEqual(await a.peer.conf_send(cid, "read this later"), {"bob": "stored"})
        info = await b.peer.login("bob", "pw-b")
        self.assertIn("read this later", [m.get("text") for m in info["offline_messages"]])

    async def test_single_session_per_user(self):
        await self.register_all()
        await self.alice.peer.login("alice", "pw-a")
        with self.assertRaises(ChatError):
            await self.bob.peer.login("alice", "pw-a")


if __name__ == "__main__":
    unittest.main()
