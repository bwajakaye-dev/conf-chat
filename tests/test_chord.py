"""Tests for the Chord DHT layer. Run: python -m unittest discover -s tests -v"""
import asyncio
import unittest

from confchat.chord import ChordNode, between, between_right, hash_key

FAST = 0.15   # stabilization period used by the tests


async def make_ring(n: int) -> list[ChordNode]:
    nodes = [ChordNode(interval=FAST)]
    await nodes[0].start()
    for _ in range(n - 1):
        node = ChordNode(interval=FAST)
        await node.start(nodes[0].addr)
        nodes.append(node)
    await settle(nodes)
    return nodes


def ring_ok(nodes) -> bool:
    order = sorted(nodes, key=lambda x: x.ref.id)
    for i, node in enumerate(order):
        nxt, prv = order[(i + 1) % len(order)], order[i - 1]
        if node.successors[0] != nxt.ref:
            return False
        if len(order) > 1 and node.predecessor != prv.ref:
            return False
    return True


async def settle(nodes, timeout: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not ring_ok(nodes):
        if loop.time() > end:
            raise AssertionError("ring did not stabilize")
        await asyncio.sleep(FAST)
    await asyncio.sleep(FAST * 4)      # let fingers / replicas catch up


def owner_of(nodes, key: str) -> ChordNode:
    h = hash_key(key)
    order = sorted(nodes, key=lambda x: x.ref.id)
    return next((n for n in order if n.ref.id >= h), order[0])


class IntervalTests(unittest.TestCase):
    def test_between(self):
        self.assertTrue(between(5, 1, 10))
        self.assertFalse(between(10, 1, 10))
        self.assertTrue(between(0, 10, 2))       # wrap-around
        self.assertTrue(between_right(10, 1, 10))
        self.assertTrue(between_right(7, 7, 7))  # single node owns everything


class RingTestCase(unittest.IsolatedAsyncioTestCase):
    """IsolatedAsyncioTestCase enables asyncio debug mode, which is ~10x slower."""

    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)


class ChordTests(RingTestCase):
    async def asyncTearDown(self):
        for n in getattr(self, "nodes", []):
            if n.running:
                await n.stop()

    async def test_ring_forms_and_lookups_are_correct(self):
        self.nodes = await make_ring(6)
        for key in (f"key-{i}" for i in range(40)):
            expected = owner_of(self.nodes, key).ref
            for asker in self.nodes[::2]:
                self.assertEqual(await asker.find_successor(hash_key(key)), expected)
        walked = await self.nodes[0].walk_ring()
        self.assertEqual(len(walked), 6)

    async def test_put_get_from_any_node_and_atomic_sets(self):
        self.nodes = await make_ring(4)
        await self.nodes[0].dht("hello", "put", value={"x": 1})
        for n in self.nodes:
            self.assertEqual(await n.dht("hello", "get"), {"x": 1})
        # concurrent writers to the same set never lose updates
        await asyncio.gather(*(self.nodes[i % 4].dht("inbox", "sadd", member=f"m{i}", value=i)
                               for i in range(30)))
        self.assertEqual(len(await self.nodes[3].dht("inbox", "smembers")), 30)
        self.assertTrue(await self.nodes[1].dht("u", "put_if_absent", value=1))
        self.assertFalse(await self.nodes[2].dht("u", "put_if_absent", value=2))

    async def test_data_survives_crash_of_owner(self):
        self.nodes = await make_ring(5)
        keys = [f"k{i}" for i in range(25)]
        for k in keys:
            await self.nodes[0].dht(k, "put", value=k.upper())
        await asyncio.sleep(FAST * 4)                       # replication
        victim = owner_of(self.nodes, keys[0])
        await victim.stop()                                 # crash, no hand-off
        self.nodes.remove(victim)
        await settle(self.nodes)
        for k in keys:
            self.assertEqual(await self.nodes[0].dht(k, "get"), k.upper())

    async def test_graceful_leave_and_new_join_keep_data(self):
        self.nodes = await make_ring(4)
        for i in range(20):
            await self.nodes[1].dht(f"d{i}", "put", value=i)
        leaver = self.nodes.pop(2)
        await leaver.leave()
        late = ChordNode(interval=FAST)
        await late.start(self.nodes[0].addr)
        self.nodes.append(late)
        await settle(self.nodes)
        for i in range(20):
            self.assertEqual(await late.dht(f"d{i}", "get"), i)
        # the newcomer received (and stores) every key it is now responsible for
        for i in range(20):
            if owner_of(self.nodes, f"d{i}") is late:
                self.assertEqual(late.store[f"d{i}"]["data"], i)


if __name__ == "__main__":
    unittest.main()
