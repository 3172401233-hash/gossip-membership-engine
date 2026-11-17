'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Membership, STATE } = require('../src/membership');

const OPTS = { probeTimeout: 100, indirectProbeCount: 2, suspectTimeout: 500, gossipRetransmits: 4 };

function makeCluster(ids, overrides = {}) {
  const nodes = new Map();
  for (const id of ids) {
    nodes.set(id, new Membership({ nodeId: id, peers: ids.filter((p) => p !== id), ...OPTS, ...overrides }));
  }
  return nodes;
}

// One gossip round: collect pending events from every node and deliver them
// to every other node whose link(from, to) is up.
function gossipRound(nodes, now, link = () => true) {
  const outbox = [];
  for (const [from, node] of nodes) {
    for (const event of node.getGossip()) outbox.push({ from, event });
  }
  for (const { from, event } of outbox) {
    for (const [to, node] of nodes) {
      if (to !== from && link(from, to)) node.receive(event, now);
    }
  }
}

// One probe round: every node probes every peer it believes is alive.
// A probe succeeds only if both directions of every hop are up.
function probeRound(nodes, now, link = () => true) {
  for (const [id, node] of nodes) {
    for (const peer of node.aliveNodes()) {
      if (peer === id) continue;
      node.beginProbe(peer, now);
      const directOk = link(id, peer) && link(peer, id);
      const requests = node.completeProbe(peer, directOk, now);
      for (const req of requests) {
        const ok =
          link(id, req.via) &&
          link(req.via, req.target) &&
          link(req.target, req.via) &&
          link(req.via, id);
        node.completeIndirectProbe(req.target, req.via, ok, now);
      }
    }
  }
}

function snapshots(nodes) {
  return [...nodes.values()].map((node) => JSON.stringify(node.snapshot()));
}

function assertConverged(nodes) {
  const snaps = snapshots(nodes);
  assert.ok(
    snaps.every((snap) => snap === snaps[0]),
    `membership tables diverged:\n${snaps.join('\n')}`,
  );
}

function lcg(seed) {
  let state = seed;
  return () => {
    state = (state * 1103515245 + 12345) & 0x7fffffff;
    return state / 0x7fffffff;
  };
}

test('converges under random packet loss', () => {
  const nodes = makeCluster(['A', 'B', 'C', 'D']);
  const rand = lcg(42);
  let now = 0;
  for (let round = 0; round < 15; round += 1) {
    now += 10;
    // 40% of messages are dropped, deterministically.
    gossipRound(nodes, now, () => rand() >= 0.4);
  }
  assertConverged(nodes);
  for (const node of nodes.values()) {
    assert.equal(node.aliveNodes().length, 4);
  }
});

test('probe timeout: unresponsive node degrades alive -> suspect -> dead', () => {
  const nodes = makeCluster(['A', 'B', 'C']);
  const a = nodes.get('A');
  // B has silently hung: its acks never come back.
  a.beginProbe('B', 0);
  const actions = a.tick(100); // direct probe times out
  assert.deepEqual(actions.indirectProbes.map((r) => r.via), ['C']);
  a.tick(199);
  assert.equal(a.getMember('B').state, STATE.ALIVE);
  a.tick(200); // indirect probe times out too
  assert.equal(a.getMember('B').state, STATE.SUSPECT);
  a.tick(699);
  assert.equal(a.getMember('B').state, STATE.SUSPECT);
  a.tick(700); // suspectTimeout elapsed
  assert.equal(a.getMember('B').state, STATE.DEAD);
});

test('one-way partition: broken direct path is saved by indirect probes', () => {
  const nodes = makeCluster(['A', 'B', 'C']);
  // A -> B is dropped, everything else works.
  const link = (from, to) => !(from === 'A' && to === 'B');
  probeRound(nodes, 0, link);
  for (const node of nodes.values()) {
    assert.equal(node.aliveNodes().length, 3);
  }
});

test('one-way partition without indirect probes: victim refutes and cluster reconverges', () => {
  const nodes = makeCluster(['A', 'B', 'C'], { indirectProbeCount: 0 });
  const link = (from, to) => !(from === 'A' && to === 'B');
  let now = 0;
  // Initial gossip so everyone knows each other.
  gossipRound(nodes, now, link);
  // B's direct probe of A fails (ack A->B lost); with no helpers B suspects A.
  now += 10;
  probeRound(nodes, now, link);
  assert.equal(nodes.get('B').getMember('A').state, STATE.SUSPECT);
  // The suspect(A) gossip reaches A via C; A refutes with a higher incarnation.
  for (let round = 0; round < 6; round += 1) {
    now += 10;
    gossipRound(nodes, now, link);
  }
  assertConverged(nodes);
  for (const node of nodes.values()) {
    assert.equal(node.getMember('A').state, STATE.ALIVE);
    assert.ok(node.getMember('A').incarnation >= 1);
  }
});

test('forged incarnation: victim refutes a forged dead, stale forgeries are ignored', () => {
  const nodes = makeCluster(['A', 'B', 'C']);
  let now = 0;
  gossipRound(nodes, now);
  // Attacker injects dead(B) with a huge incarnation into A and C.
  for (const id of ['A', 'C']) {
    nodes.get(id).receive({ type: STATE.DEAD, node: 'B', incarnation: 100 }, now);
  }
  assert.equal(nodes.get('A').getMember('B').state, STATE.DEAD);
  // The forged dead reaches B, which refutes by bumping past it.
  for (let round = 0; round < 6; round += 1) {
    now += 10;
    gossipRound(nodes, now);
  }
  assertConverged(nodes);
  for (const node of nodes.values()) {
    assert.equal(node.getMember('B').state, STATE.ALIVE);
    assert.equal(node.getMember('B').incarnation, 101);
  }
  // Stale forgeries below the current incarnation are dropped.
  for (const id of ['A', 'C']) {
    nodes.get(id).receive({ type: STATE.DEAD, node: 'B', incarnation: 50 }, now);
    nodes.get(id).receive({ type: STATE.SUSPECT, node: 'B', incarnation: 100 }, now);
  }
  assert.equal(nodes.get('A').getMember('B').state, STATE.ALIVE);
  assert.equal(nodes.get('C').getMember('B').state, STATE.ALIVE);
});

test('network partition and recovery: tables merge per-record and reconverge', () => {
  const ids = ['A', 'B', 'C', 'D', 'E'];
  const nodes = makeCluster(ids);
  const left = new Set(['A', 'B']);
  const sameSide = (x, y) => left.has(x) === left.has(y);
  let now = 0;
  gossipRound(nodes, now);
  // Partition: probes across the split fail, suspects expire to dead.
  for (let round = 0; round < 3; round += 1) {
    now += 10;
    probeRound(nodes, now, sameSide);
    gossipRound(nodes, now, sameSide);
  }
  now += OPTS.suspectTimeout;
  for (const node of nodes.values()) node.tick(now);
  gossipRound(nodes, now, sameSide);
  gossipRound(nodes, now, sameSide);

  for (const id of left) {
    const node = nodes.get(id);
    for (const other of ['C', 'D', 'E']) {
      assert.equal(node.getMember(other).state, STATE.DEAD, `${id} should see ${other} dead`);
    }
  }
  for (const id of ['C', 'D', 'E']) {
    const node = nodes.get(id);
    for (const other of left) {
      assert.equal(node.getMember(other).state, STATE.DEAD, `${id} should see ${other} dead`);
    }
  }

  // Partition heals. A heartbeat with the old incarnation is rejected:
  // the recovering node must first learn it was declared dead.
  now += 10;
  nodes.get('A').heartbeat('C', 0, now);
  assert.equal(nodes.get('A').getMember('C').state, STATE.DEAD);

  // Anti-entropy: every node merges every other node's full table.
  // Each node sees itself recorded as dead and refutes with a bumped
  // incarnation, which is how it rejoins.
  for (const node of nodes.values()) {
    for (const other of nodes.values()) {
      if (node !== other) node.mergeState(other.fullState(), now);
    }
  }
  for (let round = 0; round < 6; round += 1) {
    now += 10;
    gossipRound(nodes, now);
  }

  assertConverged(nodes);
  for (const node of nodes.values()) {
    assert.equal(node.aliveNodes().length, ids.length);
    assert.ok(node.getMember(node.nodeId).incarnation >= 1);
  }
});
