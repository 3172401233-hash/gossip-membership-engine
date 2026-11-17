'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Membership, STATE } = require('../src/membership');

const OPTS = { probeTimeout: 100, indirectProbeCount: 2, suspectTimeout: 500, gossipRetransmits: 2 };

function make(nodeId = 'A', peers = ['B', 'C', 'D']) {
  return new Membership({ nodeId, peers, ...OPTS });
}

test('heartbeat registers a new node as alive', () => {
  const m = make('A', []);
  m.heartbeat('X', 3, 10);
  assert.equal(m.getMember('X').state, STATE.ALIVE);
  assert.equal(m.getMember('X').incarnation, 3);
});

test('suspect gossip marks an alive node suspect', () => {
  const m = make();
  m.receive({ type: STATE.SUSPECT, node: 'B', incarnation: 0 }, 10);
  assert.equal(m.getMember('B').state, STATE.SUSPECT);
});

test('suspect is refuted by an alive with higher incarnation', () => {
  const m = make();
  m.receive({ type: STATE.SUSPECT, node: 'B', incarnation: 0 }, 10);
  m.receive({ type: STATE.ALIVE, node: 'B', incarnation: 1 }, 20);
  assert.equal(m.getMember('B').state, STATE.ALIVE);
  assert.equal(m.getMember('B').incarnation, 1);
});

test('alive with the same incarnation does not clear a suspicion', () => {
  const m = make();
  m.receive({ type: STATE.SUSPECT, node: 'B', incarnation: 0 }, 10);
  m.receive({ type: STATE.ALIVE, node: 'B', incarnation: 0 }, 20);
  assert.equal(m.getMember('B').state, STATE.SUSPECT);
});

test('suspect times out to dead only when the caller advances time', () => {
  const m = make();
  m.receive({ type: STATE.SUSPECT, node: 'B', incarnation: 0 }, 1000);
  m.tick(1000 + OPTS.suspectTimeout - 1);
  assert.equal(m.getMember('B').state, STATE.SUSPECT);
  m.tick(1000 + OPTS.suspectTimeout);
  assert.equal(m.getMember('B').state, STATE.DEAD);
});

test('dead node cannot be resurrected by old-incarnation messages', () => {
  const m = make();
  m.receive({ type: STATE.DEAD, node: 'B', incarnation: 5 }, 10);
  m.heartbeat('B', 5, 20);
  m.heartbeat('B', 4, 30);
  assert.equal(m.getMember('B').state, STATE.DEAD);
});

test('dead node rejoins with a strictly higher incarnation', () => {
  const m = make();
  m.receive({ type: STATE.DEAD, node: 'B', incarnation: 5 }, 10);
  m.heartbeat('B', 6, 20);
  assert.equal(m.getMember('B').state, STATE.ALIVE);
  assert.equal(m.getMember('B').incarnation, 6);
});

test('stale suspect and dead events are ignored', () => {
  const m = make();
  m.heartbeat('B', 7, 10);
  m.receive({ type: STATE.SUSPECT, node: 'B', incarnation: 6 }, 20);
  m.receive({ type: STATE.DEAD, node: 'B', incarnation: 6 }, 30);
  assert.equal(m.getMember('B').state, STATE.ALIVE);
  m.receive({ type: STATE.SUSPECT, node: 'B', incarnation: 7 }, 40);
  assert.equal(m.getMember('B').state, STATE.SUSPECT);
});

test('local node refutes suspect and dead about itself by bumping incarnation', () => {
  const m = make();
  m.receive({ type: STATE.SUSPECT, node: 'A', incarnation: 0 }, 10);
  assert.equal(m.getMember('A').incarnation, 1);
  assert.equal(m.getMember('A').state, STATE.ALIVE);
  m.receive({ type: STATE.DEAD, node: 'A', incarnation: 1 }, 20);
  assert.equal(m.getMember('A').incarnation, 2);
  const gossip = m.getGossip().filter((e) => e.node === 'A');
  assert.ok(gossip.some((e) => e.type === STATE.ALIVE && e.incarnation === 2));
});

test('direct probe failure escalates to indirect probes via helpers', () => {
  const m = make();
  m.beginProbe('B', 0);
  const requests = m.completeProbe('B', false, 10);
  assert.deepEqual(
    requests.map((r) => r.via).sort(),
    ['C', 'D'],
  );
  assert.ok(requests.every((r) => r.type === 'ping-req' && r.target === 'B'));
});

test('one successful indirect probe keeps the node alive', () => {
  const m = make();
  m.beginProbe('B', 0);
  m.completeProbe('B', false, 10);
  m.completeIndirectProbe('B', 'C', true, 20);
  m.completeIndirectProbe('B', 'D', false, 25);
  assert.equal(m.getMember('B').state, STATE.ALIVE);
});

test('all probes failing marks the node suspect and gossips it', () => {
  const m = make();
  m.beginProbe('B', 0);
  m.completeProbe('B', false, 10);
  m.completeIndirectProbe('B', 'C', false, 20);
  m.completeIndirectProbe('B', 'D', false, 25);
  assert.equal(m.getMember('B').state, STATE.SUSPECT);
  const gossip = m.getGossip();
  assert.ok(gossip.some((e) => e.type === STATE.SUSPECT && e.node === 'B'));
});

test('probe timeouts are driven by tick with caller-supplied time', () => {
  const m = make();
  m.beginProbe('B', 0);
  let actions = m.tick(OPTS.probeTimeout - 1);
  assert.equal(actions.indirectProbes.length, 0);
  actions = m.tick(OPTS.probeTimeout);
  assert.deepEqual(actions.indirectProbes.map((r) => r.via).sort(), ['C', 'D']);
  m.tick(2 * OPTS.probeTimeout - 1);
  assert.equal(m.getMember('B').state, STATE.ALIVE);
  m.tick(2 * OPTS.probeTimeout);
  assert.equal(m.getMember('B').state, STATE.SUSPECT);
});

test('gossip events are retransmitted a bounded number of times', () => {
  const m = new Membership({ nodeId: 'A', gossipRetransmits: 2 });
  const first = m.getGossip();
  const second = m.getGossip();
  const third = m.getGossip();
  assert.equal(first.length, 1);
  assert.equal(second.length, 1);
  assert.equal(third.length, 0);
});

test('mergeState applies remote records per-incarnation instead of overwriting', () => {
  const m = make();
  m.heartbeat('B', 9, 10);
  m.mergeState(
    [
      { node: 'B', state: STATE.DEAD, incarnation: 3 },
      { node: 'C', state: STATE.DEAD, incarnation: 0 },
      { node: 'E', state: STATE.ALIVE, incarnation: 1 },
    ],
    20,
  );
  assert.equal(m.getMember('B').state, STATE.ALIVE);
  assert.equal(m.getMember('C').state, STATE.DEAD);
  assert.equal(m.getMember('E').state, STATE.ALIVE);
});
