'use strict';

const STATE = Object.freeze({
  ALIVE: 'alive',
  SUSPECT: 'suspect',
  DEAD: 'dead',
});

const DEFAULTS = Object.freeze({
  incarnation: 0,
  peers: [],
  probeTimeout: 1000,
  indirectProbeCount: 2,
  suspectTimeout: 6000,
  gossipRetransmits: 3,
});

/**
 * SWIM-style gossip membership state machine.
 *
 * The engine never reads a wall clock: every method that depends on time
 * takes `now` from the caller, and `tick(now)` advances internal timers.
 *
 * Message rules (event = { type: 'alive'|'suspect'|'dead', node, incarnation }):
 *  - events with an incarnation older than the local record are dropped;
 *  - alive with a newer incarnation (re)joins the node, even from dead;
 *  - suspect/dead about the local node is refuted by bumping the local
 *    incarnation and gossiping alive;
 *  - a dead node can only return with a strictly higher incarnation.
 */
class Membership {
  constructor(options = {}) {
    const opts = { ...DEFAULTS, ...options };
    if (!opts.nodeId) throw new Error('Membership: nodeId is required');
    this.nodeId = opts.nodeId;
    this.probeTimeout = opts.probeTimeout;
    this.indirectProbeCount = opts.indirectProbeCount;
    this.suspectTimeout = opts.suspectTimeout;
    this.gossipRetransmits = opts.gossipRetransmits;

    this._members = new Map();
    this._broadcasts = [];
    this._probes = new Map();

    this._set(this.nodeId, STATE.ALIVE, opts.incarnation, 0);
    for (const peer of opts.peers) {
      if (peer !== this.nodeId) this._set(peer, STATE.ALIVE, 0, 0);
    }
    this._emit(STATE.ALIVE, this.nodeId, opts.incarnation);
  }

  getMember(id) {
    const member = this._members.get(id);
    return member ? { ...member } : undefined;
  }

  getMembers() {
    return [...this._members.values()]
      .map((member) => ({ ...member }))
      .sort((a, b) => (a.id < b.id ? -1 : 1));
  }

  aliveNodes() {
    return this.getMembers()
      .filter((member) => member.state === STATE.ALIVE)
      .map((member) => member.id);
  }

  snapshot() {
    const out = {};
    for (const member of this.getMembers()) {
      out[member.id] = { state: member.state, incarnation: member.incarnation };
    }
    return out;
  }

  /** Pending gossip events; each event is returned gossipRetransmits times. */
  getGossip() {
    const events = [];
    const remaining = [];
    for (const item of this._broadcasts) {
      events.push(item.event);
      item.left -= 1;
      if (item.left > 0) remaining.push(item);
    }
    this._broadcasts = remaining;
    return events.map((event) => ({ ...event }));
  }

  /** Observe a heartbeat from nodeId (equivalent to an alive event). */
  heartbeat(nodeId, incarnation, now = 0) {
    this.receive({ type: STATE.ALIVE, node: nodeId, incarnation }, now);
  }

  /** Process one gossip event. */
  receive(event, now = 0) {
    if (!event || typeof event.node !== 'string') return;
    if (event.node === this.nodeId) {
      this._receiveAboutSelf(event, now);
      return;
    }
    const current = this._members.get(event.node);
    switch (event.type) {
      case STATE.ALIVE:
        if (!current || event.incarnation > current.incarnation) {
          this._set(event.node, STATE.ALIVE, event.incarnation, now);
          this._emit(STATE.ALIVE, event.node, event.incarnation);
        }
        break;
      case STATE.SUSPECT:
        if (!current || current.state === STATE.DEAD) break;
        if (event.incarnation < current.incarnation) break;
        if (current.state === STATE.ALIVE || event.incarnation > current.incarnation) {
          this._set(event.node, STATE.SUSPECT, event.incarnation, now);
          this._emit(STATE.SUSPECT, event.node, event.incarnation);
        }
        break;
      case STATE.DEAD:
        if (!current || current.state === STATE.DEAD) break;
        if (event.incarnation < current.incarnation) break;
        this._set(event.node, STATE.DEAD, event.incarnation, now);
        this._emit(STATE.DEAD, event.node, event.incarnation);
        break;
      default:
        break;
    }
  }

  /** Local node voluntarily re-announces itself with a bumped incarnation. */
  bumpIncarnation(now = 0) {
    const self = this._members.get(this.nodeId);
    self.incarnation += 1;
    self.state = STATE.ALIVE;
    self.suspectSince = null;
    self.updatedAt = now;
    this._emit(STATE.ALIVE, this.nodeId, self.incarnation);
    return self.incarnation;
  }

  /** Begin a direct probe of target. Caller must deliver the ping. */
  beginProbe(target, now = 0) {
    if (target === this.nodeId) throw new Error('Membership: cannot probe self');
    if (!this._members.has(target)) throw new Error(`Membership: unknown node ${target}`);
    this._probes.set(target, {
      startedAt: now,
      direct: 'pending',
      indirectStartedAt: null,
      helpers: new Map(),
    });
    return { type: 'ping', target };
  }

  /**
   * Report the direct probe outcome. On failure, returns ping-req requests
   * [{ type: 'ping-req', via, target }] for the caller to deliver.
   */
  completeProbe(target, ok, now = 0) {
    const probe = this._probes.get(target);
    if (!probe || probe.direct !== 'pending') return [];
    if (ok) {
      this._probes.delete(target);
      return [];
    }
    return this._startIndirect(target, probe, now);
  }

  /** Report an indirect probe outcome through helper `via`. */
  completeIndirectProbe(target, via, ok, now = 0) {
    const probe = this._probes.get(target);
    if (!probe || !probe.helpers.has(via)) return;
    probe.helpers.set(via, ok);
    if (ok) {
      this._probes.delete(target);
      return;
    }
    if ([...probe.helpers.values()].every((result) => result === false)) {
      this._probes.delete(target);
      this._suspect(target, now);
    }
  }

  /**
   * Advance timers to `now`. Expires direct/indirect probes and promotes
   * suspects whose suspectTimeout elapsed to dead.
   * Returns { indirectProbes: [...] } the caller should send.
   */
  tick(now = 0) {
    const actions = { indirectProbes: [] };
    for (const [target, probe] of [...this._probes]) {
      if (probe.direct === 'pending' && now - probe.startedAt >= this.probeTimeout) {
        actions.indirectProbes.push(...this._startIndirect(target, probe, now));
      } else if (probe.direct === 'failed' && now - probe.indirectStartedAt >= this.probeTimeout) {
        this._probes.delete(target);
        this._suspect(target, now);
      }
    }
    for (const member of this._members.values()) {
      if (member.state === STATE.SUSPECT && now - member.suspectSince >= this.suspectTimeout) {
        this._set(member.id, STATE.DEAD, member.incarnation, now);
        this._emit(STATE.DEAD, member.id, member.incarnation);
      }
    }
    return actions;
  }

  /** Full local table for anti-entropy exchange after a partition heals. */
  fullState() {
    return this.getMembers().map((member) => ({
      node: member.id,
      state: member.state,
      incarnation: member.incarnation,
    }));
  }

  /**
   * Merge a remote full table record-by-record through the normal
   * incarnation rules, so neither table wholesale-overwrites the other.
   */
  mergeState(records, now = 0) {
    for (const record of records) {
      this.receive({ type: record.state, node: record.node, incarnation: record.incarnation }, now);
    }
  }

  _receiveAboutSelf(event, now) {
    const self = this._members.get(this.nodeId);
    if (event.type === STATE.ALIVE) {
      if (event.incarnation > self.incarnation) self.incarnation = event.incarnation;
      return;
    }
    if (event.incarnation >= self.incarnation) {
      self.incarnation = event.incarnation + 1;
      self.state = STATE.ALIVE;
      self.suspectSince = null;
      self.updatedAt = now;
      this._emit(STATE.ALIVE, this.nodeId, self.incarnation);
    }
  }

  _startIndirect(target, probe, now) {
    probe.direct = 'failed';
    probe.indirectStartedAt = now;
    const helpers = this.aliveNodes()
      .filter((id) => id !== this.nodeId && id !== target)
      .sort()
      .slice(0, this.indirectProbeCount);
    for (const helper of helpers) probe.helpers.set(helper, null);
    if (helpers.length === 0) {
      this._probes.delete(target);
      this._suspect(target, now);
      return [];
    }
    return helpers.map((via) => ({ type: 'ping-req', via, target }));
  }

  _suspect(target, now) {
    const member = this._members.get(target);
    if (member && member.state === STATE.ALIVE) {
      this._set(target, STATE.SUSPECT, member.incarnation, now);
      this._emit(STATE.SUSPECT, target, member.incarnation);
    }
  }

  _set(id, state, incarnation, now) {
    const member = this._members.get(id) || { id };
    member.state = state;
    member.incarnation = incarnation;
    member.updatedAt = now;
    member.suspectSince = state === STATE.SUSPECT ? now : null;
    this._members.set(id, member);
  }

  _emit(type, node, incarnation) {
    this._broadcasts.push({
      event: { type, node, incarnation },
      left: this.gossipRetransmits,
    });
  }
}

module.exports = { Membership, STATE };
