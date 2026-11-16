// The dashboard's polling loop, the not-served state, and the tiles.
import { api, clear, el, fmtNum, isUnimplemented, refreshStatus } from './common.js';
import { Sparkline } from './sparkline.js';
import { $, state } from './dashboard-state.js';
import { openStream } from './dashboard-metrics.js';
import { renderKnobs, renderRecent, renderShards } from './dashboard-cluster.js';

export function restart() {
  stopAll();
  state.snapshots = [];
  state.unserved.clear();
  for (const s of state.routeSparks.values()) s.reset();
  clear($('routes').querySelector('tbody'));
  state.activeRoutes.clear();
  openStream();
  poll('GetRuntimeKnobs', renderKnobs, 5000);
  poll('GetShardDiagnostics', renderShards, 10000);
  poll('RecentQueries', renderRecent, 3000, { limit: 50 });
  state.timers.push(setInterval(healthTiles, 5000));
  healthTiles();
}

export function stopAll() {
  if (state.stream) { state.stream.close(); state.stream = null; }
  for (const t of state.timers) clearInterval(t);
  state.timers = [];
}

// Polls one diagnostics RPC, slowing to 30 s once it answers UNIMPLEMENTED.
export function poll(method, render, every, body = {}) {
  let delay = every;
  let timer;
  const tick = async () => {
    try {
      const r = await api.rpc('DiagnosticsService', method, body, state.target);
      state.unserved.delete(method);
      delay = every;
      render(r);
    } catch (e) {
      if (isUnimplemented(e)) {
        state.unserved.add(method);
        delay = 30000;
        render(null, e);
      } else {
        render(null, e);
      }
    }
    noteUnserved();
    timer = setTimeout(tick, delay);
    state.timers.push(timer);
  };
  tick();
}

export function noteUnserved() {
  const note = $('diag-note');
  if (state.unserved.size) note.textContent = `not served by this cluster: ${[...state.unserved].join(', ')} (retrying every 30 s)`;
  else note.textContent = '';
}

// ---------------------------------------------------------------------------
// Tiles
// ---------------------------------------------------------------------------

export const TILES = [
  ['req_rate', 'requests / s', 'all routes'],
  ['p99', 'p99 latency', 'all routes, this window'],
  ['in_flight', 'in flight', 'requests open now'],
  ['err_rate', 'errors / s', 'by status code'],
  ['cand_rate', 'candidates / s', 'vector scan'],
  ['floor_rate', 'floors published / s', 'shared cutoffs on the wire'],
  ['scan_rate', 'scan bytes / s', 'encoded index bytes the kernel processed'],
  ['scan_busy', 'scan active', 'kernel seconds per wall second'],
  ['shards_up', 'shards reachable', 'from ClusterHealth'],
  ['live_docs', 'live documents', 'from ClusterHealth'],
];

export function buildTiles() {
  const grid = clear($('tiles'));
  for (const [id, label, sub] of TILES) {
    const canvas = el('canvas', { class: 'spark' });
    const tile = el('div', { class: 'tile' }, [
      el('div', { class: 'label', text: label }),
      el('div', { class: 'value', id: `tile-${id}`, text: '–' }),
      el('div', { class: 'sub', id: `tile-${id}-sub`, text: sub }),
      canvas,
    ]);
    grid.append(tile);
    state.tiles.set(id, new Sparkline(canvas));
  }
}

export function setTile(id, value, sub) {
  $(`tile-${id}`).textContent = value;
  if (sub != null) $(`tile-${id}-sub`).textContent = sub;
}

export async function healthTiles() {
  const health = await refreshStatus();
  if (!health) { setTile('shards_up', 'unreachable', ''); return; }
  const targets = health.targets || [];
  const up = targets.filter((t) => t.reachable).length;
  const live = targets.reduce((a, t) => a + Number(t.live_docs ?? t.bm25_docs ?? 0), 0);
  const deleted = targets.reduce((a, t) => a + Number(t.deleted_docs ?? 0), 0);
  setTile('shards_up', `${up} / ${targets.length}`, targets.filter((t) => !t.reachable).map((t) => `${t.shard}: ${t.error}`).join('; ') || 'all reachable');
  setTile('live_docs', fmtNum(live), `${fmtNum(deleted)} tombstoned`);
  state.tiles.get('shards_up').push(up);
  state.tiles.get('live_docs').push(live);
}
