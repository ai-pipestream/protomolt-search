// The dashboard: live telemetry from StreamMetrics (server-sent events),
// the runtime knobs, the shard map, and the recent-queries ring. Every
// diagnostics call that the cluster does not serve yet renders as a
// plain "not served" state and is polled slowly.
import { api, el, clear, fmtMs, fmtNum, loadConfig, mountHeader, errorText, isUnimplemented, refreshStatus } from '/common.js';
import { Sparkline, histogramQuantile, histogramDelta } from '/sparkline.js';

const $ = (id) => document.getElementById(id);
const WINDOW = 30; // snapshots kept for windowed rates and percentiles
const state = {
  config: null,
  target: 'coordinator',
  stream: null,
  snapshots: [],       // ring of parsed snapshots
  routeSparks: new Map(),
  tiles: new Map(),
  timers: [],
  unserved: new Set(), // diagnostics methods the cluster answered UNIMPLEMENTED
};

mountHeader('dashboard');
init();

async function init() {
  try { state.config = await loadConfig(); } catch (e) { state.config = { nodes: [] }; }
  const target = $('target');
  (state.config.nodes || []).forEach((addr, i) => target.append(el('option', { value: `node${i}`, text: `node${i} ${addr}` })));
  target.addEventListener('change', () => { state.target = target.value; restart(); });
  $('interval').addEventListener('change', () => restart());
  buildTiles();
  restart();
}

function restart() {
  stopAll();
  state.snapshots = [];
  state.unserved.clear();
  for (const s of state.routeSparks.values()) s.reset();
  clear($('routes').querySelector('tbody'));
  openStream();
  poll('GetRuntimeKnobs', renderKnobs, 5000);
  poll('GetShardDiagnostics', renderShards, 10000);
  poll('RecentQueries', renderRecent, 3000, { limit: 50 });
  state.timers.push(setInterval(healthTiles, 5000));
  healthTiles();
}

function stopAll() {
  if (state.stream) { state.stream.close(); state.stream = null; }
  for (const t of state.timers) clearInterval(t);
  state.timers = [];
}

// Polls one diagnostics RPC, slowing to 30 s once it answers UNIMPLEMENTED.
function poll(method, render, every, body = {}) {
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

function noteUnserved() {
  const note = $('diag-note');
  if (state.unserved.size) note.textContent = `not served by this cluster: ${[...state.unserved].join(', ')} (retrying every 30 s)`;
  else note.textContent = '';
}

// ---------------------------------------------------------------------------
// Tiles
// ---------------------------------------------------------------------------

const TILES = [
  ['req_rate', 'requests / s', 'all routes'],
  ['p99', 'p99 latency', 'all routes, this window'],
  ['in_flight', 'in flight', 'requests open now'],
  ['err_rate', 'errors / s', 'by status code'],
  ['cand_rate', 'candidates / s', 'vector scan'],
  ['floor_rate', 'floors published / s', 'shared cutoffs on the wire'],
  ['shards_up', 'shards reachable', 'from ClusterHealth'],
  ['live_docs', 'live documents', 'from ClusterHealth'],
];

function buildTiles() {
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

function setTile(id, value, sub) {
  $(`tile-${id}`).textContent = value;
  if (sub != null) $(`tile-${id}-sub`).textContent = sub;
}

async function healthTiles() {
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

// ---------------------------------------------------------------------------
// The metrics stream
// ---------------------------------------------------------------------------

function openStream() {
  const pill = $('stream-state');
  pill.className = 'pill';
  pill.textContent = 'connecting';
  const interval = Number($('interval').value);
  state.stream = api.stream('DiagnosticsService', 'StreamMetrics', { interval_ms: interval }, {
    onEvent: (snapshot) => {
      pill.className = 'pill ok';
      pill.textContent = `streaming every ${interval} ms`;
      ingest(snapshot);
    },
    onError: (err) => {
      if (isUnimplemented(err)) {
        pill.className = 'pill warn';
        pill.textContent = 'metrics stream not served by this cluster';
        state.unserved.add('StreamMetrics');
        noteUnserved();
        state.timers.push(setTimeout(openStream, 30000));
      } else {
        pill.className = 'pill bad';
        pill.textContent = errorText(err);
        state.timers.push(setTimeout(openStream, 5000));
      }
    },
    onEnd: () => {
      pill.className = 'pill warn';
      pill.textContent = 'stream ended';
      state.timers.push(setTimeout(openStream, 2000));
    },
  }, state.target);
}

function labelsOf(sample) {
  const out = {};
  for (const l of sample.labels || []) out[l.name] = l.value;
  return out;
}

function parseSnapshot(raw) {
  const at = Number(raw.unixMs || Date.now());
  const counters = new Map(); // name{labels} -> value
  const gauges = new Map();
  const histograms = new Map(); // key -> histogram sample
  const key = (name, labels) => `${name}{${Object.entries(labels).sort().map(([k, v]) => `${k}=${v}`).join(',')}}`;
  for (const s of raw.samples || []) {
    const labels = labelsOf(s);
    const k = key(s.name, labels);
    const entry = { name: s.name, labels, value: Number(s.value) };
    if (s.kind === 'METRIC_KIND_GAUGE') gauges.set(k, entry); else counters.set(k, entry);
  }
  for (const h of raw.histograms || []) {
    const labels = labelsOf(h);
    histograms.set(key(h.name, labels), { name: h.name, labels, buckets: h.buckets || [], sum: Number(h.sum), count: Number(h.count) });
  }
  return { at, counters, gauges, histograms, raw };
}

function ingest(raw) {
  const snap = parseSnapshot(raw);
  state.snapshots.push(snap);
  if (state.snapshots.length > WINDOW) state.snapshots.shift();
  $('raw-snapshot').textContent = JSON.stringify(raw, null, 1).slice(0, 20000);
  const prev = state.snapshots.length > 1 ? state.snapshots[state.snapshots.length - 2] : null;
  const oldest = state.snapshots[0];
  const dt = prev ? Math.max(0.001, (snap.at - prev.at) / 1000) : null;
  const windowDt = Math.max(0.001, (snap.at - oldest.at) / 1000);

  const rate = (name, filter = () => true) => {
    if (!prev) return 0;
    let now = 0, before = 0;
    for (const [k, c] of snap.counters) if (c.name === name && filter(c.labels)) { now += c.value; before += prev.counters.get(k)?.value ?? 0; }
    return Math.max(0, now - before) / dt;
  };
  const gaugeSum = (name) => { let s = 0; for (const g of snap.gauges.values()) if (g.name === name) s += g.value; return s; };

  const reqRate = rate('turbovec_requests_total');
  const errRate = rate('turbovec_request_errors_total');
  const candRate = rate('turbovec_scan_candidates_total');
  const floorRate = rate('turbovec_scan_floors_published_total');
  const inFlight = gaugeSum('turbovec_requests_in_flight');
  setTile('req_rate', reqRate.toFixed(2)); state.tiles.get('req_rate').push(reqRate);
  setTile('err_rate', errRate.toFixed(2)); state.tiles.get('err_rate').push(errRate);
  setTile('cand_rate', fmtNum(Math.round(candRate))); state.tiles.get('cand_rate').push(candRate);
  setTile('floor_rate', floorRate.toFixed(1)); state.tiles.get('floor_rate').push(floorRate);
  setTile('in_flight', String(inFlight)); state.tiles.get('in_flight').push(inFlight);

  // Overall p99 over the window: merge every request-duration histogram
  // without a phase label (phased ones count a request twice).
  const merged = mergeHistograms(snap, oldest, (labels) => !labels.phase);
  const p99 = merged ? histogramQuantile(merged.buckets, 0.99) : null;
  setTile('p99', p99 == null ? '–' : fmtMs(p99 * 1000), merged ? `${fmtNum(merged.count)} requests in ${windowDt.toFixed(0)} s` : 'no requests yet');
  if (p99 != null) state.tiles.get('p99').push(p99 * 1000);

  renderRoutes(snap, prev, oldest, dt, windowDt);
}

function isDuration(name) { return name.includes('request_duration_seconds'); }

function mergeHistograms(snap, oldest, filter) {
  let out = null;
  for (const [k, h] of snap.histograms) {
    if (!isDuration(h.name) || !filter(h.labels)) continue;
    const d = histogramDelta(h, oldest === snap ? null : oldest.histograms.get(k));
    if (!out) { out = { count: 0, sum: 0, buckets: d.buckets.map((b) => ({ le: b.le, cumulativeCount: 0 })) }; }
    out.count += d.count; out.sum += d.sum;
    d.buckets.forEach((b, i) => { if (out.buckets[i]) out.buckets[i].cumulativeCount += Number(b.cumulativeCount); });
  }
  return out;
}

function renderRoutes(snap, prev, oldest, dt, windowDt) {
  const routes = new Map();
  for (const c of snap.counters.values()) {
    if (c.name === 'turbovec_requests_total' && c.labels.rpc) routes.set(c.labels.rpc, { rpc: c.labels.rpc, total: c.value, errors: 0 });
  }
  for (const c of snap.counters.values()) {
    if (c.name === 'turbovec_request_errors_total' && routes.has(c.labels.rpc)) routes.get(c.labels.rpc).errors += c.value;
  }
  const tbody = $('routes').querySelector('tbody');
  const rows = [...routes.values()].sort((a, b) => b.total - a.total);
  for (const r of rows) {
    let tr = tbody.querySelector(`tr[data-rpc="${r.rpc}"]`);
    if (!tr) {
      const canvas = el('canvas', { class: 'spark', style: 'height:18px;width:120px' });
      tr = el('tr', { 'data-rpc': r.rpc }, [
        el('td', { class: 'mono', text: r.rpc }), el('td', { class: 'num rate' }), el('td', { class: 'num p50' }), el('td', { class: 'num p99' }), el('td', { class: 'num err' }), el('td', {}, [canvas]),
      ]);
      tbody.append(tr);
      state.routeSparks.set(r.rpc, new Sparkline(canvas, { fill: false }));
    }
    const before = prev ? [...prev.counters.values()].find((c) => c.name === 'turbovec_requests_total' && c.labels.rpc === r.rpc)?.value ?? 0 : r.total;
    const rate = prev ? Math.max(0, r.total - before) / dt : 0;
    const h = mergeHistograms(snap, oldest, (labels) => labels.rpc === r.rpc && !labels.phase);
    const p50 = h ? histogramQuantile(h.buckets, 0.5) : null;
    const p99 = h ? histogramQuantile(h.buckets, 0.99) : null;
    tr.querySelector('.rate').textContent = rate.toFixed(2);
    tr.querySelector('.p50').textContent = p50 == null ? '–' : fmtMs(p50 * 1000);
    tr.querySelector('.p99').textContent = p99 == null ? '–' : fmtMs(p99 * 1000);
    tr.querySelector('.err').textContent = fmtNum(r.errors);
    state.routeSparks.get(r.rpc).push(rate);
    tr.classList.toggle('dim', r.total === before && !rate);
  }
  void windowDt;
}

// ---------------------------------------------------------------------------
// Knobs
// ---------------------------------------------------------------------------

function renderKnobs(r, err) {
  const box = clear($('knobs'));
  if (!r) { box.textContent = isUnimplemented(err) ? 'not served by this cluster' : errorText(err); return; }
  box.append(el('div', { class: 'muted small', text: `process ${r.process || ''}` }));
  for (const k of r.knobs || []) {
    const kind = (k.kind || '').replace('KNOB_KIND_', '');
    let input;
    if (kind === 'BOOL') input = el('input', { type: 'checkbox', checked: k.value === 'true', disabled: !k.mutable });
    else if (kind === 'INT' || kind === 'FLOAT') input = el('input', { type: 'number', value: k.value, step: kind === 'FLOAT' ? 'any' : '1', disabled: !k.mutable, class: 'mono' });
    else input = el('input', { value: k.value, disabled: !k.mutable, class: 'mono' });
    const apply = el('button', { text: k.mutable ? 'apply' : 'fixed', disabled: !k.mutable, onclick: async () => {
      const value = kind === 'BOOL' ? String(input.checked) : String(input.value);
      apply.disabled = true;
      try {
        const updated = await api.rpc('DiagnosticsService', 'SetRuntimeKnob', { name: k.name, value }, state.target);
        renderKnobs(updated);
      } catch (e) {
        apply.disabled = false;
        box.prepend(el('div', { class: 'error', text: errorText(e) }));
      }
    } });
    box.append(el('div', { class: `knob ${k.mutable ? '' : 'locked'}` }, [
      el('div', {}, [el('div', { class: 'name', text: `${k.name} ` }, [el('span', { class: 'muted', text: (k.scope || '').replace('KNOB_SCOPE_', '').toLowerCase() })]), el('div', { class: 'desc', text: `${k.description || ''}${k.startupValue != null && k.startupValue !== k.value ? ` (started as ${k.startupValue})` : ''}` })]),
      input, apply,
    ]));
  }
}

// ---------------------------------------------------------------------------
// Shard map
// ---------------------------------------------------------------------------

function renderShards(r, err) {
  const box = clear($('shards'));
  if (!r) { box.textContent = isUnimplemented(err) ? 'not served by this cluster' : errorText(err); return; }
  box.classList.remove('muted');
  box.append(el('div', { class: 'muted small', text: `process ${r.process || ''}, topology ${r.topologyGeneration || 0}` }));
  for (const s of r.shards || []) {
    const shard = el('div', { class: 'shard' });
    shard.append(el('div', { class: 'head' }, [
      el('strong', { text: `shard ${s.shard}` }), el('span', { class: 'muted', text: s.address || '' }), el('span', { text: s.layout || '' }),
      el('span', { text: `${fmtNum(s.liveRows)} live / ${fmtNum(s.rows)} rows, ${fmtNum(s.tombstones)} tombstones, tail ${fmtNum(s.tailRows)}` }),
      s.partitionKey ? el('span', { class: 'team-a', text: `partitioned by ${s.partitionKey}` }) : null,
      el('span', { class: 'muted', text: `pruning ${s.segmentPruning ? 'on' : 'off'}, floors ${s.floorSharing ? 'on' : 'off'}` }),
    ]));
    const segments = s.segments || [];
    if (segments.length) {
      const total = segments.reduce((a, g) => a + Number(g.rows), 0) || 1;
      const bar = el('div', { class: 'segbar' });
      for (const g of segments) {
        const seg = el('div', {
          class: `seg ${g.partition ? 'part' : ''} ${g.hasSummary ? '' : 'nosum'}`,
          style: `flex-basis:${(100 * Number(g.rows)) / total}%`,
          onmouseenter: () => { $('seginfo').textContent = segmentInfo(g); },
        });
        bar.append(seg);
      }
      shard.append(bar);
    } else {
      shard.append(el('div', { class: 'muted small', text: 'no sealed segments' }));
    }
    box.append(shard);
  }
}

function segmentInfo(g) {
  const parts = [`${g.segmentId} gen ${g.generation} base ${g.base} rows ${fmtNum(g.rows)} live ${fmtNum(g.liveRows)}${g.mapped ? ' mapped' : ''}`];
  if (g.partition) parts.push(`partition ${g.partition.column} ${g.partition.lo}..=${g.partition.hi}`);
  for (const c of g.columns || []) {
    parts.push(c.floating ? `${c.column} [${fmtNum(c.loF)}, ${fmtNum(c.hiF)}] present ${fmtNum(c.present)}` : `${c.column} [${c.lo}, ${c.hi}] present ${fmtNum(c.present)}`);
  }
  if (!g.hasSummary) parts.push('no summary (sealed before summaries existed; never pruned)');
  return parts.join(' · ');
}

// ---------------------------------------------------------------------------
// Recent queries
// ---------------------------------------------------------------------------

function renderRecent(r, err) {
  const box = clear($('recent'));
  if (!r) { box.textContent = isUnimplemented(err) ? 'not served by this cluster' : errorText(err); return; }
  box.classList.remove('muted');
  box.append(el('div', { class: 'muted small', text: `${fmtNum(r.totalSeen)} seen since start` }));
  const table = el('table', {}, [el('thead', {}, [el('tr', {}, ['time', 'route', 'executed', 'k', 'ms', 'status', 'hits', 'segments', 'candidates'].map((h) => el('th', { text: h })))])]);
  const tbody = el('tbody');
  for (const q of r.queries || []) {
    const time = new Date(Number(q.unixMs)).toLocaleTimeString();
    const tr = el('tr', { class: `clickable ${q.status && q.status !== 'OK' ? 'team-b' : ''}` }, [
      el('td', { class: 'mono', text: time }), el('td', { class: 'mono', text: q.route || '' }), el('td', { class: 'mono', text: q.executed || '' }),
      el('td', { class: 'num', text: q.k ?? '' }), el('td', { class: 'num', text: fmtMs(q.totalMs) }), el('td', { text: q.status || '' }),
      el('td', { class: 'num', text: q.hits ?? '' }), el('td', { class: 'num', text: q.segmentsTotal ? `${q.segmentsSkipped ?? 0}/${q.segmentsTotal}` : '' }),
      el('td', { class: 'num', text: fmtNum(q.candidatesCollected) }),
    ]);
    const detail = el('tr', { class: 'hidden' }, [el('td', { colspan: 9 }, [el('pre', { class: 'raw', text: JSON.stringify(q, null, 1) })])]);
    tr.addEventListener('click', () => detail.classList.toggle('hidden'));
    tbody.append(tr, detail);
  }
  table.append(tbody);
  box.append(table);
}
