// The dashboard: live telemetry from StreamMetrics (server-sent events),
// the runtime knobs, the shard map, and the recent-queries ring. Every
// diagnostics call that the cluster does not serve yet renders as a
// plain "not served" state and is polled slowly.
import { api, el, clear, fmtMs, fmtNum, fmtBytes, loadConfig, mountHeader, errorText, isUnimplemented, refreshStatus } from '/common.js';
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
  $('plan-run').addEventListener('click', runPlacementPlan);
  $('bal-run').addEventListener('click', runBalancePlan);
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
  ['scan_rate', 'scan bytes / s', 'encoded index bytes the kernel processed'],
  ['scan_busy', 'scan active', 'kernel seconds per wall second'],
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
  // The scan budget's counters (docs/bandwidth-budget.md): bytes the
  // provider scan processed and the kernel's active time, per second of
  // wall time. Their ratio is this process's observed scan rate.
  const bytesRate = rate('turbovec_scan_bytes_total');
  const activeRate = rate('turbovec_scan_active_nanoseconds_total') / 1e9;
  const hasScan = [...snap.counters.values()].some((c) => c.name === 'turbovec_scan_bytes_total');
  if (hasScan) {
    const observed = activeRate > 0 ? bytesRate / activeRate : 0;
    setTile('scan_rate', `${fmtBytes(bytesRate)}/s`, observed > 0 ? `${fmtBytes(observed)}/s while scanning` : 'encoded index bytes the kernel processed');
    setTile('scan_busy', activeRate.toFixed(3), 'kernel seconds per wall second');
    state.tiles.get('scan_rate').push(bytesRate);
    state.tiles.get('scan_busy').push(activeRate);
  } else {
    setTile('scan_rate', '–', 'no scan counters in this snapshot');
    setTile('scan_busy', '–', 'no scan counters in this snapshot');
  }

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

// A shard whose layout diagnostics came back as a relay's refusal is a
// relay: the relay coordinator serves the node-facing surface only and
// names itself in the refusal (docs/relay-coordinators.md). The children
// behind it are its own shard map, which the coordinator's diagnostics
// do not see.
function isRelay(s) {
  return /relay/i.test(s.layout || '') && !(s.segments || []).length;
}

// The placement code as its path: one index per level, root first, in
// the tree's fixed field width (docs/placement.md "The code"). The width
// is the default unless the map says otherwise; the raw code is shown
// beside it so nothing is lost when the width differs.
function codePath(code, levelBits = 9n) {
  const c = BigInt(code);
  const path = [];
  const levels = 63n / levelBits;
  for (let level = 0n; level < levels; level++) {
    const shift = 63n - levelBits * (level + 1n);
    path.push(Number((c >> shift) & ((1n << levelBits) - 1n)));
  }
  while (path.length > 1 && path[path.length - 1] === 0) path.pop();
  return path.join('.');
}

function renderPlacement(r) {
  const box = clear($('placement'));
  const shards = (r.shards || []).filter((s) => s.hasPlacement);
  if (!shards.length) { box.append(el('div', { class: 'muted small', text: 'no placement tree: shards are hashed by stable key only' })); return; }
  const groups = new Map();
  for (const s of shards) {
    const code = String(s.placement ?? '0');
    if (!groups.has(code)) groups.set(code, []);
    groups.get(code).push(s);
  }
  box.append(el('div', { class: 'muted small', text: `placement tree: ${groups.size} group(s) served, by code` }));
  for (const [code, members] of [...groups.entries()].sort((a, b) => (BigInt(a[0]) < BigInt(b[0]) ? -1 : 1))) {
    const rows = members.reduce((a, s) => a + Number(s.rows || 0), 0);
    const mixed = members.filter((s) => s.placementMixed).length;
    box.append(el('div', { class: 'group' }, [
      el('strong', { text: `path ${codePath(code)}` }), el('span', { class: 'code', text: `code ${code}` }),
      el('span', { text: `${members.length} shard(s): ${members.map((s) => (isRelay(s) ? `relay ${s.shard}` : s.shard)).join(', ')}` }),
      el('span', { class: 'muted', text: `${fmtNum(rows)} rows` }),
      mixed ? el('span', { class: 'pill warn', text: `${mixed} mid-migration` }) : null,
    ]));
  }
}

function renderShards(r, err) {
  const box = clear($('shards'));
  if (!r) { clear($('placement')); box.textContent = isUnimplemented(err) ? 'not served by this cluster' : errorText(err); return; }
  box.classList.remove('muted');
  renderPlacement(r);
  box.append(el('div', { class: 'muted small', text: `process ${r.process || ''}, topology ${r.topologyGeneration || 0}` }));
  for (const s of r.shards || []) {
    const shard = el('div', { class: 'shard' });
    const relay = isRelay(s);
    shard.append(el('div', { class: 'head' }, [
      el('strong', { text: `shard ${s.shard}` }), el('span', { class: 'muted', text: s.address || '' }),
      relay ? el('span', { class: 'pill relay', text: 'relay' }) : el('span', { text: s.layout || '' }),
      relay ? el('span', { class: 'muted', text: 'a relay coordinator: candidates and cutoffs forwarded from its children; layout is theirs' }) : el('span', { text: `${fmtNum(s.liveRows)} live / ${fmtNum(s.rows)} rows, ${fmtNum(s.tombstones)} tombstones, tail ${fmtNum(s.tailRows)}` }),
      s.hasPlacement ? el('span', { class: 'team-a', text: `placement ${codePath(s.placement)}` }) : null,
      s.placementMixed ? el('span', { class: 'pill warn', text: 'mixed codes: mid-migration' }) : null,
      s.partitionKey ? el('span', { class: 'team-a', text: `partitioned by ${s.partitionKey}` }) : null,
      relay ? null : el('span', { class: 'muted', text: `pruning ${s.segmentPruning ? 'on' : 'off'}, floors ${s.floorSharing ? 'on' : 'off'}` }),
    ]));
    if (relay) { box.append(shard); continue; }
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

// ---------------------------------------------------------------------------
// The placement dry run
// ---------------------------------------------------------------------------

// The shard map's `[placement]` shape (docs/placement.md) as PlacementTree
// JSON. A `[[placement.nodes]]` header opens a root node, each further
// `.children` opens a child of the last node one level up; a node with no
// `cel` is its level's default. JSON input is passed through.
export function parseTree(text) {
  const trimmed = text.trim();
  if (trimmed.startsWith('{')) return JSON.parse(trimmed);
  const tree = { column: 'placement', level_bits: 0, nodes: [] };
  let stack = []; // the open node per depth
  let current = null; // the table the next key = value belongs to
  const unquote = (v) => {
    v = v.trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) return v.slice(1, -1);
    return v;
  };
  const list = (v) => {
    v = v.trim();
    if (!v.startsWith('[')) throw new Error(`expected a list: ${v}`);
    return v.slice(1, -1).split(',').map((x) => unquote(x)).filter((x) => x.length);
  };
  for (const raw of text.split('\n')) {
    const line = raw.replace(/#.*$/, '').trim();
    if (!line) continue;
    if (line.startsWith('[[')) {
      const header = line.replace(/^\[\[/, '').replace(/\]\]$/, '').trim();
      const parts = header.split('.');
      if (parts[0] !== 'placement' || parts[1] !== 'nodes') throw new Error(`unknown table ${header}`);
      const depth = parts.slice(2).filter((x) => x === 'children').length;
      if (parts.length !== 2 + depth) throw new Error(`unknown table ${header}`);
      if (depth > stack.length) throw new Error(`${header} has no parent node`);
      const node = { name: '', cel: '', shards: 0, nodes: [], children: [] };
      stack = stack.slice(0, depth);
      (depth === 0 ? tree.nodes : stack[depth - 1].children).push(node);
      stack.push(node);
      current = node;
      continue;
    }
    if (line.startsWith('[')) {
      if (line !== '[placement]') throw new Error(`unknown table ${line}`);
      current = tree;
      stack = [];
      continue;
    }
    const eq = line.indexOf('=');
    if (eq < 0) throw new Error(`not a key = value line: ${line}`);
    const key = line.slice(0, eq).trim();
    const value = line.slice(eq + 1);
    const target = current || tree;
    if (target === tree) {
      if (key === 'column') tree.column = unquote(value);
      else if (key === 'level_bits') tree.level_bits = Number(value);
      else throw new Error(`unknown placement key ${key}`);
    } else if (key === 'name') target.name = unquote(value);
    else if (key === 'cel') target.cel = unquote(value);
    else if (key === 'shards') target.shards = Number(value);
    else if (key === 'nodes') target.nodes = list(value);
    else throw new Error(`unknown node key ${key}`);
  }
  return tree;
}

async function runPlacementPlan() {
  const out = clear($('plan-out'));
  out.classList.add('plan');
  let proposed;
  try { proposed = parseTree($('plan-tree').value); } catch (e) { out.append(el('div', { class: 'error', text: `tree: ${e.message}` })); return; }
  const request = { proposed, filter: $('plan-filter').value.trim() };
  out.textContent = 'planning…';
  let r;
  try { r = await api.rpc('SearchService', 'PlanPlacement', request); } catch (e) { clear(out).append(el('div', { class: 'error', text: errorText(e) })); return; }
  clear(out);
  out.classList.remove('muted');
  const cells = (r.cells || []).slice().sort((a, b) => Number(a.shard) - Number(b.shard) || (BigInt(a.code) < BigInt(b.code) ? -1 : 1));
  const table = el('table', {}, [el('thead', {}, [el('tr', {}, ['shard', 'leaf', 'code', 'rows', 'moving'].map((h) => el('th', { class: h === 'rows' || h === 'moving' ? 'num' : '', text: h })))])]);
  const tbody = el('tbody');
  for (const c of cells) {
    tbody.append(el('tr', {}, [
      el('td', { class: 'mono', text: c.shard }), el('td', { class: 'mono', text: c.leaf || '' }), el('td', { class: 'mono', text: `${codePath(c.code)} (${c.code})` }),
      el('td', { class: 'num', text: fmtNum(c.rows) }), el('td', { class: `num ${Number(c.movingRows) ? 'team-b' : ''}`, text: fmtNum(c.movingRows) }),
    ]));
  }
  table.append(tbody);
  out.append(table);
  out.append(el('div', { class: 'totals', text: `${fmtNum(r.rows)} rows, ${fmtNum(r.movingRows)} would move, ${fmtNum(r.defaultedRows)} take a default, topology ${r.topologyGeneration || 0}` }));
  out.append(el('details', {}, [el('summary', { class: 'muted small', text: 'request as sent' }), el('pre', { class: 'raw', text: JSON.stringify(request, null, 1) })]));
}

// ---------------------------------------------------------------------------
// The balance dry run
// ---------------------------------------------------------------------------

async function runBalancePlan() {
  const out = clear($('bal-out'));
  out.classList.add('plan');
  const request = {
    min_gain: Number($('bal-gain').value) || 0,
    max_moves: Number($('bal-moves').value) || 0,
    max_rate_age_ms: Number($('bal-age').value) || 0,
  };
  out.textContent = 'planning…';
  let r;
  try { r = await api.rpc('ClusterControl', 'PlanBalance', request); } catch (e) { clear(out).append(el('div', { class: 'error', text: errorText(e) })); return; }
  clear(out);
  out.classList.remove('muted');
  const secs = (v) => (v == null || Number(v) === 0 ? '–' : `${Number(v).toFixed(2)} s`);
  out.append(el('div', { class: 'totals', text: `slowest node ${secs(r.secondsBefore)} before, ${secs(r.secondsAfter)} after ${(r.moves || []).length} move(s); min gain ${r.minGain}, max moves ${r.maxMoves}; topology ${r.topologyGeneration || 0}, control revision ${r.controlRevision || 0}` }));
  const loads = el('table', {}, [el('thead', {}, [el('tr', {}, ['node', 'residency', 'bytes', 'rate', 'seconds', 'shards'].map((h) => el('th', { class: ['bytes', 'rate', 'seconds'].includes(h) ? 'num' : '', text: h })))])]);
  const lb = el('tbody');
  for (const l of r.loads || []) {
    lb.append(el('tr', {}, [
      el('td', { class: 'mono', text: l.nodeId }), el('td', { text: (l.residency || '').replace('NODE_RESIDENCY_', '').toLowerCase() }),
      el('td', { class: 'num', text: fmtBytes(l.bytes) }), el('td', { class: 'num', text: Number(l.scanBytesPerSecond) ? `${fmtBytes(l.scanBytesPerSecond)}/s` : 'unknown' }),
      el('td', { class: 'num', text: secs(l.seconds) }), el('td', { class: 'mono', text: (l.shards || []).join(', ') }),
    ]));
  }
  loads.append(lb);
  out.append(el('h3', { text: 'Loads' }), loads);
  const moves = el('table', {}, [el('thead', {}, [el('tr', {}, ['shard', 'from', 'to', 'bytes', 'group', 'seconds after'].map((h) => el('th', { class: h === 'bytes' || h === 'seconds after' ? 'num' : '', text: h })))])]);
  const mb = el('tbody');
  for (const m of r.moves || []) {
    mb.append(el('tr', {}, [
      el('td', { class: 'mono', text: m.shard }), el('td', { class: 'mono', text: m.fromNode }), el('td', { class: 'mono', text: m.toNode }),
      el('td', { class: 'num', text: fmtBytes(m.bytes) }), el('td', { class: 'mono', text: m.leaf || '' }), el('td', { class: 'num', text: secs(m.secondsAfter) }),
    ]));
  }
  moves.append(mb);
  out.append(el('h3', { text: (r.moves || []).length ? 'Moves' : 'No move clears the gain threshold' }), moves);
  if ((r.excluded || []).length) {
    out.append(el('h3', { text: 'Excluded' }));
    for (const x of r.excluded) out.append(el('div', { class: 'small mono', text: `${x.nodeId}: ${x.reason}` }));
  }
  out.append(el('details', {}, [el('summary', { class: 'muted small', text: 'response' }), el('pre', { class: 'raw', text: JSON.stringify(r, null, 1) })]));
}
