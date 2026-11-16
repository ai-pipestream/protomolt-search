// The dashboard's metrics stream: snapshots, per-route rates, and the
// route latency distributions.
import { api, clear, el, errorText, fmtBytes, fmtMs, fmtNum, isUnimplemented } from './common.js';
import { Sparkline, histogramDelta, histogramQuantile } from './sparkline.js';
import { $, WINDOW, state } from './dashboard-state.js';
import { noteUnserved, setTile } from './dashboard-poll.js';

// ---------------------------------------------------------------------------
// The metrics stream
// ---------------------------------------------------------------------------

export function openStream() {
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

export function labelsOf(sample) {
  const out = {};
  for (const l of sample.labels || []) out[l.name] = l.value;
  return out;
}

export function parseSnapshot(raw) {
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

export function ingest(raw) {
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
  // The scan budget's counters: bytes the
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

export function isDuration(name) { return name.includes('request_duration_seconds'); }

export function mergeHistograms(snap, oldest, filter) {
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

export function renderRoutes(snap, prev, oldest, dt, windowDt) {
  const routes = new Map();
  for (const c of snap.counters.values()) {
    if (c.name === 'turbovec_requests_total' && c.labels.rpc) routes.set(c.labels.rpc, { rpc: c.labels.rpc, total: c.value, errors: 0 });
  }
  for (const c of snap.counters.values()) {
    if (c.name === 'turbovec_request_errors_total' && routes.has(c.labels.rpc)) routes.get(c.labels.rpc).errors += c.value;
  }
  const tbody = $('routes').querySelector('tbody');
  const rows = [...routes.values()];
  for (const r of rows) {
    let tr = tbody.querySelector(`tr[data-rpc="${r.rpc}"]`);
    if (!tr) {
      const canvas = el('canvas', { class: 'spark', style: 'height:18px;width:120px' });
      tr = el('tr', { 'data-rpc': r.rpc, title: 'click for the latency distribution', onclick: () => { state.routeOpen = state.routeOpen === r.rpc ? null : r.rpc; renderRouteLatency(); } }, [
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
    state.routeHists.set(r.rpc, h);
    tr.querySelector('.rate').textContent = rate.toFixed(2);
    tr.querySelector('.p50').textContent = p50 == null ? '–' : fmtMs(p50 * 1000);
    tr.querySelector('.p99').textContent = p99 == null ? '–' : fmtMs(p99 * 1000);
    tr.querySelector('.err').textContent = fmtNum(r.errors);
    state.routeSparks.get(r.rpc).push(rate);
    tr.classList.toggle('dim', r.total === before && !rate);
    // A route is active once it has served a request while the page
    // watched, or carries an error; the rest are the registry's labels.
    if (rate > 0 || r.errors > 0) state.activeRoutes.add(r.rpc);
    Object.assign(tr.dataset, { rate, errors: r.errors, p50: p50 ?? -1, p99: p99 ?? -1 });
  }
  void windowDt;
  arrangeRoutes();
  renderRouteLatency();
}

// The open route's latency histogram over the window: one column per
// bucket between the first and the last that hold a request, with the
// buckets p50 and p99 fall in named. Two humps are a cold and a warm path.
export function renderRouteLatency() {
  const box = clear($('route-latency'));
  const rpc = state.routeOpen;
  for (const tr of $('routes').querySelectorAll('tbody tr')) tr.classList.toggle('open', tr.dataset.rpc === rpc);
  box.classList.toggle('hidden', !rpc);
  if (!rpc) return;
  const h = state.routeHists.get(rpc);
  const buckets = [...(h?.buckets || [])].map((b) => ({ le: Number(b.le), cumulative: Number(b.cumulativeCount) })).sort((a, b) => a.le - b.le);
  const columns = buckets.map((b, i) => ({ le: b.le, from: i ? buckets[i - 1].le : 0, count: b.cumulative - (i ? buckets[i - 1].cumulative : 0) }));
  const first = columns.findIndex((c) => c.count > 0);
  const head = el('div', { class: 'head' }, [el('span', { class: 'mono', text: rpc }), ' ']);
  box.append(head);
  if (first < 0) {
    head.append(el('span', { class: 'muted', text: 'no request in the window' }));
    return;
  }
  const shown = columns.slice(first, columns.findLastIndex((c) => c.count > 0) + 1);
  const total = shown.reduce((a, c) => a + c.count, 0);
  const p50 = histogramQuantile(h.buckets, 0.5);
  const p99 = histogramQuantile(h.buckets, 0.99);
  head.append(el('span', { class: 'muted', text: `${fmtNum(total)} request(s) in the window · p50 ${fmtMs(p50 * 1000)} · p99 ${fmtMs(p99 * 1000)}` }));
  const top = Math.max(...shown.map((c) => c.count));
  const edge = (seconds) => (Number.isFinite(seconds) ? fmtMs(seconds * 1000) : 'over');
  const holds = (c, q) => q > c.from && q <= c.le;
  const chart = el('div', { class: 'columns', role: 'img', 'aria-label': `latency histogram of ${rpc}: ${shown.map((c) => `${c.count} up to ${edge(c.le)}`).join(', ')}` });
  shown.forEach((c, i) => {
    const mark = [holds(c, p50) && 'p50', holds(c, p99) && 'p99'].filter(Boolean).join(' · ');
    chart.append(el('div', { class: 'col', title: `${fmtNum(c.count)} request(s) from ${edge(c.from)} to ${edge(c.le)}` }, [
      el('span', { class: 'mono count', text: c.count ? fmtNum(c.count) : '' }),
      el('i', { style: `height:${(100 * c.count) / top}%` }),
      el('span', { class: 'mono le', text: i === 0 || i === shown.length - 1 || mark ? `≤ ${edge(c.le)}` : '' }),
      el('span', { class: 'mono mark', text: mark }),
    ]));
  });
  box.append(chart);
}

// Order the rows by the chosen column (ties by name) and hide the
// routes that have done nothing unless all are asked for.
export function arrangeRoutes() {
  const tbody = $('routes').querySelector('tbody');
  const rows = [...tbody.querySelectorAll('tr')];
  const key = state.routeSort;
  const value = (tr) => (key === 'rpc' ? 0 : Number(tr.dataset[key]));
  rows.sort((a, b) => value(b) - value(a) || a.dataset.rpc.localeCompare(b.dataset.rpc));
  let idle = 0;
  for (const tr of rows) {
    const active = state.activeRoutes.has(tr.dataset.rpc);
    if (!active) idle++;
    tr.classList.toggle('hidden', !active && !state.allRoutes);
    tbody.append(tr);
  }
  for (const th of $('routes').querySelectorAll('th[data-sort]')) th.classList.toggle('sorted', th.dataset.sort === key);
  const toggle = $('routes-all');
  toggle.classList.toggle('hidden', !idle);
  toggle.textContent = state.allRoutes ? `hide ${idle} idle` : `show ${idle} idle`;
  $('routes-empty').classList.toggle('hidden', state.allRoutes || idle < rows.length);
}
