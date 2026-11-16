// One page of hits, explain trees, the profile, aggregations, and the
// raw request and response tabs.
import { clear, copyText, el, fmtMs, fmtNum, grpcurlFor } from './common.js';
import {
  calendarUpper, celString, isIdentifier, rangeClause, rangeColumn, replaceRange, splitConjuncts,
} from './query-state.js';
import { $, HANDOFF_KEY, state } from './search-state.js';
import { drill, drillTitle, page, setFilter } from './search-run.js';
import { moveNode } from './search-results.js';

// What the bars on a page of hits are drawn against: the top score, and
// per signal id the largest value on the page.
export function pageScale(hits) {
  const scale = { score: 0, signals: new Map(), legs: 0, dimension: 0, moves: new Map() };
  for (const h of hits) {
    scale.score = Math.max(scale.score, Number(h.score ?? 0));
    scale.legs = Math.max(scale.legs, (h.signals || []).length);
    for (const s of h.signals || []) scale.signals.set(s.id, Math.max(scale.signals.get(s.id) ?? 0, Number(s.score)));
    for (const d of h.dimensions || []) scale.dimension = Math.max(scale.dimension, Math.abs(Number(d.normalized)));
  }
  return scale;
}

export function share(value, max) {
  return max > 0 ? Math.max(0, Math.min(100, (100 * Number(value)) / max)) : 0;
}

export function projectedValue(p) {
  return p.stringValue ?? p.uintValue ?? p.intValue ?? p.doubleValue ?? p.boolValue;
}

export function hitNode(h, scale) {
  const doc = state.texts.get(String(h.docId));
  const node = el('div', { class: 'hit' });
  node.append(el('div', { class: 'head' }, [
    el('span', { class: 'rank', text: `#${h.rank ?? ''}` }),
    el('span', { class: 'score', text: Number(h.score ?? 0).toFixed(6) }),
    scale.score > 0 ? el('span', { class: 'scorebar', title: `${share(h.score, scale.score).toFixed(0)}% of the top score on this page` }, [el('span', { style: `width:${share(h.score, scale.score)}%` })]) : null,
    el('button', { class: 'id quiet', text: `doc ${h.docId}`, title: 'copy the document id', onclick: () => copyText(String(h.docId)) }),
    moveNode(scale.moves.get(h)),
    doc?.lineage ? el('span', { class: 'muted', text: `parent ${doc.lineage.parent_id} group ${doc.lineage.group_id}` }) : null,
    h.sortValues?.length ? el('span', { class: 'muted', text: `sort ${h.sortValues.map((v) => v.text ?? v.integer ?? v.number).join(' | ')}` }) : null,
  ]));

  // The projected values carry no name: the request's projection at the
  // same index names each.
  const projections = state.request?.projections || [];
  if (h.projected?.length) {
    const columns = el('div', { class: 'columns' });
    h.projected.forEach((p, i) => {
      const name = projections[i]?.name ?? `#${i}`;
      const value = projectedValue(p);
      const shown = value == null ? '–' : p.intValue != null ? epochLabel(p.intValue) : String(value);
      const facet = p.stringValue != null && isIdentifier(name);
      columns.append(el(facet ? 'button' : 'span', {
        class: 'chip',
        title: facet ? drillTitle(name, value) : value == null ? 'no value' : String(value),
        onclick: facet ? (e) => drill(name, value, e.shiftKey) : null,
      }, [el('span', { class: 'muted', text: `${name} ` }), el('span', { class: 'mono', text: shown })]));
    });
    node.append(columns);
  }

  if (h.snippets?.length) {
    for (const s of h.snippets) node.append(el('div', { class: 'text' }, snippetNodes(s)));
  } else if (doc?.text) {
    node.append(textNode(doc.text));
  } else if (doc?.error) {
    node.append(el('div', { class: 'muted small', text: `text unavailable: ${doc.error}` }));
  }

  // One signal is the score again; two or more are the legs, each drawn
  // against the largest value that leg has on this page.
  if (scale.legs > 1 && h.signals?.length) {
    const legs = el('div', { class: 'legs' });
    for (const s of h.signals) {
      legs.append(el('div', { class: 'leg', title: `${s.id} ${s.score}: ${share(s.score, scale.signals.get(s.id)).toFixed(0)}% of this leg's best on the page` }, [
        el('span', { class: 'mono muted', text: s.id }),
        el('span', { class: 'track' }, [el('span', { style: `width:${share(s.score, scale.signals.get(s.id))}%` })]),
        el('span', { class: 'mono', text: Number(s.score).toFixed(4) }),
      ]));
    }
    node.append(legs);
  }
  if (h.dimensions?.length) {
    // One row per scorer dimension: the normalized value as a bar
    // against the largest on the page, then what it added to the score.
    const dims = el('div', { class: 'legs dims' });
    for (const d of h.dimensions) {
      const text = d.skipped ? 'skipped' : `${d.raw == null ? 'missing' : `raw ${fmtNum(d.raw)}`} · norm ${fmtNum(d.normalized)} → ${fmtNum(d.contribution)}`;
      dims.append(el('div', { class: `leg${d.skipped ? ' skipped' : ''}`, title: `dimension ${d.id}: ${text}` }, [
        el('span', { class: 'mono muted', text: d.id }),
        el('span', { class: 'track' }, [el('span', { style: `width:${d.skipped ? 0 : share(Math.abs(Number(d.normalized)), scale.dimension)}%` })]),
        el('span', { class: 'mono', text }),
      ]));
    }
    node.append(dims);
  }
  const meta = el('div', { class: 'meta signals' });
  if (h.matched?.length) meta.append(el('span', { text: `matched ${h.matched.join(',')}` }));
  if (meta.firstChild) node.append(meta);
  if (h.explain) {
    const drawer = el('details', { class: 'tree' }, [el('summary', { class: 'muted small', text: 'explain' })]);
    drawer.append(el('ul', {}, [explainNode(h.explain)]));
    node.append(drawer);
  }
  return node;
}

// Stored text, clamped to a few lines with a toggle when it is long.
export function textNode(text) {
  const long = text.length > 420 || text.split('\n').length > 5;
  const body = el('div', { class: long ? 'text clamp' : 'text', text });
  if (!long) return body;
  const toggle = el('button', { class: 'quiet small', text: 'more', onclick: () => { toggle.textContent = body.classList.toggle('clamp') ? 'more' : 'less'; } });
  return el('div', {}, [body, toggle]);
}

export function snippetNodes(s) {
  const text = s.text || '';
  const spans = (s.highlights || []).map((x) => [Number(x.start ?? 0), Number(x.end ?? 0)]).sort((a, b) => a[0] - b[0]);
  const out = [];
  let pos = 0;
  for (const [start, end] of spans) {
    if (start < pos || end > text.length) continue;
    out.push(text.slice(pos, start), el('mark', { text: text.slice(start, end) }));
    pos = end;
  }
  out.push(text.slice(pos));
  if (s.field) out.unshift(el('span', { class: 'muted small', text: `${s.field}: ` }));
  return out;
}

export function explainNode(node) {
  const details = (node.details || []);
  const label = [el('span', { class: 'v', text: fmtNum(node.value) }), node.description || ''];
  if (!details.length) return el('li', {}, label);
  const d = el('details', { open: true }, [el('summary', {}, label), el('ul', {}, details.map(explainNode))]);
  return el('li', {}, [d]);
}

// The phases of QueryProfile in the order they run, each with its slot
// in the categorical palette (app.css): the slot follows the phase.
export const PHASES = [
  ['selection', 'selectionMs'], ['boost', 'boostMs'], ['values', 'valuesMs'], ['scorer', 'scorerMs'],
  ['projection', 'projectionMs'], ['rerank', 'rerankMs'], ['collapse', 'collapseMs'],
];

export function renderProfile(response) {
  const line = clear($('profile-line'));
  const bar = clear($('phases'));
  const p = response.profile;
  line.append(el('span', { text: `wall ${fmtMs(response._wall_ms)}` }));
  bar.classList.toggle('hidden', !p);
  if (!p) return;
  line.append(el('span', { text: `total ${fmtMs(p.totalMs)}` }));
  if (p.shardsTotal) line.append(el('span', { text: `shards ${p.shardsSkipped ?? 0}/${p.shardsTotal} skipped` }));
  if (p.segmentsTotal) line.append(el('span', { text: `segments ${p.segmentsSkipped ?? 0}/${p.segmentsTotal} skipped` }));
  if (Number(p.rerankRows)) line.append(el('span', { text: `rerank ${fmtNum(p.rerankRows)} rows, ${fmtNum(p.rerankPages)} pages` }));
  if (response.servedTopologyGeneration) line.append(el('span', { text: `topology ${response.servedTopologyGeneration}` }));

  const parts = PHASES.map(([name, key], i) => ({ name, ms: Number(p[key] || 0), slot: i + 1 })).filter((x) => x.ms > 0);
  const total = Number(p.totalMs || 0);
  const rest = total - parts.reduce((a, x) => a + x.ms, 0);
  if (rest > 0.0005) parts.push({ name: 'fan-out and merge', ms: rest, slot: 0 });
  const sum = parts.reduce((a, x) => a + x.ms, 0);
  if (!sum) { bar.classList.add('hidden'); return; }
  const track = el('div', { class: 'stack', role: 'img', 'aria-label': `phase timings: ${parts.map((x) => `${x.name} ${fmtMs(x.ms)}`).join(', ')}` });
  const legend = el('div', { class: 'legend' });
  for (const x of parts) {
    const title = `${x.name} ${fmtMs(x.ms)} (${((100 * x.ms) / sum).toFixed(0)}%)`;
    track.append(el('span', { class: `seg s${x.slot}`, style: `flex-grow:${x.ms}`, title }));
    legend.append(el('span', { title }, [el('i', { class: `swatch s${x.slot}` }), `${x.name} `, el('span', { class: 'mono', text: fmtMs(x.ms) })]));
  }
  bar.append(track, legend);
}

export function foldValue(x) {
  return x.uintValue ?? x.intValue ?? x.doubleValue ?? '–';
}

export function renderAggregate(agg) {
  const out = clear($('agg-out'));
  if (!agg) { out.textContent = 'Add a group-by, a fold, a histogram, or percentiles.'; return; }
  const spec = state.request?.aggregate ?? state.request?.selection?.boolean?.aggregate ?? {};
  out.classList.remove('muted');
  out.append(el('div', { class: 'small mono muted', text: `matched ${fmtNum(agg.matched)}${Number(agg.ungrouped) ? `, ungrouped ${fmtNum(agg.ungrouped)}` : ''}` }));
  const folds = (r) => (r || []).filter((x) => x.name !== 'count').map((x) => `${x.name} = ${fmtNum(foldValue(x))} (${fmtNum(x.present)} present)`);
  for (const line of folds(agg.results)) out.append(el('div', { class: 'small', text: line }));

  if (agg.groups?.length) {
    const column = spec.group_by;
    const drillable = isIdentifier(column);
    const active = splitConjuncts($('filter').value);
    const max = Math.max(1, ...agg.groups.map((g) => Number(g.matched)));
    const list = el('div', { class: 'facets' });
    for (const g of agg.groups) {
      const on = active.includes(`${column} == ${celString(g.value)}`);
      list.append(el(drillable ? 'button' : 'div', {
        class: `facet${on ? ' on' : ''}`,
        title: drillable ? drillTitle(column, g.value) : null,
        onclick: drillable ? (e) => drill(column, g.value, e.shiftKey) : null,
      }, [
        el('span', { class: 'name' }, [g.value, folds(g.results).length ? el('span', { class: 'muted small', text: ` ${folds(g.results).join('; ')}` }) : null]),
        el('span', { class: 'count', text: fmtNum(g.matched) }),
        el('span', { class: 'bar', style: `width:${(100 * Number(g.matched)) / max}%` }),
      ]));
    }
    out.append(el('h3', { text: column }), list);
  }
  (agg.histograms || []).forEach((h, i) => out.append(histogramNode(h, spec.histograms?.[i])));
  for (const p of agg.percentiles || []) {
    const table = el('table', {}, [el('tbody', {}, (p.values || []).map((v) => el('tr', {}, [
      el('td', { text: `p${v.percentile}` }), el('td', { class: 'num', text: fmtNum(foldValue(v)) }),
    ])))]);
    out.append(el('h3', { text: p.name.replace(/^pct:/, '') }), el('div', { class: 'small muted', text: `${fmtNum(p.present)} present` }), table);
  }
}

// One histogram as a column chart. A fixed histogram's buckets are keyed
// by `lower` and the gaps between them are empty buckets; a calendar
// one's by `lowerInt`, epoch microseconds, drawn as returned.
export function histogramNode(h, spec) {
  const expression = spec?.expression ?? h.name.replace(/^hist:/, '');
  const calendar = spec?.calendar;
  const interval = Number(spec?.interval || 0);
  let buckets = (h.buckets || []).map((b) => ({ lower: calendar ? Number(b.lowerInt) : Number(b.lower), count: Number(b.count) }));
  if (!calendar && interval > 0 && buckets.length > 1) {
    const counts = new Map(buckets.map((b) => [b.lower, b.count]));
    const first = buckets[0].lower, last = buckets[buckets.length - 1].lower;
    const steps = Math.round((last - first) / interval);
    if (steps < 400) buckets = Array.from({ length: steps + 1 }, (_, i) => ({ lower: first + i * interval, count: counts.get(first + i * interval) ?? 0 }));
  }
  // Bucket bounds print without digit grouping: 1990 is as likely a year as a count.
  const label = (lower) => (calendar ? epochLabel(lower) : String(Number(lower.toPrecision(12))));
  const upper = (lower) => (calendar ? calendarUpper(lower, calendar) : lower + interval);
  const target = rangeColumn(expression, calendar);
  const clause = (lower) => (target ? rangeClause(target, lower, upper(lower)) : null);
  const active = splitConjuncts($('filter').value);
  const max = Math.max(1, ...buckets.map((b) => b.count));
  const idle = `${fmtNum(h.present)} present${Number(h.unbucketable) ? `, ${fmtNum(h.unbucketable)} unbucketable` : ''}`;
  const readout = el('div', { class: 'small muted mono', text: idle });
  const plot = el('div', { class: 'columns-chart' });
  for (const b of buckets) {
    const c = clause(b.lower);
    const text = `${label(b.lower)} to ${label(upper(b.lower))}: ${fmtNum(b.count)}`;
    plot.append(el(c ? 'button' : 'span', {
      class: `col${c && active.includes(c) ? ' on' : ''}`,
      'aria-label': text,
      onmouseenter: () => { readout.textContent = text; },
      onfocus: () => { readout.textContent = text; },
      onmouseleave: () => { readout.textContent = idle; },
      title: c ? 'click: only this range; click again to undo' : null,
      onclick: c ? () => setFilter(replaceRange($('filter').value, target.column, c)) : null,
    }, [el('span', { style: `height:${b.count ? Math.max(3, (100 * b.count) / max) : 0}%` })]));
  }
  const axis = buckets.length ? el('div', { class: 'axis small muted mono' }, [el('span', { text: label(buckets[0].lower) }), el('span', { text: label(upper(buckets[buckets.length - 1].lower)) })]) : null;
  return el('div', { class: 'histogram' }, [el('h3', { text: expression }), plot, axis, readout]);
}

export function epochLabel(micros) {
  const n = Number(micros);
  if (!Number.isFinite(n) || Math.abs(n) < 1e14) return String(micros);
  return new Date(n / 1000).toISOString().slice(0, 10);
}

export function renderSynonyms(list) {
  const out = clear($('synonyms-out'));
  if (!list?.length) { out.textContent = 'none reported'; return; }
  for (const s of list) {
    out.append(el('div', { class: 'small mono' }, [
      el('span', { class: 'muted', text: s.field ? `${s.field}: ` : '' }), `${s.term} → ${(s.terms || []).join(', ')}`,
    ]));
  }
}

// The dense outcome messages as name and value rows; unset strings are left out.
export function renderDense(response) {
  const out = clear($('dense-out'));
  const parts = [response.denseExecution, response.denseQuality].filter(Boolean);
  if (!parts.length) { out.textContent = 'no dense leaf'; return; }
  const list = el('dl', { class: 'kv' });
  for (const part of parts) {
    for (const [name, value] of Object.entries(part)) {
      if (value === '' || value == null) continue;
      const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
      list.append(el('dt', { text: name }), el('dd', { class: 'mono', text }));
    }
  }
  out.append(list);
}

export function renderRaw() {
  const pre = $('raw');
  for (const b of document.querySelectorAll('.tabs button')) b.classList.toggle('active', b.dataset.tab === state.tab);
  // The request tab links to the API explorer with this request as the
  // body: in the fragment when it fits a URL, through storage otherwise.
  const link = $('open-explorer');
  link.classList.toggle('hidden', state.tab !== 'request' || !state.request);
  if (state.request) {
    const body = JSON.stringify(state.request);
    const fits = body.length <= 4000;
    link.href = `/explorer#${new URLSearchParams(fits ? { m: 'SearchService.Query', b: body } : { m: 'SearchService.Query', from: 'search' })}`;
    link.onclick = fits ? null : () => { try { localStorage.setItem(HANDOFF_KEY, body); } catch { /* the explorer opens with an empty body */ } };
  }
  if (state.tab === 'request') pre.textContent = JSON.stringify(state.request, null, 2);
  else if (state.tab === 'grpcurl') pre.textContent = state.request ? grpcurlFor(state.config, 'SearchService', 'Query', state.request) : '';
  else {
    const r = state.response ? { ...state.response } : null;
    if (r) delete r._wall_ms;
    pre.textContent = JSON.stringify(r, null, 2);
  }
}
