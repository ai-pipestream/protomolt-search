// The dashboard's cluster views: the runtime knobs, the shard map, the
// recent queries, and the placement and balance dry runs.
import { api, clear, el, errorText, fmtBytes, fmtMs, fmtNum, isUnimplemented } from './common.js';
import { $, state } from './dashboard-state.js';

// ---------------------------------------------------------------------------
// Knobs
// ---------------------------------------------------------------------------

export function renderKnobs(r, err) {
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
// names itself in the refusal. The children
// behind it are its own shard map, which the coordinator's diagnostics
// do not see.
export function isRelay(s) {
  return /relay/i.test(s.layout || '') && !(s.segments || []).length;
}

// The placement code as its path: one index per level, root first, in
// the tree's fixed field width. The width
// is the default unless the map says otherwise; the raw code is shown
// beside it so nothing is lost when the width differs.
export function codePath(code, levelBits = 9n) {
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

export function renderPlacement(r) {
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

export function renderShards(r, err) {
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

export function segmentInfo(g) {
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

export function renderRecent(r, err) {
  const box = clear($('recent'));
  if (!r) { box.textContent = isUnimplemented(err) ? 'not served by this cluster' : errorText(err); return; }
  box.classList.remove('muted');
  box.append(el('div', { class: 'muted small', text: `${fmtNum(r.totalSeen)} seen since start` }));
  if (!r.queries?.length) {
    box.append(el('div', { class: 'muted small', text: 'No public request has reached this coordinator yet. Run one from the search page.' }));
    return;
  }
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

// The shard map's `[placement]` shape as PlacementTree
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

export async function runPlacementPlan() {
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

export async function runBalancePlan() {
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
