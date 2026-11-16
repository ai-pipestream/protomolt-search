// Keyboard shortcuts, typeahead and did-you-mean, the streaming query,
// and the A/B comparison through VariantSearch.
import { bff, progressive } from './bff.js';
import { clear, el, enums, fmtMs, fmtNum } from './common.js';
import { $, state } from './search-state.js';
import { buildRequest, queryVector } from './search-request.js';
import { hideError, page, render, run, showError } from './search-run.js';
import { fetchTexts } from './search-results.js';
import { hitNode, pageScale, renderRaw } from './search-hits.js';

// ---------------------------------------------------------------------------
// Keyboard
// ---------------------------------------------------------------------------

export function toggleKeys() {
  const hidden = $('keys-help').classList.toggle('hidden');
  $('keys').setAttribute('aria-expanded', String(!hidden));
}

export function queryKey(e) {
  const items = [...$('suggest').querySelectorAll('li')];
  const open = !$('suggest').classList.contains('hidden') && items.length;
  if (open && (e.key === 'ArrowDown' || e.key === 'ArrowUp')) {
    e.preventDefault();
    if (state.suggestIndex < 0) state.suggestIndex = e.key === 'ArrowDown' ? 0 : items.length - 1;
    else state.suggestIndex = (state.suggestIndex + (e.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
    items.forEach((li, i) => li.classList.toggle('active', i === state.suggestIndex));
  } else if (e.key === 'Enter' && !e.ctrlKey && !e.metaKey) {
    if (open && state.suggestIndex >= 0) { e.preventDefault(); return items[state.suggestIndex].choose(); }
    hideSuggest();
    run();
  } else if (e.key === 'Escape') {
    hideSuggest();
  }
}

export function pageKey(e) {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); hideSuggest(); return run(); }
  if (e.key === 'Escape') { $('keys-help').classList.add('hidden'); $('keys').setAttribute('aria-expanded', 'false'); return; }
  if (e.ctrlKey || e.metaKey || e.altKey || e.target.closest('input, textarea, select')) return;
  if (e.key === '/') { e.preventDefault(); $('q').focus(); $('q').select(); }
  else if (e.key === '?') toggleKeys();
  else if (e.key === '[') page(-1);
  else if (e.key === ']') page(1);
}

// ---------------------------------------------------------------------------
// Typeahead and did-you-mean
// ---------------------------------------------------------------------------

export async function typeahead() {
  const input = $('q');
  const text = input.value;
  const m = text.match(/(\S+)$/);
  if (!m || m[1].length < 2) return hideSuggest();
  const prefix = m[1].toLowerCase();
  try {
    const asked = state.suggestSeq;
    const r = await bff.call('Suggest', { field: 'body', prefix, limit: 8, analysis: state.config.body_spec });
    // A run, or a later keystroke's answer, closed the list meanwhile.
    if (asked !== state.suggestSeq) return;
    const list = clear($('suggest'));
    state.suggestIndex = -1;
    const suggestions = r.suggestions || [];
    if (!suggestions.length) return hideSuggest();
    for (const s of suggestions) {
      const li = el('li', {}, [el('span', { text: s.term }), el('span', { class: 'muted mono', text: `df ${fmtNum(s.df)}` })]);
      li.choose = () => { input.value = `${text.slice(0, m.index)}${s.term} `; hideSuggest(); input.focus(); };
      li.addEventListener('mousedown', li.choose);
      list.append(li);
    }
    list.classList.remove('hidden');
  } catch {
    hideSuggest();
  }
}

export function hideSuggest() {
  $('suggest').classList.add('hidden');
  state.suggestIndex = -1;
  state.suggestSeq++;
}

export async function didYouMean() {
  const box = $('dym');
  box.classList.add('hidden');
  const text = $('q').value.trim();
  if (!text || $('shape').value === 'browse') return;
  try {
    const r = await bff.call('TermSuggest', { field: 'body', text, mode: 'TERM_SUGGEST_MODE_MISSING', limit: 3, analysis: state.config.body_spec });
    const terms = (r.terms || []).filter((t) => t.candidates?.length);
    if (!terms.length) return;
    clear(box).append('Did you mean: ');
    for (const t of terms) {
      for (const c of t.candidates.slice(0, 3)) {
        box.append(el('button', {
          text: `${t.term} → ${c.term}`,
          title: `df ${c.df}, ${c.distance} edit(s)`,
          onclick: () => { $('q').value = $('q').value.replace(t.term, c.term); run(); },
        }), ' ');
      }
    }
    box.classList.remove('hidden');
  } catch {
    // The suggester is optional; a rejection leaves the box hidden.
  }
}

// ---------------------------------------------------------------------------
// The streaming query
// ---------------------------------------------------------------------------

export function phaseName(phase) {
  return String(phase || '').replace('QUERY_STREAM_PHASE_', '').toLowerCase();
}

// Each revision replaces the hit list as it arrives; the completion's
// response then renders as a unary one does.
export async function streamQuery() {
  hideError();
  state.cursor = ''; state.cursors = [];
  $('ab-panel').classList.add('hidden');
  $('results-panel').classList.remove('hidden');
  let request;
  try {
    request = await buildRequest();
  } catch (e) {
    return showError(e);
  }
  // Explain is served on the unary route only: a revision's hits carry no tree.
  delete request.explain;
  state.request = request;
  const log = clear($('stream-log'));
  log.classList.remove('hidden');
  $('stream').textContent = 'Stop';
  const started = performance.now();
  // A new stream stops the one before it (progressive.start aborts it).
  state.stream = { close: progressive.start('QueryStream', { query: request }, {
    onMessage: async (msg) => {
      if (msg.revision) {
        const r = msg.revision;
        const hits = r.hits || [];
        log.append(el('div', { text: `revision ${r.revision} ${phaseName(r.phase)}: ${hits.length} hit(s) at ${fmtMs(performance.now() - started)}` }));
        if (!hits.length) return;
        if ($('fetch-text').checked) await fetchTexts(hits.map((h) => h.docId));
        if (!state.stream) return;
        $('result-count').textContent = `revision ${r.revision} (${phaseName(r.phase)}): ${hits.length} hit(s), still settling`;
        const scale = pageScale(hits);
        const results = clear($('results'));
        for (const h of hits) results.append(hitNode(h, scale));
      } else if (msg.completion) {
        const c = msg.completion;
        log.append(el('div', { text: `completion: ${c.completed ? 'certified' : 'NOT completed'} at revision ${c.finalRevision}${c.errorMessage ? `, ${c.errorMessage}` : ''}` }));
        if (c.response) {
          c.response._wall_ms = performance.now() - started;
          state.response = c.response;
          await render(c.response);
        }
      }
    },
    onError: (err) => { showError(err); stopStream(); },
    onEnd: () => stopStream(),
  }) };
}

export function stopStream() {
  if (state.stream) { state.stream.close(); state.stream = null; }
  $('stream').textContent = 'Stream';
}

// ---------------------------------------------------------------------------
// A/B through VariantSearch
// ---------------------------------------------------------------------------

export function variantPresets() {
  const hybrid = (label, mode, extra = {}) => ({
    id: `hybrid:${mode}`, label, needsVector: true,
    build: (text, vector) => ({ hybrid: { text, vector, legs: { fusion_mode: enums.fusion[mode], ...extra } } }),
  });
  return [
    { id: 'bm25', label: 'BM25 only', needsVector: false, build: (text) => ({ bm25: { text } }) },
    hybrid('Hybrid, rank fusion', 'global_rank'),
    hybrid('Hybrid, cascade', 'cascade'),
    hybrid('Hybrid, score blend', 'score_blend'),
    hybrid('Hybrid, weighted sum', 'decomposed'),
    hybrid('Hybrid, two-level', 'two_level'),
  ];
}

export async function runAb() {
  stopStream();
  hideError();
  const text = $('q').value.trim();
  if (!text) return showError({ error: 'query text is required' });
  const presets = variantPresets();
  const a = presets.find((p) => p.id === $('ab-a').value);
  const b = presets.find((p) => p.id === $('ab-b').value);
  let vector = null;
  try {
    if (a.needsVector || b.needsVector) vector = await queryVector(text);
  } catch (e) {
    return showError(e);
  }
  const k = Number($('k').value || 10);
  const request = {
    k,
    interleave: true,
    variants: [
      { label: 'A', ...a.build(text, vector) },
      { label: 'B', ...b.build(text, vector) },
    ],
  };
  const filter = $('filter').value.trim();
  if (filter) for (const v of request.variants) { (v.bm25 || v.hybrid).filter = filter; }
  state.request = request;
  $('run-ab').disabled = true;
  try {
    const response = await bff.call('VariantSearch', request);
    state.response = response;
    state.tab = 'response';
    renderRaw();
    await renderAb(response, a, b);
  } catch (e) {
    showError(e);
  } finally {
    $('run-ab').disabled = false;
  }
}

export async function renderAb(response, a, b) {
  const panel = clear($('ab-panel'));
  // The comparison stands in for the hit list, which belongs to the
  // last Query and would read as part of it.
  $('results-panel').classList.add('hidden');
  panel.classList.remove('hidden');
  const results = response.results || [];
  if ($('fetch-text').checked) {
    await fetchTexts(results.flatMap((r) => (r.hits || []).map((h) => h.docId)).concat(response.interleaving?.docIds || []));
  }
  const cols = el('div', { class: 'ab' });
  const label = { A: a.label, B: b.label };
  for (const r of results) {
    const col = el('div', { class: 'col' }, [el('h3', { text: `${r.label}: ${label[r.label] || ''}`, class: r.label === 'A' ? 'team-a' : 'team-b' }), el('div', { class: 'muted small', text: fmtMs(r.elapsedMs) })]);
    const other = results.find((o) => o.label !== r.label);
    (r.hits || []).forEach((h, i) => {
      const rankThere = other?.hits?.findIndex((o) => o.docId === h.docId) ?? -1;
      const move = rankThere < 0 ? 'only here' : rankThere === i ? '=' : rankThere > i ? `▲${rankThere - i}` : `▼${i - rankThere}`;
      const doc = state.texts.get(String(h.docId));
      col.append(el('div', { class: 'hit' }, [
        el('div', { class: 'head' }, [el('span', { class: 'rank', text: `#${i + 1}` }), el('span', { class: 'score', text: Number(h.score).toFixed(4) }), el('span', { class: 'id', text: `doc ${h.docId}` }), el('span', { class: 'muted', text: move })]),
        doc?.text ? el('div', { class: 'text small', text: doc.text.slice(0, 240) }) : null,
      ]));
    });
    cols.append(col);
  }
  panel.append(el('h2', { text: 'A/B' }), cols);
  for (const d of response.diffs || []) {
    panel.append(el('div', { class: 'profile' }, [
      `${d.reference} vs ${d.variant} at depth ${d.depth}:`,
      `overlap ${d.overlap} (${(Number(d.overlapFraction) * 100).toFixed(0)}%)`,
      `kendall τ ${fmtNum(d.kendallTau)}`, `rbo ${fmtNum(d.rbo)}`,
      `score regret ${fmtNum(d.scoreRegret)}`, d.top1Flipped ? 'top-1 flipped' : 'top-1 same',
    ].map((text) => el('span', { text }))));
  }
  if (response.interleaving) {
    const il = response.interleaving;
    const list = el('div');
    (il.docIds || []).forEach((id, i) => {
      const team = (il.teams || [])[i] || '';
      const cls = team.endsWith('_A') ? 'team-a' : team.endsWith('_B') ? 'team-b' : '';
      const doc = state.texts.get(String(id));
      list.append(el('div', { class: 'hit' }, [
        el('div', { class: 'head' }, [el('span', { class: 'rank', text: `#${i + 1}` }), el('span', { class: cls, text: team.replace('INTERLEAVE_TEAM_', 'team ') }), el('span', { class: 'id', text: `doc ${id}` })]),
        doc?.text ? el('div', { class: 'text small', text: doc.text.slice(0, 200) }) : null,
      ]));
    });
    panel.append(el('h3', { text: `Interleaved (seed ${il.seed})` }), el('div', { class: 'note', text: 'Team draft interleaving: each position was drawn from A or B; a click on a document credits its team.' }), list);
  }
}
