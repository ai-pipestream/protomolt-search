// The search form as a value: reading and writing it, the URL state, and
// the history.
import { clear, el, fmtMs, fmtNum } from './common.js';
import { decodeState } from './query-state.js';
import { $, FORM_IDS, HISTORY_KEY, HISTORY_MAX, state } from './search-state.js';
import { addAgg, addClause, addDim, addStage, syncShape } from './search-request.js';
import { renderChips, run } from './search-run.js';

// ---------------------------------------------------------------------------
// The form as a value: the URL state and the history restore from it
// ---------------------------------------------------------------------------

export function readForm() {
  const form = {};
  for (const id of FORM_IDS) {
    const node = $(id);
    form[id] = node.type === 'checkbox' ? node.checked : node.value;
  }
  form.clauses = [...$('clauses').querySelectorAll('.clause')].map((row) => ({
    kind: row.querySelector('.kind').value,
    leaf: row.querySelector('.leaf').value,
    value: row.querySelector('.value').value,
  }));
  form.aggs = [...$('aggs').querySelectorAll('.agg')].map((row) => ({
    name: row.querySelector('.name').value,
    expression: row.querySelector('.expr').value,
    op: row.querySelector('.op').value,
  }));
  form.stages = [...$('stages').querySelectorAll('.stage')].map((row) => ({
    op: row.querySelector('.op').value,
    column: row.querySelector('.column').value,
    weight: row.querySelector('.weight').value,
    origin: row.querySelector('.origin').value,
    scale: row.querySelector('.scale').value,
  }));
  form.dims = [...$('dims').querySelectorAll('.stage')].map((row) => ({
    id: row.querySelector('.dim-id').value,
    source: row.querySelector('.source').value,
    weight: row.querySelector('.weight').value,
    normalization: row.querySelector('.normalization').value,
    missing: row.querySelector('.missing').value,
  }));
  return form;
}

export function writeForm(form) {
  for (const id of FORM_IDS) {
    const node = $(id);
    if (node.type === 'checkbox') node.checked = !!form[id];
    else node.value = form[id];
  }
  clear($('clauses'));
  for (const c of form.clauses) addClause(c.kind, c.leaf, c.value);
  clear($('aggs'));
  for (const a of form.aggs) addAgg(a.name, a.expression, a.op);
  clear($('stages'));
  for (const st of form.stages) addStage(st);
  clear($('dims'));
  for (const d of form.dims) addDim(d);
  syncShape();
  renderChips();
}

export function currentHash() { return location.hash.replace(/^#/, ''); }

// The URL fragment is the form: restore it, and run when it names a query.
export function restoreFromUrl() {
  const hash = currentHash();
  writeForm(decodeState(hash, state.defaults));
  if (hash) run({ push: false });
}

export function loadHistory() {
  try {
    const list = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
    return Array.isArray(list) ? list : [];
  } catch {
    return [];
  }
}

export function saveHistory(list) {
  try { localStorage.setItem(HISTORY_KEY, JSON.stringify(list)); } catch { /* storage is a convenience */ }
}

export function remember(hash, response) {
  if (!hash) return;
  const form = readForm();
  const entry = {
    hash,
    text: form.q,
    shape: form.shape,
    filter: form.filter,
    hits: (response.groups?.length ? response.groups.flatMap((g) => g.hits || []) : response.hits || []).length,
    ms: response._wall_ms,
    at: Date.now(),
  };
  saveHistory([entry, ...loadHistory().filter((h) => h.hash !== hash)].slice(0, HISTORY_MAX));
  renderHistory();
}

export function renderHistory() {
  const list = loadHistory();
  const box = clear($('history'));
  $('clear-history').classList.toggle('hidden', !list.length);
  if (!list.length) { box.textContent = 'Queries you run are listed here.'; return; }
  for (const h of list) {
    box.append(el('a', { class: 'past', href: `#${h.hash}`, title: new Date(h.at).toLocaleString() }, [
      el('span', { class: 'what' }, [
        el('span', { class: 'q', text: h.text || '(no text)' }),
        h.filter ? el('span', { class: 'mono muted', text: ` ${h.filter}` }) : null,
      ]),
      el('span', { class: 'mono muted', text: `${h.shape} · ${fmtNum(h.hits)} · ${fmtMs(h.ms)}` }),
    ]));
  }
}
