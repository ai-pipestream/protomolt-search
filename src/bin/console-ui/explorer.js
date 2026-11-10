// The API explorer: every method the facade exposes, its request and
// response shapes from /api/describe, a JSON editor that a click on a
// field fills in, and the call itself through /api/rpc or /api/stream.
import {
  api, el, clear, fmtMs, loadConfig, grpcurlFor, mountHeader, copyText, errorText,
} from '/common.js';
import { defaultValue, insertAt } from '/query-state.js';

const $ = (id) => document.getElementById(id);
// A stream shows its newest messages first, as many as fit this many
// characters, so a long stream of large messages costs the same to draw.
const STREAM_CHARS = 200_000;
const HANDOFF_KEY = 'pipestream.console.explorer-body';
const state = {
  config: null,
  described: null, // the /api/describe answer for the chosen method
  shape: 'request',
  tab: 'response',
  response: '',
  stream: null,
};

mountHeader('api');
init();

async function init() {
  try {
    state.config = await loadConfig();
  } catch (e) {
    showError(e);
    state.config = { methods: [], nodes: [] };
  }
  (state.config.nodes || []).forEach((addr, i) => $('target').append(el('option', { value: `node${i}`, text: `node${i} ${addr}` })));
  $('method-filter').addEventListener('input', renderMethods);
  $('run').addEventListener('click', () => (state.stream ? stop() : run()));
  $('body').addEventListener('input', checkBody);
  $('body').addEventListener('keydown', (e) => { if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); run(); } });
  $('format').addEventListener('click', () => { const v = parsedBody(); if (v !== undefined) setBody(v); });
  $('reset').addEventListener('click', () => setBody({}));
  $('copy-raw').addEventListener('click', () => copyText($('raw').textContent));
  for (const b of document.querySelectorAll('.tabs button[data-tab]')) b.addEventListener('click', () => { state.tab = b.dataset.tab; renderRaw(); });
  for (const b of document.querySelectorAll('.tabs button[data-shape]')) b.addEventListener('click', () => { state.shape = b.dataset.shape; renderShape(); });
  window.addEventListener('popstate', restore);
  renderMethods();
  restore();
}

// The fragment names the method, the process and the request body.
function restore() {
  const params = new URLSearchParams(location.hash.replace(/^#/, ''));
  const [service, method] = (params.get('m') || '').split('.');
  if (params.get('t')) $('target').value = params.get('t');
  // The search page hands over a request too long for a URL in storage.
  let body = params.get('b');
  if (params.get('from') === 'search') {
    try { body = localStorage.getItem(HANDOFF_KEY) ?? body; } catch { /* an empty body */ }
  }
  if (service && method) choose(service, method, body, false);
}

function remember() {
  if (!state.described) return;
  const params = new URLSearchParams({ m: `${state.described.service}.${state.described.method}` });
  if ($('target').value !== 'coordinator') params.set('t', $('target').value);
  if ($('body').value.trim() !== '{}') params.set('b', $('body').value);
  const hash = `#${params}`;
  if (hash !== location.hash) history.pushState(null, '', hash);
}

function renderMethods() {
  const box = clear($('methods'));
  const needle = $('method-filter').value.trim().toLowerCase();
  const chosen = state.described ? `${state.described.service}.${state.described.method}` : '';
  let service = '';
  for (const m of state.config.methods || []) {
    if (needle && !m.method.toLowerCase().includes(needle)) continue;
    if (m.service !== service) { service = m.service; box.append(el('h3', { text: service })); }
    box.append(el('button', {
      class: `method${chosen === `${m.service}.${m.method}` ? ' on' : ''}`,
      onclick: () => choose(m.service, m.method, null, true),
    }, [el('span', { class: 'mono', text: m.method }), m.server_streaming ? el('span', { class: 'pill', text: 'stream' }) : null]));
  }
  if (!box.firstChild) box.append(el('div', { class: 'muted small', text: 'No exposed method matches.' }));
}

async function choose(service, method, body, push) {
  stop();
  hideError();
  try {
    state.described = await api.get(`/api/describe/${service}/${method}`);
  } catch (e) {
    return showError(e);
  }
  const d = state.described;
  $('method-name').textContent = `${d.service}.${d.method}`;
  $('method-kind').textContent = d.server_streaming ? 'server streaming' : 'unary';
  $('method-kind').classList.remove('hidden');
  $('method-comment').textContent = d.comment;
  $('run').disabled = false;
  $('body').value = body || '{}';
  state.response = '';
  $('status').textContent = '';
  checkBody();
  renderMethods();
  renderShape();
  renderRaw();
  if (push) remember();
}

function parsedBody() {
  try { return JSON.parse($('body').value || '{}'); } catch { return undefined; }
}

function setBody(value) {
  $('body').value = JSON.stringify(value, null, 2);
  checkBody();
}

function checkBody() {
  const ok = parsedBody() !== undefined;
  $('body').classList.toggle('invalid', !ok);
  $('body-state').textContent = ok ? '' : 'not valid JSON: a click on a field cannot add to it until it parses';
}

// ---------------------------------------------------------------------------
// The message shapes
// ---------------------------------------------------------------------------

function shortName(type) { return type.split('.').pop(); }

function renderShape() {
  for (const b of document.querySelectorAll('.tabs button[data-shape]')) b.classList.toggle('active', b.dataset.shape === state.shape);
  const d = state.described;
  if (!d) return;
  const root = d[state.shape];
  $('shape-name').textContent = root;
  const box = clear($('shape'));
  box.classList.remove('muted');
  box.append(fieldList(root, [], new Set()));
}

// The fields of one message. `path` is the JSON path to the message from
// the request's root; `open` holds the messages on the way down, so a
// message that contains itself is offered once and not unrolled forever.
function fieldList(type, path, open) {
  const d = state.described;
  const message = d.messages[type];
  const list = el('ul');
  if (!message.fields.length) list.append(el('li', { class: 'muted', text: 'no fields' }));
  for (const f of message.fields) {
    const isMessage = !!d.messages[f.type];
    const values = d.enums[f.type];
    const label = `${f.map ? `map<${shortName(f.map.key)}, ${shortName(f.type)}>` : shortName(f.type)}${f.repeated ? '[]' : ''}`;
    const here = [...path, { name: f.name, repeated: f.repeated, map: !!f.map }];
    const head = [
      state.shape === 'request'
        ? el('button', { class: 'quiet mono name', text: f.name, title: 'add this field to the request JSON', onclick: () => addField(here, f) })
        : el('span', { class: 'mono name', text: f.name }),
      el('span', { class: 'mono muted', text: ` ${label}` }),
      f.oneof ? el('span', { class: 'pill', text: `oneof ${f.oneof}` }) : null,
    ];
    const notes = [
      f.comment ? el('div', { class: 'comment', text: f.comment }) : null,
      values ? el('div', { class: 'mono comment', text: values.join(' | ') }) : null,
    ];
    if (isMessage && !open.has(f.type)) {
      const inner = el('details', {}, [el('summary', {}, head), ...notes]);
      // Built when opened: the request graph is large and mostly unread.
      inner.addEventListener('toggle', () => {
        if (inner.open && !inner.querySelector('ul')) inner.append(fieldList(f.type, here, new Set([...open, type])));
      });
      list.append(el('li', {}, [inner]));
    } else {
      list.append(el('li', {}, [el('div', {}, head), ...notes]));
    }
  }
  return list;
}

function addField(path, field) {
  const body = parsedBody();
  if (body === undefined || typeof body !== 'object' || Array.isArray(body) || body === null) return checkBody();
  const d = state.described;
  setBody(insertAt(body, path, defaultValue(field, d.messages, d.enums)));
}

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

async function run() {
  const d = state.described;
  if (!d) return;
  hideError();
  const body = parsedBody();
  if (body === undefined) return showError({ error: 'the request is not valid JSON' });
  remember();
  const target = $('target').value;
  const started = performance.now();
  if (d.server_streaming) {
    const messages = []; // rendered JSON, newest first
    let seen = 0;
    let chars = 0;
    $('run').textContent = 'Stop';
    state.tab = 'response';
    const show = (ending) => {
      state.response = messages.join('\n\n');
      const kept = seen > messages.length ? `, newest ${messages.length} shown` : seen > 1 ? ', newest first' : '';
      $('status').textContent = `${seen} message(s) in ${fmtMs(performance.now() - started)}${kept}${ending || ''}`;
      renderRaw();
    };
    state.stream = api.stream(d.service, d.method, body, {
      onEvent: (msg) => {
        const text = JSON.stringify(msg, null, 2);
        seen++;
        messages.unshift(text);
        chars += text.length;
        while (messages.length > 1 && chars > STREAM_CHARS) chars -= messages.pop().length;
        show();
      },
      onError: (err) => { showError(err); stop(); },
      onEnd: () => { show(', ended'); stop(); },
    }, target);
    return;
  }
  $('run').disabled = true;
  try {
    const response = await api.rpc(d.service, d.method, body, target);
    state.response = JSON.stringify(response, null, 2);
    $('status').textContent = `200 OK in ${fmtMs(performance.now() - started)}, ${state.response.length.toLocaleString()} characters`;
  } catch (e) {
    state.response = JSON.stringify(e, null, 2);
    $('status').textContent = `${e.http ?? ''} ${e.code ?? 'error'} in ${fmtMs(performance.now() - started)}`;
    showError(e);
  } finally {
    $('run').disabled = false;
  }
  state.tab = 'response';
  renderRaw();
}

function stop() {
  if (state.stream) { state.stream.close(); state.stream = null; }
  $('run').textContent = 'Run';
}

function renderRaw() {
  for (const b of document.querySelectorAll('.tabs button[data-tab]')) b.classList.toggle('active', b.dataset.tab === state.tab);
  const d = state.described;
  if (state.tab === 'grpcurl') {
    const body = parsedBody();
    $('raw').textContent = d && body !== undefined ? grpcurlFor(state.config, d.service, d.method, body) : '';
  } else {
    $('raw').textContent = state.response;
  }
}

function showError(e) {
  $('error').textContent = errorText(e);
  $('error').classList.remove('hidden');
}

function hideError() { $('error').classList.add('hidden'); }
