// Shared plumbing for the console pages: the facade's API, DOM helpers,
// and the grpcurl rendering. Plain ES module, no dependencies.

export const api = {
  async get(path) {
    const r = await fetch(path, { cache: 'no-store' });
    return finish(r);
  },
  async post(path, body) {
    const r = await fetch(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    });
    return finish(r);
  },
  // One unary RPC through the facade. `target` is `coordinator` or `nodeN`.
  rpc(service, method, body, target) {
    const q = target ? `?target=${encodeURIComponent(target)}` : '';
    return api.post(`/api/rpc/${service}/${method}${q}`, body);
  },
  // A server-streaming RPC as server-sent events. Returns an object
  // with `close()`; `onEvent(json)` is called per message, `onEnd()`
  // when the stream ends, `onError(err)` on a stream error.
  stream(service, method, body, handlers, target) {
    const controller = new AbortController();
    const q = target ? `?target=${encodeURIComponent(target)}` : '';
    (async () => {
      let response;
      try {
        response = await fetch(`/api/stream/${service}/${method}${q}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(body ?? {}),
          signal: controller.signal,
        });
      } catch (e) {
        if (!controller.signal.aborted) handlers.onError?.(errorOf(e));
        return;
      }
      if (!response.ok) {
        let err;
        try { err = await response.json(); } catch { err = { error: response.statusText }; }
        err.http = response.status;
        handlers.onError?.(err);
        return;
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          let idx;
          while ((idx = buf.indexOf('\n\n')) >= 0) {
            const frame = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            const ev = parseFrame(frame);
            if (ev.event === 'error') handlers.onError?.(ev.data);
            else if (ev.event === 'end') handlers.onEnd?.();
            else handlers.onEvent?.(ev.data);
          }
        }
        handlers.onEnd?.();
      } catch (e) {
        if (!controller.signal.aborted) handlers.onError?.(errorOf(e));
      }
    })();
    return { close: () => controller.abort() };
  },
};

function parseFrame(frame) {
  let event = 'message';
  const dataLines = [];
  for (const line of frame.split('\n')) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
  }
  let data = dataLines.join('\n');
  try { data = JSON.parse(data); } catch { /* keep text */ }
  return { event, data };
}

async function finish(r) {
  const text = await r.text();
  let body;
  try { body = text ? JSON.parse(text) : {}; } catch { body = { error: text }; }
  if (!r.ok) {
    const err = typeof body === 'object' && body ? body : { error: String(body) };
    err.http = r.status;
    throw err;
  }
  return body;
}

function errorOf(e) {
  return { error: e?.message ?? String(e), code: 'CLIENT' };
}

export function isUnimplemented(err) {
  return err && (err.http === 501 || err.code === 'UNIMPLEMENTED');
}

export function errorText(err) {
  if (!err) return '';
  if (typeof err === 'string') return err;
  const code = err.code ? `${err.code}: ` : '';
  return `${code}${err.error ?? err.message ?? JSON.stringify(err)}`;
}

// DOM: el('div', {class: 'x', onclick: fn}, [children...])
export function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === 'class') node.className = v;
    else if (k === 'text') node.textContent = v;
    else if (k === 'html') node.innerHTML = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else if (k === 'value') node.value = v;
    else if (k === 'checked') node.checked = !!v;
    else if (k === 'disabled') node.disabled = !!v;
    else node.setAttribute(k, v === true ? '' : v);
  }
  for (const c of [].concat(children)) {
    if (c == null || c === false) continue;
    node.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return node;
}

export function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
  return node;
}

export function fmtMs(v) {
  if (v == null || Number.isNaN(v)) return '';
  const n = Number(v);
  if (n >= 1000) return `${(n / 1000).toFixed(2)} s`;
  if (n >= 10) return `${n.toFixed(1)} ms`;
  return `${n.toFixed(2)} ms`;
}

export function fmtNum(v) {
  if (v == null) return '';
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  if (Number.isInteger(n)) return n.toLocaleString();
  return n.toPrecision(5);
}

export function debounce(fn, ms) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

let configCache;
export async function loadConfig() {
  if (!configCache) configCache = await api.get('/api/config');
  return configCache;
}

// A working grpcurl line for the same call. The token itself never
// reaches the browser, so it is spelled as a shell variable.
export function grpcurlFor(config, service, method, body) {
  const target = (config.coordinator || '').replace(/^https?:\/\//, '');
  const parts = ['grpcurl'];
  if (config.tls) parts.push('-cacert ca.pem -cert client.pem -key client.key');
  else parts.push('-plaintext');
  if (config.bearer) parts.push(`-H "authorization: Bearer $BEARER_TOKEN"`);
  const json = JSON.stringify(body).replace(/'/g, `'\\''`);
  parts.push(`-d '${json}'`);
  parts.push(target, `ai.pipestream.search.v1.${service}/${method}`);
  return parts.join(' ');
}

export function mountHeader(current) {
  const header = document.querySelector('.top');
  if (!header) return;
  header.append(
    el('h1', { text: 'Pipestream Search console' }),
    el('nav', {}, [
      el('a', { href: '/', class: current === 'search' ? 'current' : '', text: 'Search' }),
      el('a', { href: '/dashboard', class: current === 'dashboard' ? 'current' : '', text: 'Dashboard' }),
    ]),
    el('span', { class: 'status', id: 'cluster-status', text: 'connecting' }),
  );
  refreshStatus();
}

export async function refreshStatus() {
  const node = document.getElementById('cluster-status');
  if (!node) return;
  try {
    const health = await api.get('/api/health');
    const targets = health.targets || [];
    const up = targets.filter((t) => t.reachable).length;
    const docs = targets.reduce((a, t) => a + Number(t.live_docs ?? t.bm25_docs ?? 0), 0);
    clear(node).append(
      el('span', { class: `pill ${up === targets.length ? 'ok' : 'warn'}`, text: `${up}/${targets.length} shards` }),
      ' ',
      `${fmtNum(docs)} live docs`,
      health.topology_generation ? ` · topology ${health.topology_generation}` : '',
    );
    return health;
  } catch (e) {
    clear(node).append(el('span', { class: 'pill bad', text: 'coordinator unreachable' }), ' ', errorText(e));
    return null;
  }
}

export function copyText(text) {
  if (navigator.clipboard?.writeText) return navigator.clipboard.writeText(text);
  const ta = el('textarea', { value: text });
  document.body.append(ta);
  ta.select();
  document.execCommand('copy');
  ta.remove();
  return Promise.resolve();
}

// Enum names as the proto3 JSON expects them.
export const enums = {
  and: 'SELECTION_OPERATOR_AND',
  or: 'SELECTION_OPERATOR_OR',
  native: 'DENSE_SCORE_MODE_NATIVE',
  fp32: 'DENSE_SCORE_MODE_FP32_RERANK',
  normalization: {
    min_max: 'SCORE_NORMALIZATION_MIN_MAX',
    z_score: 'SCORE_NORMALIZATION_Z_SCORE',
    none: 'SCORE_NORMALIZATION_NONE',
  },
  combination: {
    arithmetic: 'SCORE_COMBINATION_ARITHMETIC',
    geometric: 'SCORE_COMBINATION_GEOMETRIC',
    harmonic: 'SCORE_COMBINATION_HARMONIC',
  },
  aggregateOp: ['COUNT', 'SUM', 'MIN', 'MAX', 'MEAN', 'VARIANCE', 'STDDEV', 'CARDINALITY'].map((n) => `AGGREGATE_OP_${n}`),
  calendar: ['MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'].map((n) => `CALENDAR_INTERVAL_${n}`),
  fusion: {
    global_rank: 'FUSION_MODE_GLOBAL_RANK',
    two_level: 'FUSION_MODE_TWO_LEVEL',
    cascade: 'FUSION_MODE_CASCADE',
    score_blend: 'FUSION_MODE_SCORE_BLEND',
    decomposed: 'FUSION_MODE_DECOMPOSED',
  },
};
