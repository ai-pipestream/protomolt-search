// The parts of the search page that are plain functions of text: the CEL
// filter as a list of conditions, the clauses a click on the rail adds,
// and the form's encoding in the URL fragment. No DOM, so
// `node tests/console_ui.mjs` exercises them directly.

// Split at a separator outside quotes and brackets; pieces are trimmed
// and empty ones dropped.
export function splitTopLevel(text, separator) {
  const parts = [];
  let depth = 0, quote = null, start = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quote) {
      if (c === '\\') i++;
      else if (c === quote) quote = null;
    } else if (c === '"' || c === "'") quote = c;
    else if ('([{'.includes(c)) depth++;
    else if (')]}'.includes(c)) depth--;
    else if (depth === 0 && text.startsWith(separator, i)) {
      parts.push(text.slice(start, i));
      i += separator.length - 1;
      start = i + 1;
    }
  }
  parts.push(text.slice(start));
  return parts.map((p) => p.trim()).filter(Boolean);
}

// The filter's conditions: its pieces at top-level `&&`. `||` binds
// looser than `&&`, so a filter with a top-level `||` is one condition.
export function splitConjuncts(filter) {
  const text = filter.trim();
  if (!text) return [];
  if (splitTopLevel(text, '||').length > 1) return [text];
  return splitTopLevel(text, '&&');
}

function conjunctsForEdit(filter) {
  const text = filter.trim();
  if (text && splitTopLevel(text, '||').length > 1) return [`(${text})`];
  return splitConjuncts(text);
}

// Remove the condition when the filter has it, add it otherwise.
export function toggleConjunct(filter, clause) {
  const parts = conjunctsForEdit(filter);
  const at = parts.indexOf(clause);
  if (at >= 0) parts.splice(at, 1);
  else parts.push(clause);
  return parts.join(' && ');
}

export function isIdentifier(text) {
  return typeof text === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(text);
}

export function celString(value) {
  return JSON.stringify(String(value));
}

// The column a histogram's buckets can be filtered on, and the type its
// literals take. A filter compares a column in its own type and does not
// compile `double()`, so `double(year)` filters on `year` with integer
// literals, a bare column of a fixed histogram is a double column, and a
// calendar histogram's column holds integer microseconds. Any other
// expression has no filter form and its buckets are not clickable.
export function rangeColumn(expression, calendar) {
  const text = expression.trim();
  if (isIdentifier(text)) return { column: text, integer: !!calendar };
  const wrapped = text.match(/^double\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)$/);
  if (wrapped && !calendar) return { column: wrapped[1], integer: true };
  return null;
}

function literal(n, integer) {
  if (integer) return Number.isInteger(n) ? String(n) : null;
  return Number.isInteger(n) ? `${n}.0` : String(n);
}

// The half-open range `[lower, upper)` on a column as one condition.
export function rangeClause(target, lower, upper) {
  const lo = literal(lower, target.integer), hi = literal(upper, target.integer);
  if (lo == null || hi == null) return null;
  return `(${target.column} >= ${lo} && ${target.column} < ${hi})`;
}

// Put one range on a column: any other range on it goes, and choosing
// the range the filter already has removes it.
export function replaceRange(filter, column, clause) {
  const shape = new RegExp(`^\\(${column} >= \\S+ && ${column} < \\S+\\)$`);
  const parts = conjunctsForEdit(filter);
  const kept = parts.filter((p) => !shape.test(p));
  if (!parts.includes(clause)) kept.push(clause);
  return kept.join(' && ');
}

// The start of the calendar bucket after the one starting at `micros`.
export function calendarUpper(micros, unit) {
  const d = new Date(micros / 1000);
  const name = unit.replace('CALENDAR_INTERVAL_', '');
  if (name === 'MINUTE') d.setUTCMinutes(d.getUTCMinutes() + 1);
  else if (name === 'HOUR') d.setUTCHours(d.getUTCHours() + 1);
  else if (name === 'DAY') d.setUTCDate(d.getUTCDate() + 1);
  else if (name === 'WEEK') d.setUTCDate(d.getUTCDate() + 7);
  else if (name === 'MONTH') d.setUTCMonth(d.getUTCMonth() + 1);
  else if (name === 'QUARTER') d.setUTCMonth(d.getUTCMonth() + 3);
  else d.setUTCFullYear(d.getUTCFullYear() + 1);
  return d.getTime() * 1000;
}

// The form as a URL fragment: only what differs from the page's
// defaults, lists as JSON.
export function encodeState(form, defaults) {
  const out = new URLSearchParams();
  for (const [key, value] of Object.entries(form)) {
    const encoded = Array.isArray(value) ? JSON.stringify(value) : String(value);
    const initial = Array.isArray(defaults[key]) ? JSON.stringify(defaults[key]) : String(defaults[key]);
    if (encoded !== initial) out.set(key, encoded);
  }
  return out.toString();
}

// The defaults with the fragment's values over them. A key the form
// does not have, or a list that does not parse, is ignored.
export function decodeState(hash, defaults) {
  const form = structuredClone(defaults);
  for (const [key, value] of new URLSearchParams(hash)) {
    if (!(key in defaults)) continue;
    if (Array.isArray(defaults[key])) {
      try {
        const list = JSON.parse(value);
        if (Array.isArray(list)) form[key] = list;
      } catch { /* keep the default */ }
    } else if (typeof defaults[key] === 'boolean') form[key] = value === 'true';
    else form[key] = value;
  }
  return form;
}

// Synonym rules as typed, one per line: `ship, vessel` is symmetric,
// `car => automobile, auto` is one-way. Blank lines are skipped.
export function parseSynonyms(text) {
  const words = (part) => part.split(',').map((w) => w.trim()).filter(Boolean);
  const rules = [];
  for (const line of text.split('\n')) {
    const [from, to] = line.split('=>');
    const terms = words(from);
    if (!terms.length) continue;
    const target = to === undefined ? [] : words(to);
    rules.push(target.length ? { terms, to: target } : { terms });
  }
  return rules;
}

const CEL_WORDS = new Set([
  'true', 'false', 'null', 'in', 'has', 'size', 'double', 'int', 'uint', 'string', 'bool', 'bytes',
  'contains', 'startsWith', 'endsWith', 'matches', 'timestamp', 'duration', 'desc', 'asc',
]);

// The identifiers an expression names, outside its string literals and
// minus CEL's own words: the column names a query used.
export function identifiersIn(text) {
  const bare = text.replace(/"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/g, ' ');
  const found = bare.match(/[A-Za-z_][A-Za-z0-9_]*/g) || [];
  return [...new Set(found.filter((w) => !CEL_WORDS.has(w)))];
}

// How a run moved against a pinned baseline. Both are document ids in
// rank order. `moves` has one entry per current hit: the ranks it rose
// (positive), fell (negative), or null when the baseline did not have it.
export function compareRuns(baseline, current) {
  const was = new Map(baseline.map((id, i) => [id, i]));
  const now = new Set(current);
  const moves = current.map((id, i) => (was.has(id) ? was.get(id) - i : null));
  return {
    moves,
    overlap: moves.filter((m) => m !== null).length,
    added: moves.filter((m) => m === null).length,
    dropped: baseline.map((id, i) => ({ id, rank: i + 1 })).filter((d) => !now.has(d.id)),
    topSame: baseline.length > 0 && current.length > 0 && baseline[0] === current[0],
  };
}

// Rows of cells as CSV text (RFC 4180 quoting).
export function toCsv(rows) {
  const cell = (v) => {
    const text = v == null ? '' : String(v);
    return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  };
  return rows.map((row) => row.map(cell).join(',')).join('\r\n');
}

// The value a click on a field of the API explorer puts in the request:
// the type's zero, an enum's first meaningful name, an empty message.
// `field` is an entry of /api/describe; a repeated field gets one
// element and a map an empty object.
export function defaultValue(field, messages, enums) {
  if (field.map) return {};
  let value;
  if (messages[field.type]) value = {};
  else if (enums[field.type]) value = enums[field.type].find((name) => !name.endsWith('_UNSPECIFIED')) ?? enums[field.type][0];
  else if (field.type === 'string' || field.type === 'bytes') value = '';
  else if (field.type === 'bool') value = false;
  else value = 0;
  return field.repeated ? [value] : value;
}

// Set `value` at a path of `{ name, repeated, map }` steps under `root`,
// creating the messages on the way; a repeated step goes through its
// first element. A field already present keeps its value. Returns root.
export function insertAt(root, path, value) {
  let node = root;
  path.forEach((step, i) => {
    const last = i === path.length - 1;
    if (last) {
      if (!(step.name in node)) node[step.name] = value;
      return;
    }
    if (step.map) {
      // A message-valued map: the nested field goes under a placeholder key.
      if (typeof node[step.name] !== 'object' || node[step.name] === null) node[step.name] = {};
      const key = Object.keys(node[step.name])[0] ?? 'key';
      if (typeof node[step.name][key] !== 'object' || node[step.name][key] === null) node[step.name][key] = {};
      node = node[step.name][key];
    } else if (step.repeated) {
      if (!Array.isArray(node[step.name]) || !node[step.name].length) node[step.name] = [{}];
      if (typeof node[step.name][0] !== 'object' || node[step.name][0] === null) node[step.name][0] = {};
      node = node[step.name][0];
    } else {
      if (typeof node[step.name] !== 'object' || node[step.name] === null || Array.isArray(node[step.name])) node[step.name] = {};
      node = node[step.name];
    }
  });
  return root;
}
