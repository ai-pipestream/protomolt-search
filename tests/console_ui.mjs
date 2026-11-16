// The console UI's modules (src/bin/console-ui/): `node tests/console_ui.mjs`
// or `node --test tests/console_ui.mjs` from the repository root. No
// dependencies. query-state.js is pure; the page modules load under the
// minimal document below and their DOM-free helpers are checked directly.
import assert from 'node:assert/strict';
import {
  splitTopLevel, splitConjuncts, toggleConjunct, isIdentifier, celString, rangeColumn,
  rangeClause, replaceRange, calendarUpper, encodeState, decodeState, parseSynonyms,
  identifiersIn, compareRuns, toCsv, defaultValue, insertAt, distribution,
} from '../src/bin/console-ui/query-state.js';

// Separators inside quotes and brackets do not split.
assert.deepEqual(splitTopLevel('court, f(a, b), "x, y"', ','), ['court', 'f(a, b)', '"x, y"']);
assert.deepEqual(splitConjuncts(''), []);
assert.deepEqual(splitConjuncts('year >= 2010 && court == "a && b" && (x > 1 && y < 2)'),
  ['year >= 2010', 'court == "a && b"', '(x > 1 && y < 2)']);
assert.deepEqual(splitConjuncts('title == "say \\"&&\\" twice" && k == 1'), ['title == "say \\"&&\\" twice"', 'k == 1']);
// `a || b && c` is `a || (b && c)`: one condition, not two.
assert.deepEqual(splitConjuncts('a == 1 || b == 2 && c == 3'), ['a == 1 || b == 2 && c == 3']);

// Toggle adds, then removes; a top-level `||` is parenthesized first.
assert.equal(toggleConjunct('', 'court == "ca9"'), 'court == "ca9"');
assert.equal(toggleConjunct('year > 2000', 'court == "ca9"'), 'year > 2000 && court == "ca9"');
assert.equal(toggleConjunct('year > 2000 && court == "ca9"', 'year > 2000'), 'court == "ca9"');
assert.equal(toggleConjunct('a == 1 || b == 2', 'c == 3'), '(a == 1 || b == 2) && c == 3');

assert.ok(isIdentifier('court') && !isIdentifier('double(year)') && !isIdentifier(undefined));
assert.equal(celString('say "hi"'), '"say \\"hi\\""');

// A filter does not compile double(): the range goes on the column in
// the column's own type.
assert.deepEqual(rangeColumn('double(year)', ''), { column: 'year', integer: true });
assert.deepEqual(rangeColumn('citations', ''), { column: 'citations', integer: false });
assert.deepEqual(rangeColumn('filed', 'CALENDAR_INTERVAL_YEAR'), { column: 'filed', integer: true });
assert.equal(rangeColumn('year + 1', ''), null);
assert.equal(rangeColumn('double(year)', 'CALENDAR_INTERVAL_YEAR'), null);
assert.equal(rangeClause({ column: 'year', integer: true }, 2000, 2005), '(year >= 2000 && year < 2005)');
assert.equal(rangeClause({ column: 'citations', integer: false }, 0, 100), '(citations >= 0.0 && citations < 100.0)');
assert.equal(rangeClause({ column: 'citations', integer: false }, 0.5, 1), '(citations >= 0.5 && citations < 1.0)');
assert.equal(rangeClause({ column: 'year', integer: true }, 0.5, 1), null);

// One range per column: another bucket replaces it, the same one clears it.
const a = '(year >= 2000 && year < 2005)', b = '(year >= 2005 && year < 2010)';
assert.equal(replaceRange('court == "ca9"', 'year', a), `court == "ca9" && ${a}`);
assert.equal(replaceRange(`court == "ca9" && ${a}`, 'year', b), `court == "ca9" && ${b}`);
assert.equal(replaceRange(`court == "ca9" && ${a}`, 'year', a), 'court == "ca9"');

const jan2005 = Date.UTC(2005, 0, 1) * 1000;
assert.equal(calendarUpper(jan2005, 'CALENDAR_INTERVAL_YEAR'), Date.UTC(2006, 0, 1) * 1000);
assert.equal(calendarUpper(jan2005, 'CALENDAR_INTERVAL_QUARTER'), Date.UTC(2005, 3, 1) * 1000);
assert.equal(calendarUpper(jan2005, 'CALENDAR_INTERVAL_MONTH'), Date.UTC(2005, 1, 1) * 1000);
assert.equal(calendarUpper(jan2005, 'CALENDAR_INTERVAL_WEEK'), Date.UTC(2005, 0, 8) * 1000);
assert.equal(calendarUpper(jan2005, 'CALENDAR_INTERVAL_DAY'), Date.UTC(2005, 0, 2) * 1000);

// The URL carries only what differs from the defaults and round-trips.
const defaults = { q: '', shape: 'lexical', k: '10', explain: true, clauses: [{ kind: 'must', leaf: 'lexical', value: '' }], aggs: [] };
assert.equal(encodeState(defaults, defaults), '');
const form = { ...defaults, q: 'merger & "competition"', explain: false, aggs: [{ name: 'n', expression: 'year', op: 'AGGREGATE_OP_MAX' }] };
const hash = encodeState(form, defaults);
assert.ok(!hash.includes('shape') && !hash.includes('clauses'));
assert.deepEqual(decodeState(hash, defaults), form);
// Unknown keys and unparseable lists fall back to the defaults.
assert.deepEqual(decodeState('nope=1&aggs=%5Bbroken&q=x', defaults), { ...defaults, q: 'x' });

assert.deepEqual(parseSynonyms('ship, vessel\n\n car => automobile , auto\n=> x'),
  [{ terms: ['ship', 'vessel'] }, { terms: ['car'], to: ['automobile', 'auto'] }]);

// Column names: identifiers outside strings, minus CEL's own words.
assert.deepEqual(identifiersIn('year >= 2010 && court == "scotus && x" && double(citations) > 1.0 && topic in ["a"]'),
  ['year', 'court', 'citations', 'topic']);

// Movement against a baseline: positive rose, negative fell, null is new.
const cmp = compareRuns(['a', 'b', 'c', 'd'], ['b', 'a', 'x', 'd']);
assert.deepEqual(cmp.moves, [1, -1, null, 0]);
assert.deepEqual([cmp.overlap, cmp.added, cmp.topSame], [3, 1, false]);
assert.deepEqual(cmp.dropped, [{ id: 'c', rank: 3 }]);
assert.equal(compareRuns(['a'], ['a']).topSame, true);

assert.equal(toCsv([['rank', 'text'], [1, 'say "hi", twice'], [2, 'two\nlines'], [3, null]]),
  'rank,text\r\n1,"say ""hi"", twice"\r\n2,"two\nlines"\r\n3,');
// The API explorer: a starting value per field kind, and nested inserts.
const messages = { 'pkg.Leaf': {} };
const enums = { 'pkg.Mode': ['MODE_UNSPECIFIED', 'MODE_FAST'], 'pkg.Bare': ['ONLY'] };
assert.equal(defaultValue({ type: 'string' }, messages, enums), '');
assert.equal(defaultValue({ type: 'uint32' }, messages, enums), 0);
assert.equal(defaultValue({ type: 'bool' }, messages, enums), false);
assert.equal(defaultValue({ type: 'pkg.Mode' }, messages, enums), 'MODE_FAST');
assert.equal(defaultValue({ type: 'pkg.Bare' }, messages, enums), 'ONLY');
assert.deepEqual(defaultValue({ type: 'pkg.Leaf', repeated: true }, messages, enums), [{}]);
assert.deepEqual(defaultValue({ type: 'string', map: true }, messages, enums), {});
assert.deepEqual(
  insertAt({ k: 5 }, [{ name: 'selection' }, { name: 'clauses', repeated: true }, { name: 'text' }], ''),
  { k: 5, selection: { clauses: [{ text: '' }] } });
// A value already there stays; a message-valued map goes through a placeholder key.
assert.deepEqual(insertAt({ k: 5 }, [{ name: 'k' }], 0), { k: 5 });
assert.deepEqual(insertAt({}, [{ name: 'by_name', map: true }, { name: 'weight' }], 0), { by_name: { key: { weight: 0 } } });
assert.deepEqual(insertAt({ by_name: { a: {} } }, [{ name: 'by_name', map: true }, { name: 'weight' }], 1), { by_name: { a: { weight: 1 } } });
// Nearest-rank order statistics: each figure is one of the samples.
const twenty = Array.from({ length: 20 }, (_, i) => 20 - i);
assert.deepEqual(distribution(twenty), { min: 1, p50: 10, p95: 19, max: 20 });
assert.deepEqual(distribution([7]), { min: 7, p50: 7, p95: 7, max: 7 });

// A document just large enough for the page modules to load and for el().
class FakeNode {
  constructor(tag) { Object.assign(this, { tag, attributes: {}, children: [], listeners: {}, dataset: {} }); }
  setAttribute(k, v) { this.attributes[k] = String(v); }
  addEventListener(type, fn) { this.listeners[type] = fn; }
  append(...nodes) { this.children.push(...nodes); }
  set innerHTML(v) { throw new Error(`markup parsed: ${v}`); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  documentElement: new FakeNode('html'),
  createElement: (tag) => new FakeNode(tag),
  createTextNode: (text) => Object.assign(new FakeNode('#text'), { text }),
  getElementById: () => null,
};
const ui = (name) => import(`../src/bin/console-ui/${name}`);
const common = await ui('common.js');

// el(): text is text, listeners attach, and markup is never parsed.
const node = common.el('a', { class: 'pill', text: '<b>x</b>', onclick: () => 1, html: '<b>y</b>', href: '/' }, ['t', null]);
assert.deepEqual([node.className, node.textContent, typeof node.listeners.click], ['pill', '<b>x</b>', 'function']);
assert.deepEqual(node.attributes, { html: '<b>y</b>', href: '/' });
assert.equal(node.children.length, 1);
assert.deepEqual([common.fmtMs(5), common.fmtMs(12.34), common.fmtMs(1500), common.fmtMs(null)], ['5.00 ms', '12.3 ms', '1.50 s', '']);
assert.deepEqual([common.fmtBytes(999), common.fmtBytes(1500), common.fmtBytes(123456), common.fmtBytes('x')], ['999 B', '1.50 kB', '123 kB', '']);
assert.equal(common.fmtNum(1.23456789), '1.2346');
assert.equal(common.errorText({ code: 'NOT_FOUND', error: 'gone' }), 'NOT_FOUND: gone');
assert.ok(common.isUnimplemented({ http: 501 }) && common.isUnimplemented({ code: 'UNIMPLEMENTED' }));
assert.equal(common.grpcurlFor({ coordinator: 'https://h:1', tls: true, bearer: true }, 'SearchService', 'Query', { q: "it's" }),
  `grpcurl -cacert ca.pem -cert client.pem -key client.key -H "authorization: Bearer $BEARER_TOKEN" -d '{"q":"it'\\''s"}' h:1 ai.protomolt.search.v1.SearchService/Query`);
assert.equal(common.grpcurlFor({ coordinator: 'http://h:2' }, 'SearchService', 'Query', {}), `grpcurl -plaintext -d '{}' h:2 ai.protomolt.search.v1.SearchService/Query`);

// The search page's request pieces.
const request = await ui('search-request.js');
assert.deepEqual(request.parseSort('year desc, court , '), [{ column: 'year', descending: true }, { column: 'court' }]);
assert.deepEqual(request.lexicalLeaf('l', 'coffee'), { search: { id: 'l', lexical: { text: 'coffee' } } });
assert.deepEqual(request.filterLeaf('f', 'year > 1'), { filter: { id: 'f', cel: 'year > 1' } });

// Hit rendering arithmetic: bars against the page maximum, clamped.
const hits = await ui('search-hits.js');
const scale = hits.pageScale([
  { score: 2, signals: [{ id: 'lex', score: 1 }], dimensions: [{ normalized: -0.5 }] },
  { score: 3, signals: [{ id: 'lex', score: 4 }, { id: 'dense', score: 0.5 }] },
]);
assert.deepEqual([scale.score, scale.legs, scale.dimension, [...scale.signals]], [3, 2, 0.5, [['lex', 4], ['dense', 0.5]]]);
assert.deepEqual([hits.share(50, 200), hits.share(5, 0), hits.share(300, 100), hits.share(-1, 10)], [25, 0, 100, 0]);
assert.deepEqual([hits.projectedValue({ uintValue: '7' }), hits.projectedValue({ boolValue: false })], ['7', false]);
assert.deepEqual([hits.foldValue({ doubleValue: 0.5 }), hits.foldValue({})], [0.5, '–']);
assert.deepEqual([hits.epochLabel(Date.UTC(2005, 0, 1) * 1000), hits.epochLabel(2005)], ['2005-01-01', '2005']);

// The streaming phases and the A/B presets.
const live = await ui('search-live.js');
assert.deepEqual([live.phaseName('QUERY_STREAM_PHASE_LEXICAL'), live.phaseName(undefined)], ['lexical', '']);
const presets = live.variantPresets();
assert.deepEqual(presets.map((p) => p.id),
  ['bm25', 'hybrid:global_rank', 'hybrid:cascade', 'hybrid:score_blend', 'hybrid:decomposed', 'hybrid:two_level']);
assert.deepEqual(presets[0].build('t'), { bm25: { text: 't' } });
assert.deepEqual(presets[3].build('t', [1]), { hybrid: { text: 't', vector: [1], legs: { fusion_mode: 'FUSION_MODE_SCORE_BLEND' } } });

// The dashboard: snapshots keyed by name and sorted labels, merged
// latency histograms, placement codes, and the placement tree file.
const metrics = await ui('dashboard-metrics.js');
const snap = metrics.parseSnapshot({
  unixMs: '1000',
  samples: [
    { name: 'up', kind: 'METRIC_KIND_GAUGE', value: '1', labels: [{ name: 'b', value: '2' }, { name: 'a', value: '1' }] },
    { name: 'calls', value: '5', labels: [] },
  ],
  histograms: [
    { name: 'rpc_request_duration_seconds', labels: [{ name: 'rpc', value: 'Query' }], buckets: [{ le: '0.1', cumulativeCount: '3' }], sum: '0.2', count: '3' },
    { name: 'rpc_request_duration_seconds', labels: [{ name: 'rpc', value: 'Suggest' }], buckets: [{ le: '0.1', cumulativeCount: '2' }], sum: '0.1', count: '2' },
  ],
});
assert.equal(snap.at, 1000);
assert.deepEqual([...snap.gauges.keys(), ...snap.counters.keys()], ['up{a=1,b=2}', 'calls{}']);
assert.ok(metrics.isDuration('rpc_request_duration_seconds') && !metrics.isDuration('calls'));
const merged = metrics.mergeHistograms(snap, snap, () => true);
assert.deepEqual([merged.count, merged.buckets[0].cumulativeCount], [5, 5]);
assert.equal(metrics.mergeHistograms(snap, snap, (l) => l.rpc === 'Query').count, 3);
assert.equal(metrics.mergeHistograms(snap, snap, () => false), null);

const cluster = await ui('dashboard-cluster.js');
assert.deepEqual([cluster.codePath(0), cluster.codePath(1n << 54n), cluster.codePath((3n << 54n) | (5n << 45n))], ['0', '1', '3.5']);
assert.ok(cluster.isRelay({ layout: 'RELAY' }) && !cluster.isRelay({ layout: 'relay', segments: [{}] }));
const tree = cluster.parseTree(`
[placement]
column = "region"  # the column placement keys on
level_bits = 9
[[placement.nodes]]
name = "eu"
shards = 2
nodes = ["n1", 'n2']
[[placement.nodes.children]]
name = "eu-west"
cel = 'region == "eu-west"'
`);
assert.deepEqual(tree, {
  column: 'region', level_bits: 9, nodes: [{
    name: 'eu', cel: '', shards: 2, nodes: ['n1', 'n2'],
    children: [{ name: 'eu-west', cel: 'region == "eu-west"', shards: 0, nodes: [], children: [] }],
  }],
});
assert.deepEqual(cluster.parseTree(' {"column": "c"} '), { column: 'c' });
assert.throws(() => cluster.parseTree('[[placement.nodes.children]]\nname = "x"'), /no parent node/);
assert.throws(() => cluster.parseTree('[other]'), /unknown table/);
console.log('console_ui: ok');
