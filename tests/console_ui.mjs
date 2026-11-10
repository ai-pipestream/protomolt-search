// The console UI's pure module (src/bin/console-ui/query-state.js):
// `node tests/console_ui.mjs` from the repository root. No dependencies.
import assert from 'node:assert/strict';
import {
  splitTopLevel, splitConjuncts, toggleConjunct, isIdentifier, celString, rangeColumn,
  rangeClause, replaceRange, calendarUpper, encodeState, decodeState, parseSynonyms,
  identifiersIn, compareRuns, toCsv, defaultValue, insertAt,
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
console.log('console_ui: ok');
