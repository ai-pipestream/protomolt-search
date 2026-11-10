// The console UI's pure module (src/bin/console-ui/query-state.js):
// `node tests/console_ui.mjs` from the repository root. No dependencies.
import assert from 'node:assert/strict';
import {
  splitTopLevel, splitConjuncts, toggleConjunct, isIdentifier, celString, rangeColumn,
  rangeClause, replaceRange, calendarUpper, encodeState, decodeState,
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
console.log('console_ui: ok');
