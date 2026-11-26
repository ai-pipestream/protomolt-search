// The search page's shared state, the element lookup, and the constants
// its modules agree on.

export const $ = (id) => document.getElementById(id);
export const state = {
  config: null,
  request: null,
  response: null,
  cursors: [],      // cursor stack for paging back
  cursor: '',
  texts: new Map(), // doc id -> text
  tab: 'response',
  stream: null,
  vector: null,     // last embedded vector, with the text it came from
  defaults: null,   // the form as the page loads it, for the URL state
  suggestIndex: -1,
  suggestSeq: 0,    // bumped when the list closes, so a late answer does not reopen it
  baseline: null,   // the pinned run: { label, hash, ids, at }
  timings: [],      // the last two timed queries, newest first
};

// The controls the URL carries. A pasted vector is left out: it does not
// fit in a URL.
export const FORM_IDS = [
  'q', 'shape', 'phrase', 'slop', 'prefix', 'prefix-max', 'synonyms', 'synonyms-off',
  'boost-text', 'boost-window', 'boost-base', 'boost-weight', 'fusion', 'rrf-k', 'blend-norm', 'blend-comb', 'w-dense', 'w-lex', 'precision', 'exec-mode', 'scorer-op',
  'msm', 'filter', 'k', 'selection-k', 'sort', 'collapse', 'inner-hits', 'columns', 'highlight',
  'hl-mode', 'explain', 'profile', 'fetch-text', 'group-by', 'hist-expr', 'hist-interval',
  'hist-calendar', 'pct-expr', 'pct-list', 'ab-a', 'ab-b',
];
export const HISTORY_KEY = 'pipestream.console.history';
export const HISTORY_MAX = 30;
export const COLUMNS_KEY = 'pipestream.console.columns';
export const COLUMNS_MAX = 60;
export const BASELINE_KEY = 'pipestream.console.baseline';
// The inputs whose text names columns, for the suggestions.
export const COLUMN_INPUTS = ['filter', 'sort', 'collapse', 'columns', 'group-by', 'hist-expr', 'pct-expr'];
export const SCORE_OPS = [
  ['SCORE_OP_MULT_EXP_DECAY', 'decay', 'score × exp(−|x − origin| / scale)', ['origin', 'scale']],
  ['SCORE_OP_MULT_LOG', 'log boost', 'score × (1 + weight · ln(1 + x))', ['weight']],
  ['SCORE_OP_ADD_LINEAR', 'add', 'score + weight · x', ['weight']],
];
export const NORMALIZATIONS = [['', 'min-max (default)'], ['SCORE_NORMALIZATION_Z_SCORE', 'z-score'], ['SCORE_NORMALIZATION_NONE', 'raw']];
export const MISSING = [
  ['', 'missing: zero', 'a candidate without the signal scores a normalized zero, its weight still counted'],
  ['MISSING_SCORE_POLICY_SKIP', 'missing: skip', 'the dimension leaves the combination for that candidate'],
  ['MISSING_SCORE_POLICY_ERROR', 'missing: refuse', 'a candidate without the signal refuses the request'],
];
export const TIMED_RUNS = 20;
// Where a request too long for a URL waits for the API explorer.
export const HANDOFF_KEY = 'pipestream.console.explorer-body';
