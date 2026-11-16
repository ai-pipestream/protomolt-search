// The dashboard's shared state and element lookup.

export const $ = (id) => document.getElementById(id);
export const WINDOW = 30; // snapshots kept for windowed rates and percentiles
export const state = {
  config: null,
  target: 'coordinator',
  stream: null,
  snapshots: [],       // ring of parsed snapshots
  routeSparks: new Map(),
  routeHists: new Map(), // rpc -> its latency histogram over the window
  routeOpen: null,       // the route whose distribution is drawn
  activeRoutes: new Set(), // routes seen serving, or with errors
  allRoutes: false,        // whether idle routes are listed too
  routeSort: 'rate',
  tiles: new Map(),
  timers: [],
  unserved: new Set(), // diagnostics methods the cluster answered UNIMPLEMENTED
};
