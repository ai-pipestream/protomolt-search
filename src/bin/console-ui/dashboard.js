// The dashboard: live telemetry from StreamMetrics (server-sent events),
// the runtime knobs, the shard map, and the recent-queries ring. Every
// diagnostics call that the cluster does not serve yet renders as a
// plain "not served" state and is polled slowly. This module wires the
// page; the work lives in the dashboard-*.js modules it imports.
import { el, loadRawConfig, mountHeader } from './common.js';
import { $, state } from './dashboard-state.js';
import { buildTiles, restart } from './dashboard-poll.js';
import { arrangeRoutes } from './dashboard-metrics.js';
import { runBalancePlan, runPlacementPlan } from './dashboard-cluster.js';

mountHeader('dashboard');
init();

async function init() {
  try { state.config = await loadRawConfig(); } catch (e) { state.config = { nodes: [] }; }
  const target = $('target');
  (state.config.nodes || []).forEach((addr, i) => target.append(el('option', { value: `node${i}`, text: `node${i} ${addr}` })));
  target.addEventListener('change', () => { state.target = target.value; restart(); });
  $('interval').addEventListener('change', () => restart());
  buildTiles();
  $('routes-all').addEventListener('click', () => { state.allRoutes = !state.allRoutes; arrangeRoutes(); });
  for (const th of $('routes').querySelectorAll('th[data-sort]')) {
    th.addEventListener('click', () => { state.routeSort = th.dataset.sort; arrangeRoutes(); });
  }
  $('plan-run').addEventListener('click', runPlacementPlan);
  $('bal-run').addEventListener('click', runBalancePlan);
  restart();
}
