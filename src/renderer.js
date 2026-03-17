const SYSTEMS = ['duckdb', 'postgres', 'fabric dw'];
const XBOUND_SUFFIX = ' xBound-ed';
const BENCHMARKS = ['JOBlight', 'SO-CEB', 'STATS-CEB'];

let queryStore = Object.fromEntries(BENCHMARKS.map((name) => [name, {}]));
const loadedBenchmarks = new Set();
let customQueryData = null;

const MOCK_PLAN_JSON = {
  duckdb: {
    op: 'PROJECTION',
    rows: '1.4M',
    cost: 920,
    children: [
      {
        op: 'HASH GROUP BY',
        rows: '1.4M',
        cost: 700,
        children: [
          {
            op: 'SEQ SCAN lineitem',
            rows: '6.0M',
            cost: 510,
            children: []
          }
        ]
      }
    ]
  },
  postgres: {
    op: 'Finalize Aggregate',
    rows: '1.4M',
    cost: 1330,
    children: [
      {
        op: 'Gather Merge',
        rows: '2.2M',
        cost: 1200,
        children: [
          {
            op: 'Partial HashAggregate',
            rows: '2.2M',
            cost: 940,
            children: [
              {
                op: 'Parallel Seq Scan lineitem',
                rows: '6.0M',
                cost: 800,
                children: []
              }
            ]
          }
        ]
      }
    ]
  },
  'fabric dw': {
    op: 'DISTRIBUTE STREAM',
    rows: '1.4M',
    cost: 1110,
    children: [
      {
        op: 'HASH AGGREGATE',
        rows: '1.4M',
        cost: 970,
        children: [
          {
            op: 'COLUMNSTORE SCAN lineitem',
            rows: '6.0M',
            cost: 640,
            children: []
          }
        ]
      }
    ]
  }
};

const els = {
  appName: document.getElementById('appName'),
  benchmarkSelect: document.getElementById('benchmarkSelect'),
  querySelect: document.getElementById('querySelect'),
  sqlInput: document.getElementById('sqlInput'),
  statusText: document.getElementById('statusText'),
  xboundToggle: document.getElementById('xboundToggle'),
  planSystemSelect: document.getElementById('planSystemSelect'),
  planControls: document.getElementById('planControls'),
  runBtn: document.getElementById('runBtn'),
  planViewBtn: document.getElementById('planViewBtn'),
  leaderboardBtn: document.getElementById('leaderboardBtn'),
  runPanel: document.getElementById('runPanel'),
  planPanel: document.getElementById('planPanel'),
  leaderboardPanel: document.getElementById('leaderboardPanel'),
  chartCanvas: document.getElementById('chartCanvas'),
  planTree: document.getElementById('planTree'),
  leaderboardList: document.getElementById('leaderboardList')
};

let currentMode = 'run';
let sqlEditor = null;

function getCurrentQueryData() {
  if (customQueryData) return customQueryData;
  const benchmark = els.benchmarkSelect.value;
  const queryName = els.querySelect.value;
  return queryStore[benchmark]?.[queryName];
}

function buildEstimateEntries(includeXBound) {
  const queryData = getCurrentQueryData();
  if (!queryData) return [];

  const entries = SYSTEMS
    .map((system) => {
      const estimate = queryData.estimates?.[system];
      const actual = queryData.actual;
      if (!Number.isFinite(estimate) || !Number.isFinite(actual) || actual === 0) return null;
      return {
        system,
        estimate,
        actual,
        qError: Math.max(estimate / actual, actual / estimate),
        signedQError: estimate >= actual
          ? Math.max(estimate / actual, actual / estimate)
          : -Math.max(estimate / actual, actual / estimate)
      };
    })
    .filter(Boolean);

  if (includeXBound) {
    SYSTEMS.forEach((system) => {
      const estimate = queryData.xbound?.[system];
      const actual = queryData.actual;
      if (!Number.isFinite(estimate) || !Number.isFinite(actual) || actual === 0) return;
      entries.push({
        system: `${system}${XBOUND_SUFFIX}`,
        estimate,
        actual,
        qError: Math.max(estimate / actual, actual / estimate),
        signedQError: estimate >= actual
          ? Math.max(estimate / actual, actual / estimate)
          : -Math.max(estimate / actual, actual / estimate)
      });
    });
  }

  return entries;
}

function renderQErrorBarPlot(entries) {
  const canvas = els.chartCanvas;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const parentWidth = canvas.parentElement?.clientWidth || 900;
  const cssWidth = Math.max(320, parentWidth - 2);
  const cssHeight = Math.max(240, canvas.clientHeight || 340);

  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  const margin = { top: 20, right: 26, bottom: 76, left: 68 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;
  const baselineY = margin.top + height / 2;
  const maxAbsQ = Math.max(1.2, ...entries.map((e) => Math.abs(e.signedQError || e.qError || 1)));
  const domainMin = -maxAbsQ * 1.1;
  const domainMax = maxAbsQ * 1.1;

  const y = (value) => {
    const ratio = (value - domainMin) / (domainMax - domainMin);
    return margin.top + height - ratio * height;
  };

  const xStep = width / Math.max(1, entries.length);

  ctx.clearRect(0, 0, cssWidth, cssHeight);
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, cssWidth, cssHeight);

  const ticks = [domainMin, domainMin / 2, 0, domainMax / 2, domainMax];

  ctx.strokeStyle = '#cdd6ea';
  ctx.lineWidth = 1;
  ticks.forEach((tick) => {
    const yy = y(tick);
    ctx.beginPath();
    ctx.moveTo(margin.left, yy);
    ctx.lineTo(cssWidth - margin.right, yy);
    ctx.stroke();

    ctx.fillStyle = '#6b718c';
    ctx.font = '12px IBM Plex Sans, sans-serif';
    if (Math.abs(tick) < 1e-9) ctx.fillText('1.00x', 8, yy + 4);
    else ctx.fillText(`${Math.abs(tick).toFixed(2)}x`, 8, yy + 4);
  });

  ctx.strokeStyle = '#1e2b54';
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(margin.left, baselineY);
  ctx.lineTo(cssWidth - margin.right, baselineY);
  ctx.stroke();

  ctx.fillStyle = '#1e2b54';
  ctx.font = '12px IBM Plex Sans, sans-serif';
  const actualBaseline = entries[0]?.actual;
  const baselineLabel = Number.isFinite(actualBaseline)
    ? `actual: ${actualBaseline.toLocaleString()}`
    : 'actual';
  ctx.fillText(baselineLabel, margin.left + 8, baselineY - 8);

  entries.forEach((entry, idx) => {
    const centerX = margin.left + xStep * idx + xStep / 2;
    const barWidth = Math.min(42, xStep * 0.58);
    const barTop = y(entry.signedQError || entry.qError);
    const barHeight = Math.abs(barTop - baselineY);
    const isXBound = entry.system.includes(XBOUND_SUFFIX);
    const accent = isXBound ? '#09b48b' : '#1461ff';

    ctx.fillStyle = accent;
    ctx.globalAlpha = 0.25;
    ctx.fillRect(centerX - barWidth / 2, Math.min(barTop, baselineY), barWidth, Math.max(1, barHeight));
    ctx.globalAlpha = 1;

    ctx.strokeStyle = accent;
    ctx.lineWidth = 2;
    ctx.strokeRect(centerX - barWidth / 2, Math.min(barTop, baselineY), barWidth, Math.max(1, barHeight));

    ctx.fillStyle = '#1f2a4d';
    ctx.font = '11px IBM Plex Sans, sans-serif';
    const label = entry.system.length > 14 ? `${entry.system.slice(0, 13)}...` : entry.system;
    const qLabelY = (entry.signedQError || entry.qError) >= 0 ? barTop - 6 : barTop + 15;
    const estimateLabel = Number.isFinite(entry.estimate)
      ? entry.estimate.toLocaleString()
      : String(entry.estimate);
    ctx.fillText(label, centerX - barWidth / 2 - 6, cssHeight - 36);
    ctx.fillText(estimateLabel, centerX - barWidth / 2 - 2, qLabelY);
  });
}

function createNodeElement(node) {
  const li = document.createElement('li');
  li.className = 'tree-node';

  const pill = document.createElement('div');
  pill.className = 'node-pill';

  const op = document.createElement('span');
  op.className = 'node-op';
  op.textContent = node.op;

  const meta = document.createElement('span');
  meta.className = 'node-meta';
  meta.textContent = `rows ${node.rows} | cost ${node.cost}`;

  pill.append(op, meta);
  li.appendChild(pill);

  if (node.children && node.children.length) {
    const ul = document.createElement('ul');
    node.children.forEach((child) => ul.appendChild(createNodeElement(child)));
    li.appendChild(ul);
  }

  return li;
}

function renderPlanTree(system) {
  els.planTree.innerHTML = '';
  const root = document.createElement('ul');
  root.className = 'tree-root';
  root.appendChild(createNodeElement(MOCK_PLAN_JSON[system]));
  els.planTree.appendChild(root);
}

function renderLeaderboard(entries) {
  const ranked = [...entries].sort((a, b) => Math.abs(a.qError - 1) - Math.abs(b.qError - 1));

  els.leaderboardList.innerHTML = '';
  ranked.forEach((entry) => {
    const li = document.createElement('li');
    li.textContent = `${entry.system}: q-error ${entry.qError.toFixed(2)}x (estimate ${entry.estimate.toLocaleString()}, actual ${entry.actual.toLocaleString()})`;
    els.leaderboardList.appendChild(li);
  });
}

function activeEntries() {
  return buildEstimateEntries(els.xboundToggle.checked);
}

function getSqlText() {
  if (sqlEditor) return sqlEditor.getValue();
  return els.sqlInput.value;
}

function setMode(mode) {
  currentMode = mode;
  els.runBtn.classList.toggle('active', mode === 'run');
  els.planViewBtn.classList.toggle('active', mode === 'plan');
  els.leaderboardBtn.classList.toggle('active', mode === 'leaderboard');

  els.runPanel.classList.toggle('hidden', mode !== 'run');
  els.planPanel.classList.toggle('hidden', mode !== 'plan');
  els.leaderboardPanel.classList.toggle('hidden', mode !== 'leaderboard');

  els.planControls.classList.toggle('hidden', mode !== 'plan');

  const entries = activeEntries();
  if (mode === 'run') renderQErrorBarPlot(entries);
  if (mode === 'plan') renderPlanTree(els.planSystemSelect.value);
  if (mode === 'leaderboard') renderLeaderboard(entries);
}

function populateSelectors() {
  BENCHMARKS.forEach((benchmark) => {
    const option = document.createElement('option');
    option.value = benchmark;
    option.textContent = benchmark;
    els.benchmarkSelect.appendChild(option);
  });

  SYSTEMS.forEach((s) => {
    const option = document.createElement('option');
    option.value = s;
    option.textContent = s;
    els.planSystemSelect.appendChild(option);
  });

  updateQuerySelector();
}

function updateQuerySelector() {
  const benchmark = els.benchmarkSelect.value;
  const queries = Object.keys(queryStore[benchmark] || {});

  els.querySelect.innerHTML = '';
  queries.forEach((q) => {
    const option = document.createElement('option');
    option.value = q;
    option.textContent = q;
    els.querySelect.appendChild(option);
  });

  if (queries.length === 0) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = 'No precomputed queries loaded';
    els.querySelect.appendChild(option);
  }

  syncSqlInput();
}

function syncSqlInput() {
  const queryData = getCurrentQueryData();
  const sqlText = queryData?.sql || '-- select a query';
  if (sqlEditor) sqlEditor.setValue(sqlText);
  else els.sqlInput.value = sqlText;

  if (queryData && Number.isFinite(queryData.actual)) {
    els.statusText.textContent = `actual cardinality: ${queryData.actual.toLocaleString()}`;
  }
}

function normalizeLoadedQuery(raw) {
  if (!raw || typeof raw !== 'object') return null;
  return {
    sql: raw.sql,
    actual: Number.isFinite(Number(raw.actual)) ? Number(raw.actual) : undefined,
    estimates: raw.estimates && typeof raw.estimates === 'object' ? raw.estimates : {},
    xbound: raw.xbound && typeof raw.xbound === 'object' ? raw.xbound : {}
  };
}

function canonicalQueryName(benchmark, queryName) {
  const b = String(benchmark || '').toLowerCase();
  const q = String(queryName || '').trim();
  if (!q) return q;
  if (b === 'joblight' || b === 'job' || b === 'so-ceb' || b === 'so_full_ceb' || b === 'stats-ceb' || b === 'stats_ceb') {
    if (/^q\d+$/i.test(q)) return `Q${q.replace(/^q/i, '')}`;
    if (/^\d+$/.test(q)) return `Q${q}`;
  }
  return q;
}

async function ensureBenchmarkLoaded(benchmark) {
  if (loadedBenchmarks.has(benchmark)) return;
  loadedBenchmarks.add(benchmark);

  const estimateLoader = window.xbound?.loadPrecomputedEstimates;
  const workloadLoader = window.xbound?.loadWorkloadQueries;
  queryStore[benchmark] ||= {};

  try {
    if (typeof workloadLoader === 'function') {
      const workloadResult = await workloadLoader(benchmark);
      if (workloadResult?.ok && workloadResult.queries && typeof workloadResult.queries === 'object') {
        if (String(benchmark).toLowerCase() === 'joblight') {
          queryStore[benchmark] = {};
        }
        Object.entries(workloadResult.queries).forEach(([queryName, queryData]) => {
          const canonicalName = canonicalQueryName(benchmark, queryName);
          queryStore[benchmark][canonicalName] ||= { sql: '', actual: 0, estimates: {}, xbound: {} };
          if (typeof queryData?.sql === 'string' && queryData.sql) {
            queryStore[benchmark][canonicalName].sql = queryData.sql;
          }
        });
        els.statusText.textContent = `Loaded workload queries from ${workloadResult.sourcePath}`;
      }
    }

    if (typeof estimateLoader === 'function') {
      const estimateResult = await estimateLoader(benchmark);
      if (estimateResult?.ok && estimateResult.queries && typeof estimateResult.queries === 'object') {
        Object.entries(estimateResult.queries).forEach(([queryName, queryData]) => {
          const canonicalName = canonicalQueryName(benchmark, queryName);
          const loaded = normalizeLoadedQuery(queryData);
          if (!loaded) return;

          queryStore[benchmark][canonicalName] ||= { sql: '', actual: 0, estimates: {}, xbound: {} };
          const current = queryStore[benchmark][canonicalName];
          queryStore[benchmark][canonicalName] = {
            ...current,
            sql: loaded.sql || current.sql,
            actual: loaded.actual || current.actual,
            estimates: { ...(current.estimates || {}), ...(loaded.estimates || {}) },
            xbound: { ...(current.xbound || {}), ...(loaded.xbound || {}) }
          };
        });
        els.statusText.textContent = `Loaded precomputed estimates from ${estimateResult.sourcePath}`;
      }
    }
  } catch {
    // Keep mock data and stay silent on load failures.
  }
}

function bindEvents() {
  els.benchmarkSelect.addEventListener('change', async () => {
    customQueryData = null;
    await ensureBenchmarkLoaded(els.benchmarkSelect.value);
    updateQuerySelector();
    const entries = activeEntries();
    if (currentMode === 'run') renderQErrorBarPlot(entries);
    if (currentMode === 'leaderboard') renderLeaderboard(entries);
  });

  els.querySelect.addEventListener('change', () => {
    customQueryData = null;
    syncSqlInput();
    const entries = activeEntries();
    if (currentMode === 'run') renderQErrorBarPlot(entries);
    if (currentMode === 'leaderboard') renderLeaderboard(entries);
  });

  els.xboundToggle.addEventListener('change', () => {
    const entries = activeEntries();
    if (currentMode === 'run') renderQErrorBarPlot(entries);
    if (currentMode === 'leaderboard') renderLeaderboard(entries);
  });

  els.planSystemSelect.addEventListener('change', () => {
    if (currentMode === 'plan') renderPlanTree(els.planSystemSelect.value);
  });

  els.runBtn.addEventListener('click', async () => {
    setMode('run');
    const sql = getSqlText().trim();
    const queryData = queryStore[els.benchmarkSelect.value]?.[els.querySelect.value];
    const isCustom = !queryData || sql !== String(queryData.sql || '').trim();

    if (isCustom && sql && window.xbound?.estimateCustomQuery) {
      els.statusText.textContent = 'Estimating custom query...';
      try {
        const result = await window.xbound.estimateCustomQuery(els.benchmarkSelect.value, sql);
        if (result?.errors && Object.keys(result.errors).length) {
          console.error('[custom-query-estimation][errors]', result.errors);
        }
        const actual = Number(result?.actual);
        customQueryData = {
          sql,
          actual: Number.isFinite(actual) && actual > 0 ? actual : 1,
          estimates: result?.estimates || {},
          xbound: result?.xbound || {}
        };
        renderQErrorBarPlot(activeEntries());
        const numEntries = activeEntries().length;
        if (numEntries === 0) {
          els.statusText.textContent = 'No estimates produced. Check terminal logs for details.';
        } else if (result?.errors && Object.keys(result.errors).length) {
          els.statusText.textContent = 'Partial estimates produced. Check terminal logs for failures.';
        } else {
          els.statusText.textContent = 'Custom query estimated (duckdb/postgres/xbound)';
        }
      } catch (err) {
        console.error('[custom-query-estimation][failed]', err);
        customQueryData = null;
        renderQErrorBarPlot(activeEntries());
        els.statusText.textContent = 'Failed to estimate custom query. Check terminal logs.';
      }
      return;
    }

    customQueryData = null;
    renderQErrorBarPlot(activeEntries());
    els.statusText.textContent = 'Q-error bars refreshed';
  });

  els.planViewBtn.addEventListener('click', () => {
    setMode('plan');
    renderPlanTree(els.planSystemSelect.value);
    els.statusText.textContent = 'Plan view generated from JSON';
  });

  els.leaderboardBtn.addEventListener('click', () => {
    setMode('leaderboard');
    renderLeaderboard(activeEntries());
    els.statusText.textContent = 'Leaderboard updated';
  });

  window.addEventListener('resize', () => {
    if (currentMode === 'run') renderQErrorBarPlot(activeEntries());
  });
}

async function init() {
  els.appName.textContent = window.xbound?.appName || 'xBound';
  if (window.CodeMirror && els.sqlInput) {
    sqlEditor = window.CodeMirror.fromTextArea(els.sqlInput, {
      mode: 'text/x-sql',
      theme: 'neo',
      lineNumbers: true,
      lineWrapping: true,
      matchBrackets: true,
      viewportMargin: Infinity
    });
  }
  await ensureBenchmarkLoaded('JOBlight');
  await ensureBenchmarkLoaded('SO-CEB');
  await ensureBenchmarkLoaded('STATS-CEB');
  populateSelectors();
  bindEvents();
  setMode('run');
}

init();
