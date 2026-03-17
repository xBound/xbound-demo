const SYSTEMS = ['duckdb', 'postgres', 'fabric dw'];
const XBOUND_SUFFIX = ' xBound-ed';
const BENCHMARKS = ['JOBlight', 'SO-CEB', 'STATS-CEB'];
const SYSTEM_ICON_PATHS = {
  duckdb: '../icons/duckdb-icon.png',
  postgres: '../icons/postgres-icon.png',
  'fabric dw': '../icons/dw-icon.png',
  xbound: '../icons/xbound-icon.png'
};
const SYSTEM_LABELS = {
  duckdb: 'DuckDB',
  postgres: 'PostgreSQL',
  'fabric dw': 'Fabric DW',
  xbound: 'xBound-ed DBMS'
};
const SYSTEM_COLORS = {
  duckdb: '#FFF100',
  postgres: '#0064a5',
  'fabric dw': '#8ae8ff',
  xbound: '#D3D3D3'
};
const UI_FONT_FAMILY = '"Palatino Linotype", Palatino, "URW Palladio L", "Book Antiqua", serif';
const ESTIMATE_FONT = `15px ${UI_FONT_FAMILY}`;

let queryStore = Object.fromEntries(BENCHMARKS.map((name) => [name, {}]));
const loadedBenchmarks = new Set();
let customQueryData = null;
const systemIconCache = {};

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
  xboundParams: document.getElementById('xboundParams'),
  xboundParts: document.getElementById('xboundParts'),
  xboundL0Theta: document.getElementById('xboundL0Theta'),
  xboundHhTheta: document.getElementById('xboundHhTheta'),
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

function normalizeSql(sql) {
  return String(sql || '')
    .replace(/\s+/g, ' ')
    .replace(/\s*;\s*$/g, '')
    .trim()
    .toLowerCase();
}

function formatSql(sql) {
  const raw = String(sql || '').trim();
  if (!raw) return raw;
  return fallbackFormatSql(raw);
}

function isSelectCountQuery(sql) {
  const normalized = String(sql || '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
  return /^select\s+count\s*\(\s*\*\s*\)/.test(normalized);
}

function formatCardinality(value) {
  if (!Number.isFinite(value)) return String(value);
  const n = Math.ceil(value);
  if (n >= 1_000_000_000) return `${Math.ceil(n / 1_000_000_000)}B`;
  if (n >= 1_000_000) return `${Math.ceil(n / 1_000_000)}M`;
  if (n >= 999_500) return '1M';
  if (n >= 1_000) return `${Math.ceil(n / 1_000)}K`;
  return String(n);
}

function fallbackFormatSql(sql) {
  const compact = String(sql || '').replace(/\s+/g, ' ').trim();
  const breakBefore = [
    'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT',
    'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL JOIN', 'JOIN'
  ];
  let out = compact;
  breakBefore.forEach((kw) => {
    const pattern = new RegExp(`\\s+${kw}\\s+`, 'ig');
    out = out.replace(pattern, `\n${kw} `);
  });
  out = out.replace(/\s*,\s*/g, ',\n  ');
  out = out.replace(/\nFROM\s+/gi, '\nFROM\n  ');
  return out;
}

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

  // xBound is rendered as an overlay line, not separate bars.
  if (includeXBound) {}

  return entries;
}

function xboundOverlayEntry() {
  const queryData = getCurrentQueryData();
  if (!queryData) return null;
  const actual = queryData.actual;
  if (!Number.isFinite(actual) || actual === 0) return null;

  for (const system of SYSTEMS) {
    const estimate = queryData.xbound?.[system];
    if (!Number.isFinite(estimate)) continue;
    if (estimate === -1) {
      return {
        system: 'xbound',
        estimate,
        actual,
        unsupported: true
      };
    }
    if (estimate === 0) {
      return {
        system: 'xbound',
        estimate,
        actual,
        zeroLowerBound: true
      };
    }
    const q = Math.max(estimate / actual, actual / estimate);
    return {
      system: 'xbound',
      estimate,
      actual,
      qError: q,
      signedQError: estimate >= actual ? q : -q
    };
  }

  return null;
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

  const margin = { top: 64, right: 26, bottom: 52, left: 68 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;
  const baselineY = margin.top + height / 2;
  const xboundOverlay = xboundOverlayEntry();
  const maxAbsQ = Math.max(
    1.2,
    ...entries.map((e) => Math.abs(e.signedQError || e.qError || 1)),
    Math.abs(xboundOverlay?.signedQError || 1)
  );
  const domainMin = -maxAbsQ * 1.1;
  const domainMax = maxAbsQ * 1.1;

  const y = (value) => {
    const ratio = (value - domainMin) / (domainMax - domainMin);
    return margin.top + height - ratio * height;
  };

  const xStep = width / Math.max(1, SYSTEMS.length);
  const barWidth = Math.min(42, xStep * 0.58);
  const entryBySystem = new Map(entries.map((entry) => [systemKeyForEntry(entry.system), entry]));

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
    ctx.font = `12px ${UI_FONT_FAMILY}`;
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
  ctx.font = ESTIMATE_FONT;
  const actualBaseline = entries[0]?.actual;
  const baselineLabel = Number.isFinite(actualBaseline)
    ? `actual: ${formatCardinality(actualBaseline)}`
    : 'actual';
  ctx.fillText(baselineLabel, margin.left + 8, baselineY - 8);
  drawLegend(ctx, cssWidth / 2, 18);

  if (xboundOverlay) {
    if (xboundOverlay.unsupported) {
      ctx.fillStyle = '#a33a3a';
      ctx.font = `13px ${UI_FONT_FAMILY}`;
      const warningText = '⚠️ Query not supported in xBound';
      const textWidth = ctx.measureText(warningText).width;
      ctx.fillText(warningText, cssWidth - margin.right - textWidth - 4, margin.top - 12);
    } else if (xboundOverlay.zeroLowerBound) {
      ctx.fillStyle = '#a36a00';
      ctx.font = `13px ${UI_FONT_FAMILY}`;
      const warningText = '⚠️ Lower bound is 0';
      const textWidth = ctx.measureText(warningText).width;
      ctx.fillText(warningText, cssWidth - margin.right - textWidth - 4, margin.top - 12);
    } else {
    const lineY = y(xboundOverlay.signedQError);
    ctx.strokeStyle = SYSTEM_COLORS.xbound;
    ctx.lineWidth = 5;
    ctx.beginPath();
    ctx.moveTo(margin.left, lineY);
    ctx.lineTo(cssWidth - margin.right, lineY);
    ctx.stroke();

    const icon = loadSystemIcon('xbound');
    if (icon && icon.complete && icon.naturalWidth > 0) {
      const iconSize = 22;
      const estimateLabel = Number.isFinite(xboundOverlay.estimate)
        ? formatCardinality(xboundOverlay.estimate)
        : String(xboundOverlay.estimate);
      const lowerBoundLabel = `lower bound: ${estimateLabel}`;
      ctx.fillStyle = '#1f2a4d';
      ctx.font = ESTIMATE_FONT;
      const gap = 8;
      const textWidth = ctx.measureText(lowerBoundLabel).width;
      const pairWidth = iconSize + gap + textWidth;
      const minPairX = margin.left + 4;
      const maxPairX = cssWidth - margin.right - pairWidth - 4;
      const preferredPairX = cssWidth - margin.right - pairWidth - 8;
      const pairX = Math.max(minPairX, Math.min(maxPairX, preferredPairX));
      const iconX = pairX;
      const textX = iconX + iconSize + gap;
      const iconY = lineY - iconSize - 6;
      ctx.drawImage(icon, iconX, iconY, iconSize, iconSize);
      ctx.fillText(lowerBoundLabel, textX, lineY - 10);
    }
    }
  }

  SYSTEMS.forEach((system, idx) => {
    const entry = entryBySystem.get(system);
    if (!entry) return;
    const centerX = margin.left + xStep * idx + xStep / 2;
    const barTop = y(entry.signedQError || entry.qError);
    const barHeight = Math.abs(barTop - baselineY);
    const isXBound = entry.system.includes(XBOUND_SUFFIX);
    const colorKey = isXBound ? 'xbound' : systemKeyForEntry(entry.system);
    const accent = SYSTEM_COLORS[colorKey] || '#1461ff';

    ctx.fillStyle = accent;
    ctx.globalAlpha = 0.25;
    const barY1 = Math.min(barTop, baselineY);
    const barY2 = Math.max(barTop, baselineY);
    ctx.fillRect(centerX - barWidth / 2, barY1, barWidth, Math.max(1, barHeight));
    ctx.globalAlpha = 1;

    if (xboundOverlay && !xboundOverlay.unsupported && !xboundOverlay.zeroLowerBound && Number.isFinite(xboundOverlay.estimate) && entry.estimate < xboundOverlay.estimate) {
      const lineY = y(xboundOverlay.signedQError);
      const overflowTop = Math.max(lineY, barY1);
      const overflowBottom = barY2;
      if (overflowBottom > overflowTop) {
        ctx.fillStyle = '#ff4d4f';
        ctx.globalAlpha = 0.5;
        ctx.fillRect(centerX - barWidth / 2, overflowTop, barWidth, overflowBottom - overflowTop);
        ctx.globalAlpha = 1;
      }
    }

    ctx.strokeStyle = '#000000';
    ctx.lineWidth = 2;
    ctx.strokeRect(centerX - barWidth / 2, Math.min(barTop, baselineY), barWidth, Math.max(1, barHeight));

    ctx.fillStyle = '#1f2a4d';
    ctx.font = ESTIMATE_FONT;
    const qLabelY = (entry.signedQError || entry.qError) >= 0 ? barTop - 6 : barTop + 15;
    const estimateLabel = Number.isFinite(entry.estimate)
      ? formatCardinality(entry.estimate)
      : String(entry.estimate);
    const estimateLabelWidth = ctx.measureText(estimateLabel).width;
    ctx.fillText(estimateLabel, centerX - estimateLabelWidth / 2, qLabelY);

    const hasComparableLowerBound = Boolean(
      xboundOverlay &&
      !xboundOverlay.unsupported &&
      !xboundOverlay.zeroLowerBound &&
      Number.isFinite(xboundOverlay.estimate)
    );
    if (hasComparableLowerBound) {
      const symbol = entry.estimate < xboundOverlay.estimate ? '🤦' : '👌';
      const symbolY = (entry.signedQError || entry.qError) >= 0 ? qLabelY - 22 : qLabelY + 22;
      ctx.font = `21px ${UI_FONT_FAMILY}`;
      const symbolWidth = ctx.measureText(symbol).width;
      ctx.fillText(symbol, centerX - symbolWidth / 2, symbolY);
      ctx.font = ESTIMATE_FONT;
    }
  });
}

function systemKeyForEntry(systemLabel) {
  const s = String(systemLabel || '').toLowerCase();
  if (s.includes('duckdb')) return 'duckdb';
  if (s.includes('postgres')) return 'postgres';
  if (s.includes('fabric')) return 'fabric dw';
  return null;
}

function loadSystemIcon(key) {
  if (!key || !SYSTEM_ICON_PATHS[key]) return null;
  if (systemIconCache[key]) return systemIconCache[key];
  const img = new Image();
  img.src = SYSTEM_ICON_PATHS[key];
  img.onload = () => {
    if (currentMode === 'run') renderQErrorBarPlot(activeEntries());
  };
  systemIconCache[key] = img;
  return img;
}

function drawLegend(ctx, centerX, startY) {
  const keys = ['duckdb', 'postgres', 'fabric dw'];

  ctx.font = `14px ${UI_FONT_FAMILY}`;
  const iconSize = 22;
  const itemGap = 28;
  const iconGap = 8;

  const itemWidths = keys.map((key) => {
    const label = SYSTEM_LABELS[key] || key;
    return iconSize + iconGap + ctx.measureText(label).width;
  });
  const totalWidth = itemWidths.reduce((a, b) => a + b, 0) + itemGap * Math.max(0, keys.length - 1);

  let x = centerX - totalWidth / 2;
  keys.forEach((key) => {
    const icon = loadSystemIcon(key);
    const label = SYSTEM_LABELS[key] || key;

    if (icon && icon.complete && icon.naturalWidth > 0) {
      ctx.drawImage(icon, x, startY + 1, iconSize, iconSize);
      x += iconSize + iconGap;
    }

    ctx.fillStyle = '#1f2a4d';
    ctx.font = `14px ${UI_FONT_FAMILY}`;
    ctx.fillText(label, x, startY + 18);
    x += ctx.measureText(label).width + itemGap;
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
    li.textContent = `${entry.system}: q-error ${entry.qError.toFixed(2)}x (estimate ${formatCardinality(entry.estimate)}, actual ${formatCardinality(entry.actual)})`;
    els.leaderboardList.appendChild(li);
  });
}

function activeEntries() {
  return buildEstimateEntries(true);
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
  const sqlText = formatSql(queryData?.sql || '-- select a query');
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

function xboundParamOptionsForBenchmark(benchmark) {
  if (String(benchmark).toLowerCase() === 'so-ceb') {
    return {
      parts: Number(els.xboundParts?.value || 16),
      l0Theta: Number(els.xboundL0Theta?.value || 8),
      hhTheta: Number(els.xboundHhTheta?.value || 12)
    };
  }
  return null;
}

function xboundParamCacheKey(benchmark) {
  const params = xboundParamOptionsForBenchmark(benchmark);
  return `${benchmark}|${JSON.stringify(params || {})}`;
}

function updateXboundParamsState() {
  if (!els.xboundParams) return;
}

async function ensureBenchmarkLoaded(benchmark) {
  const cacheKey = xboundParamCacheKey(benchmark);
  if (loadedBenchmarks.has(cacheKey)) return;
  loadedBenchmarks.add(cacheKey);

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
      const estimateResult = await estimateLoader(benchmark, xboundParamOptionsForBenchmark(benchmark));
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
    updateXboundParamsState();
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

  [els.xboundParts, els.xboundL0Theta, els.xboundHhTheta].forEach((el) => {
    if (!el) return;
    el.addEventListener('change', async () => {
      if (els.benchmarkSelect.value !== 'SO-CEB') return;
      customQueryData = null;
      await ensureBenchmarkLoaded('SO-CEB');
      updateQuerySelector();
      const entries = activeEntries();
      if (currentMode === 'run') renderQErrorBarPlot(entries);
      if (currentMode === 'leaderboard') renderLeaderboard(entries);
    });
  });

  els.planSystemSelect.addEventListener('change', () => {
    if (currentMode === 'plan') renderPlanTree(els.planSystemSelect.value);
  });

  els.runBtn.addEventListener('click', async () => {
    setMode('run');
    const sql = getSqlText().trim();
    if (!isSelectCountQuery(sql)) {
      window.alert('Please keep the query as SELECT COUNT(*). Do not modify that part.');
      els.statusText.textContent = 'Run blocked: only SELECT COUNT(*) queries are supported.';
      return;
    }
    const prettySql = formatSql(sql);
    if (sqlEditor) sqlEditor.setValue(prettySql);
    else els.sqlInput.value = prettySql;
    const queryData = queryStore[els.benchmarkSelect.value]?.[els.querySelect.value];
    const isCustom = !queryData || normalizeSql(prettySql) !== normalizeSql(queryData.sql || '');

    if (isCustom && prettySql && window.xbound?.estimateCustomQuery) {
      els.statusText.textContent = 'Estimating custom query...';
      try {
        const result = await window.xbound.estimateCustomQuery(
          els.benchmarkSelect.value,
          prettySql,
          xboundParamOptionsForBenchmark(els.benchmarkSelect.value),
          els.querySelect.value
        );
        if (result?.errors && Object.keys(result.errors).length) {
          console.error('[custom-query-estimation][errors]', result.errors);
        }
        const actual = Number(result?.actual);
        customQueryData = {
          sql: prettySql,
          actual: Number.isFinite(actual) && actual > 0
            ? actual
            : (Number.isFinite(Number(queryData?.actual)) && Number(queryData.actual) > 0 ? Number(queryData.actual) : 0),
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
      viewportMargin: Infinity,
      extraKeys: {
        'Ctrl-Shift-F': (cm) => cm.setValue(formatSql(cm.getValue())),
        'Cmd-Shift-F': (cm) => cm.setValue(formatSql(cm.getValue()))
      }
    });
  }
  await ensureBenchmarkLoaded('JOBlight');
  await ensureBenchmarkLoaded('SO-CEB');
  await ensureBenchmarkLoaded('STATS-CEB');
  populateSelectors();
  updateXboundParamsState();
  bindEvents();
  setMode('run');
}

init();
