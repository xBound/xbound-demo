const XBOUND_SUFFIX = ' xBound-ed';
const BENCHMARKS = ['JOBlight', 'SO-CEB', 'STATS-CEB'];
const IS_ELECTRON = typeof window.xbound !== 'undefined';
const SYSTEMS = IS_ELECTRON ? ['duckdb', 'postgres', 'fabric dw'] : ['duckdb', 'postgres'];
function webBasePath() {
  const pathname = window.location.pathname || '/';
  if (pathname.endsWith('/')) return pathname;
  if (pathname.endsWith('.html')) return pathname.slice(0, pathname.lastIndexOf('/') + 1);
  return `${pathname}/`;
}
const WEB_BASE_PATH = IS_ELECTRON ? '' : webBasePath();
const ICON_BASE_PATH = IS_ELECTRON ? '../icons' : `${WEB_BASE_PATH}icons`;
function resolveAssetPath(pathname) {
  if (IS_ELECTRON) return pathname;
  try {
    return new URL(pathname, window.location.href).toString();
  } catch {
    return pathname;
  }
}
const SYSTEM_ICON_PATHS = {
  duckdb: resolveAssetPath(`${ICON_BASE_PATH}/duckdb-icon.png`),
  postgres: resolveAssetPath(`${ICON_BASE_PATH}/postgres-icon.png`),
  'fabric dw': resolveAssetPath(`${ICON_BASE_PATH}/dw-icon.png`),
  xbound: resolveAssetPath(`${ICON_BASE_PATH}/xbound-icon.png`)
};
const SYSTEM_ICON_HEAD_SCALE = {
  duckdb: 1.08,
  postgres: 0.92,
  'fabric dw': 0.9,
  xbound: 0.94
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
const PLOT_FONT = {
  estimatePx: IS_ELECTRON ? 15 : 17,
  tickPx: IS_ELECTRON ? 12 : 14,
  legendPx: IS_ELECTRON ? 14 : 16,
  warningPx: IS_ELECTRON ? 13 : 15
};
const ESTIMATE_FONT = `${PLOT_FONT.estimatePx}px ${UI_FONT_FAMILY}`;
const WEB_DATA_BASE = IS_ELECTRON ? './data/benchmarks' : `${WEB_BASE_PATH}data/benchmarks`;

let queryStore = Object.fromEntries(BENCHMARKS.map((name) => [name, {}]));
const loadedBenchmarks = new Set();
const benchmarkDataCache = new Map();
let customQueryData = null;
const systemIconCache = {};
let iconRedrawRaf = 0;

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
  appShell: document.querySelector('.app-shell'),
  navRail: document.querySelector('.nav-rail'),
  appName: document.getElementById('appName'),
  sqlPanel: document.querySelector('.sql-panel'),
  sqlEditorWrap: document.querySelector('.sql-editor-wrap'),
  controlsPanel: document.querySelector('.controls-panel'),
  xboundPanel: document.querySelector('.xbound-panel'),
  benchmarkSelect: document.getElementById('benchmarkSelect'),
  querySelect: document.getElementById('querySelect'),
  queryControls: document.getElementById('queryControls'),
  sqlInput: document.getElementById('sqlInput'),
  statusText: document.getElementById('statusText'),
  xboundParams: document.getElementById('xboundParams'),
  xboundParts: document.getElementById('xboundParts'),
  xboundL0Theta: document.getElementById('xboundL0Theta'),
  xboundHhTheta: document.getElementById('xboundHhTheta'),
  xboundPartsValue: document.getElementById('xboundPartsValue'),
  xboundL0ThetaValue: document.getElementById('xboundL0ThetaValue'),
  xboundHhThetaValue: document.getElementById('xboundHhThetaValue'),
  xboundWarning: document.getElementById('xboundWarning'),
  planSystemSelect: document.getElementById('planSystemSelect'),
  planControls: document.getElementById('planControls'),
  motivationBtn: document.getElementById('motivationBtn'),
  dashboardTabBtn: document.getElementById('dashboardTabBtn'),
  runBtn: document.getElementById('runBtn'),
  planViewBtn: document.getElementById('planViewBtn'),
  leaderboardBtn: document.getElementById('leaderboardBtn'),
  motivationPanel: document.getElementById('motivationPanel'),
  runPanel: document.getElementById('runPanel'),
  planPanel: document.getElementById('planPanel'),
  leaderboardPanel: document.getElementById('leaderboardPanel'),
  motivationPrevBtn: document.getElementById('motivationPrevBtn'),
  motivationNextBtn: document.getElementById('motivationNextBtn'),
  motivationSlides: Array.from(document.querySelectorAll('.motivation-slide')),
  chartLegend: document.getElementById('chartLegend'),
  chartCanvas: document.getElementById('chartCanvas'),
  planTree: document.getElementById('planTree'),
  leaderboardList: document.getElementById('leaderboardList')
};

let currentMode = 'motivation';
let motivationSlideIndex = 0;
let sqlEditor = null;
const supportsCustomQuery = typeof window.xbound?.estimateCustomQuery === 'function';
let leaderboardTab = 'sanity';
let qerrorBoundMode = { xbound: true, lpbound: true };
const benchmarkLoadWarnings = new Map();
let xboundSliderRefreshTimer = 0;
let xboundSliderRefreshSeq = 0;
let syncedPanelHeight = 0;
const MAX_SYNCED_PANEL_HEIGHT = 220;

function benchmarkSlug(benchmark) {
  const b = String(benchmark || '').trim().toLowerCase();
  if (b === 'job' || b === 'joblight') return 'joblight';
  if (b === 'so-ceb' || b === 'so_ceb' || b === 'so_full_ceb') return 'so_full_ceb';
  if (b === 'stats-ceb' || b === 'stats_ceb') return 'stats_ceb';
  return b.replace(/[^a-z0-9_]+/g, '_');
}

function queryKey(obj) {
  return (
    obj.tag ||
    obj.query ||
    obj.query_name ||
    obj.query_id ||
    obj.id ||
    obj.name ||
    null
  );
}

function numericValue(...values) {
  for (const value of values) {
    const num = Number(value);
    if (Number.isFinite(num)) return num;
  }
  return null;
}

async function fetchText(pathname) {
  const response = await fetch(pathname, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`Failed to load ${pathname}: ${response.status}`);
  }
  return response.text();
}

function parseJsonl(content) {
  const rows = [];
  String(content || '')
    .split(/\r?\n/)
    .forEach((rawLine) => {
      const line = rawLine.trim();
      if (!line) return;
      try {
        rows.push(JSON.parse(line));
      } catch {
        // Ignore malformed lines.
      }
    });
  return rows;
}

async function loadWorkloadQueriesWeb(benchmark) {
  const alias = benchmarkSlug(benchmark);
  const fileByAlias = {
    joblight: 'joblight-queries.jsonl',
    so_full_ceb: 'so_full_ceb-queries00.jsonl',
    stats_ceb: 'stats_ceb-queries.jsonl'
  };
  const fileName = fileByAlias[alias];
  if (!fileName) {
    throw new Error(`Unsupported benchmark for workload load: ${benchmark}`);
  }

  const sourcePath = `${WEB_DATA_BASE}/workloads/${alias}/${fileName}`;
  const rows = parseJsonl(await fetchText(sourcePath));
  const queries = {};

  rows.forEach((obj) => {
    const key = queryKey(obj);
    if (!key) return;
    queries[key] ||= {};
    if (typeof obj.sql === 'string' && obj.sql) queries[key].sql = obj.sql;
    const actual = numericValue(obj.actual, obj.act, obj.actual_cardinality, obj.true_cardinality, obj.ground_truth);
    if (actual !== null) queries[key].actual = actual;
  });

  return { ok: true, sourcePath, queries };
}

function soCebXboundFileName(xboundParams) {
  const parts = Number(xboundParams?.parts);
  const l0Theta = Number(xboundParams?.l0Theta);
  const hhTheta = Number(xboundParams?.hhTheta);
  if (!Number.isFinite(parts) || !Number.isFinite(l0Theta) || !Number.isFinite(hhTheta)) {
    return 'xbound::so_full_ceb-queries00_host=hausberg_parts=16_ns=1_ub=0_l0-theta=8_hh-theta=12_mcv=1024.jsonl';
  }
  return `xbound::so_full_ceb-queries00_host=hausberg_parts=${Math.trunc(parts)}_ns=1_ub=0_l0-theta=${Math.trunc(l0Theta)}_hh-theta=${Math.trunc(hhTheta)}_mcv=1024.jsonl`;
}

async function loadPrecomputedEstimatesWeb(benchmark, xboundParams) {
  const alias = benchmarkSlug(benchmark);
  const dirPath = `${WEB_DATA_BASE}/est/${alias}`;
  const systemFilesByAlias = {
    joblight: [
      'duckdb::joblight-queries.jsonl',
      'postgres::joblight-queries.jsonl',
      'dw::joblight-queries.jsonl',
      'xbound::joblight-queries_host=hausberg_parts=16_ns=1_ub=0_l0-theta=8_hh-theta=12_mcv=1024.jsonl',
      'lpbound-10::joblight-queries.jsonl'
    ],
    so_full_ceb: [
      'duckdb::so_full_ceb-queries00.jsonl',
      'postgres::so_full_ceb-queries00.jsonl',
      'dw::so_full_ceb-queries00.jsonl'
    ],
    stats_ceb: []
  };

  const files = [...(systemFilesByAlias[alias] || [])];
  if (alias === 'so_full_ceb') {
    files.push(soCebXboundFileName(xboundParams));
  }

  const queries = {};
  const upsert = (obj) => {
    const key = queryKey(obj);
    if (!key) return null;
    queries[key] ||= { estimates: {}, xbound: {}, lpbound: {} };
    const entry = queries[key];
    if (!entry.sql && typeof obj.sql === 'string') entry.sql = obj.sql;
    if (!entry.actual) {
      entry.actual = numericValue(obj.actual, obj.act, obj.actual_cardinality, obj.true_cardinality, obj.ground_truth);
    }
    return entry;
  };

  for (const fileName of files) {
    const sourcePath = `${dirPath}/${fileName}`;
    let text;
    try {
      text = await fetchText(sourcePath);
    } catch (err) {
      if (fileName.startsWith('dw::')) continue;
      if (fileName.startsWith('xbound::')) {
        const missingError = new Error(`Missing xBound lower-bound file for ${benchmark}: ${fileName}`);
        missingError.code = 'MISSING_XBOUND_FILE';
        missingError.benchmark = benchmark;
        missingError.fileName = fileName;
        throw missingError;
      }
      throw err;
    }
    const rows = parseJsonl(text);
    rows.forEach((obj) => {
      const entry = upsert(obj);
      if (!entry) return;
      if (fileName.startsWith('duckdb::')) {
        const val = numericValue(obj.duckdb);
        if (val !== null) entry.estimates.duckdb = val;
      } else if (fileName.startsWith('postgres::')) {
        const val = numericValue(obj.postgres);
        if (val !== null) entry.estimates.postgres = val;
      } else if (fileName.startsWith('dw::')) {
        const val = numericValue(obj.dw);
        if (val !== null) entry.estimates['fabric dw'] = val;
      } else if (fileName.startsWith('xbound::')) {
        const val = numericValue(obj.xbound, obj?.meta?.best?.val);
        if (val !== null) {
          entry.xbound.duckdb = val;
          entry.xbound.postgres = val;
          entry.xbound['fabric dw'] = val;
        }
      } else if (fileName.startsWith('lpbound')) {
        const lpKey = Object.keys(obj).find((k) => /^lpbound/i.test(String(k)));
        const val = numericValue(lpKey ? obj[lpKey] : undefined, obj.lpbound, obj.upper_bound, obj.ub);
        if (val !== null) {
          entry.lpbound.duckdb = val;
          entry.lpbound.postgres = val;
          entry.lpbound['fabric dw'] = val;
        }
      }
    });
  }
  return { ok: true, sourcePath: dirPath, queries };
}

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

function benchmarkSupportsXboundParamVariants(benchmark) {
  return String(benchmark || '').toLowerCase() === 'so-ceb';
}

function currentXboundSliderParams() {
  return {
    parts: discreteSliderValue(els.xboundParts, 16),
    l0Theta: discreteSliderValue(els.xboundL0Theta, 8),
    hhTheta: discreteSliderValue(els.xboundHhTheta, 12)
  };
}

function isDefaultXboundSliderParams(params) {
  return Number(params?.parts) === 16 && Number(params?.l0Theta) === 8 && Number(params?.hhTheta) === 12;
}

function currentXboundAvailabilityWarning() {
  if (customQueryData) return '';
  const benchmark = els.benchmarkSelect.value;
  const benchmarkWarning = benchmarkLoadWarnings.get(benchmark);
  if (benchmarkWarning && benchmarkWarning.includes('missing xBound file')) {
    return benchmarkWarning;
  }
  const sliderParams = currentXboundSliderParams();
  const benchmarkKey = String(benchmark || '').trim().toLowerCase();
  if (benchmarkKey === 'joblight' && !isDefaultXboundSliderParams(sliderParams)) {
    return `missing xBound file for ${benchmark} (parts=${sliderParams.parts}, l0-theta=${sliderParams.l0Theta}, hh-theta=${sliderParams.hhTheta})`;
  }
  if ((benchmarkKey === 'stats-ceb' || benchmarkKey === 'stats_ceb')) {
    return `missing xBound file for ${benchmark}`;
  }
  const queryData = getCurrentQueryData();
  if (!queryData) return '';

  const hasAnyXboundEstimate = SYSTEMS.some((system) => Number.isFinite(Number(queryData.xbound?.[system])));
  if (hasAnyXboundEstimate) return '';

  if (!benchmarkSupportsXboundParamVariants(benchmark)) {
    return 'xBound estimates are not available for this benchmark/query.';
  }
  return 'xBound estimates are not available for the current benchmark/query.';
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

function lpboundOverlayEntry() {
  const queryData = getCurrentQueryData();
  if (!queryData) return null;
  const actual = queryData.actual;
  if (!Number.isFinite(actual) || actual === 0) return null;

  for (const system of SYSTEMS) {
    const estimate = queryData.lpbound?.[system];
    if (!Number.isFinite(estimate) || estimate <= 0) continue;
    const q = Math.max(estimate / actual, actual / estimate);
    return {
      system: 'lpbound',
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
  if (!ctx) return;
  const dpr = window.devicePixelRatio || 1;
  const parentWidth = canvas.parentElement?.clientWidth || 900;
  const cssWidth = Math.max(320, parentWidth - 2);
  const cssHeight = Math.max(240, canvas.clientHeight || 340);

  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  const margin = { top: 88, right: 26, bottom: 76, left: 68 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;
  const xboundAvailabilityWarning = currentXboundAvailabilityWarning();
  const baselineY = margin.top + height / 2;
  const xboundOverlay = xboundOverlayEntry();
  const lpboundOverlay = lpboundOverlayEntry();
  const maxAbsQ = Math.max(
    1.2,
    ...entries.map((e) => Math.abs(e.signedQError || e.qError || 1)),
    Math.abs(xboundOverlay?.signedQError || 1),
    Math.abs(lpboundOverlay?.signedQError || 1)
  );
  const toSignedLog = (value) => {
    const v = Number(value);
    if (!Number.isFinite(v) || Math.abs(v) < 1) return 0;
    return Math.sign(v) * Math.log10(Math.max(1, Math.abs(v)));
  };
  const maxAbsLog = Math.max(0.35, Math.log10(maxAbsQ) * 1.05);
  const domainMin = -maxAbsLog;
  const domainMax = maxAbsLog;

  const y = (signedQError) => {
    const t = toSignedLog(signedQError);
    const ratio = (t - domainMin) / (domainMax - domainMin);
    return margin.top + height - ratio * height;
  };

  const clusterWidth = width * (SYSTEMS.length <= 2 ? 0.45 : 0.6);
  const clusterStartX = margin.left + (width - clusterWidth) / 2;
  const xStep = clusterWidth / Math.max(1, SYSTEMS.length);
  const stemWidth = 2;
  const iconHeadSize = Math.max(28, Math.min(44, xStep * 0.42));
  const entryBySystem = new Map(entries.map((entry) => [systemKeyForEntry(entry.system), entry]));
  const benchmarkWarning = benchmarkLoadWarnings.get(els.benchmarkSelect.value) || '';
  const boundLineColor = '#b8b8b8';

  ctx.clearRect(0, 0, cssWidth, cssHeight);
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, cssWidth, cssHeight);
  if (xboundAvailabilityWarning) return;

  const maxExp = Math.max(1, Math.ceil(Math.log10(maxAbsQ)));
  const tickQs = [1];
  for (let exp = 1; exp <= maxExp; exp += 1) tickQs.push(10 ** exp);
  const ticks = [
    ...tickQs.filter((q) => q > 1).map((q) => -q).reverse(),
    1,
    ...tickQs.filter((q) => q > 1)
  ];

  ctx.strokeStyle = '#cdd6ea';
  ctx.lineWidth = 1;
  const yTickLabelGap = 10;
  const yAxisLabelGap = 14;
  const expFont = `${PLOT_FONT.tickPx}px ${UI_FONT_FAMILY}`;
  let maxTickLabelWidth = 0;
  ticks.forEach((tick) => {
    const absTick = Math.abs(tick);
    const exp = Math.round(Math.log10(absTick));
    const baseText = '10';
    const expText = `${exp}`;
    ctx.font = `${PLOT_FONT.tickPx}px ${UI_FONT_FAMILY}`;
    const baseWidth = ctx.measureText(baseText).width;
    ctx.font = expFont;
    const expWidth = ctx.measureText(expText).width;
    const totalLabelWidth = baseWidth + 1 + expWidth;
    if (totalLabelWidth > maxTickLabelWidth) maxTickLabelWidth = totalLabelWidth;
  });

  ticks.forEach((tick) => {
    const yy = tick === 1 ? baselineY : y(tick);
    ctx.beginPath();
    ctx.moveTo(margin.left, yy);
    ctx.lineTo(cssWidth - margin.right, yy);
    ctx.stroke();

    ctx.fillStyle = '#6b718c';
    ctx.font = `${PLOT_FONT.tickPx}px ${UI_FONT_FAMILY}`;
    const absTick = Math.abs(tick);
    const exp = Math.round(Math.log10(absTick));
    const baseText = '10';
    const expText = `${exp}`;
    const baseY = yy + 4;
    ctx.font = `${PLOT_FONT.tickPx}px ${UI_FONT_FAMILY}`;
    const baseWidth = ctx.measureText(baseText).width;
    ctx.font = expFont;
    const expWidth = ctx.measureText(expText).width;
    const totalLabelWidth = baseWidth + 1 + expWidth;
    const baseX = margin.left - yTickLabelGap - totalLabelWidth;

    ctx.fillText(baseText, baseX, baseY);
    ctx.font = expFont;
    ctx.fillText(expText, baseX + baseWidth + 1, baseY - Math.max(4, Math.round(PLOT_FONT.tickPx * 0.32)));
  });

  const yLabelX = margin.left - yTickLabelGap - maxTickLabelWidth - yAxisLabelGap;
  const yLabelY = margin.top + height / 2;
  ctx.save();
  ctx.translate(yLabelX, yLabelY);
  ctx.rotate(-Math.PI / 2);
  ctx.fillStyle = '#1e2b54';
  ctx.font = `${PLOT_FONT.legendPx}px ${UI_FONT_FAMILY}`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('Result size Q-error [log scale]', 0, 0);
  ctx.restore();

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
  if (benchmarkWarning) {
    ctx.fillStyle = '#a33a3a';
    ctx.font = `${PLOT_FONT.warningPx}px ${UI_FONT_FAMILY}`;
    ctx.fillText(`⚠️ ${benchmarkWarning}`, margin.left + 8, margin.top - 12);
  }

  const hasLowerBoundLine = Boolean(
    xboundOverlay &&
    !xboundOverlay.unsupported &&
    !xboundOverlay.zeroLowerBound &&
    Number.isFinite(xboundOverlay.signedQError)
  );
  const hasZeroLowerBound = Boolean(
    xboundOverlay &&
    !xboundOverlay.unsupported &&
    xboundOverlay.zeroLowerBound
  );
  const hasUpperBoundLine = Boolean(
    lpboundOverlay &&
    Number.isFinite(lpboundOverlay.signedQError)
  );
  if ((hasLowerBoundLine || hasZeroLowerBound) && hasUpperBoundLine) {
    const lowerY = hasZeroLowerBound ? (margin.top + height) : y(xboundOverlay.signedQError);
    const upperY = y(lpboundOverlay.signedQError);
    const topY = Math.min(lowerY, upperY);
    const bandHeight = Math.max(1, Math.abs(upperY - lowerY));
    const plotWidth = cssWidth - margin.left - margin.right;
    const plotTop = margin.top;
    const plotHeight = height;
    ctx.fillStyle = 'rgba(245, 150, 150, 0.10)';
    ctx.fillRect(margin.left, plotTop, plotWidth, Math.max(0, topY - plotTop));
    ctx.fillRect(margin.left, topY + bandHeight, plotWidth, Math.max(0, plotTop + plotHeight - (topY + bandHeight)));
    ctx.fillStyle = 'rgba(156, 216, 156, 0.18)';
    ctx.fillRect(margin.left, topY, plotWidth, bandHeight);
  }

  if (xboundOverlay) {
    if (xboundOverlay.unsupported) {
      ctx.fillStyle = '#a33a3a';
      ctx.font = `${PLOT_FONT.warningPx}px ${UI_FONT_FAMILY}`;
      const warningText = '⚠️ Query not supported in xBound';
      const textWidth = ctx.measureText(warningText).width;
      ctx.fillText(warningText, cssWidth - margin.right - textWidth - 4, margin.top - 24);
    } else if (xboundOverlay.zeroLowerBound) {
      ctx.fillStyle = '#a36a00';
      ctx.font = `${PLOT_FONT.warningPx}px ${UI_FONT_FAMILY}`;
      const warningText = '⚠️ Lower bound is 0';
      const textWidth = ctx.measureText(warningText).width;
      ctx.fillText(warningText, cssWidth - margin.right - textWidth - 4, margin.top - 24);
    } else {
    const lineY = y(xboundOverlay.signedQError);
    ctx.strokeStyle = boundLineColor;
    ctx.lineWidth = 5;
    ctx.beginPath();
    ctx.moveTo(margin.left, lineY);
    ctx.lineTo(cssWidth - margin.right, lineY);
    ctx.stroke();

    const icon = loadSystemIcon('xbound');
    if (icon && icon._xboundState === 'ready' && icon.complete && icon.naturalWidth > 0) {
      const iconSize = 20;
      const gap = 7;
      const estimateLabel = Number.isFinite(xboundOverlay.estimate)
        ? formatCardinality(xboundOverlay.estimate)
        : String(xboundOverlay.estimate);
      const lowerBoundLabel = `lower bound: ${estimateLabel}`;
      ctx.fillStyle = '#1f2a4d';
      ctx.font = ESTIMATE_FONT;
      const textWidth = ctx.measureText(lowerBoundLabel).width;
      const pairWidth = iconSize + gap + textWidth;
      const pairX = cssWidth - margin.right - pairWidth - 8;
      const textY = lineY + 22;
      const iconY = textY - iconSize + 5;
      ctx.drawImage(icon, pairX, iconY, iconSize, iconSize);
      ctx.fillText(lowerBoundLabel, pairX + iconSize + gap, textY);
    }
    }
  }
  if (lpboundOverlay) {
    const lineY = y(lpboundOverlay.signedQError);
    ctx.strokeStyle = boundLineColor;
    ctx.lineWidth = 5;
    ctx.beginPath();
    ctx.moveTo(margin.left, lineY);
    ctx.lineTo(cssWidth - margin.right, lineY);
    ctx.stroke();

    const estimateLabel = Number.isFinite(lpboundOverlay.estimate)
      ? formatCardinality(lpboundOverlay.estimate)
      : String(lpboundOverlay.estimate);
    const upperBoundLabel = `🛖 upper bound: ${estimateLabel}`;
    ctx.fillStyle = '#1f2a4d';
    ctx.font = ESTIMATE_FONT;
    const textWidth = ctx.measureText(upperBoundLabel).width;
    const upperLabelX = cssWidth - margin.right - textWidth - 8;
    ctx.fillText(upperBoundLabel, upperLabelX, lineY - 10);
  }

  SYSTEMS.forEach((system, idx) => {
    const entry = entryBySystem.get(system);
    if (!entry) return;
    const centerX = clusterStartX + xStep * idx + xStep / 2;
    const barTop = y(entry.signedQError || entry.qError);
    const isXBound = entry.system.includes(XBOUND_SUFFIX);
    const colorKey = isXBound ? 'xbound' : systemKeyForEntry(entry.system);
    const icon = loadSystemIcon(colorKey);
    const stemY1 = Math.min(barTop, baselineY);
    const stemY2 = Math.max(barTop, baselineY);

    ctx.strokeStyle = '#000000';
    ctx.lineWidth = stemWidth;
    ctx.beginPath();
    ctx.moveTo(centerX, stemY1);
    ctx.lineTo(centerX, stemY2);
    ctx.stroke();

    // Icon-only lollipop head (no circular container).
    ctx.save();
    if (icon && icon._xboundState === 'ready' && icon.complete && icon.naturalWidth > 0) {
      const iconScale = SYSTEM_ICON_HEAD_SCALE[colorKey] || 0.95;
      const iconSize = iconHeadSize * iconScale;
      ctx.drawImage(icon, centerX - iconSize / 2, barTop - iconSize / 2, iconSize, iconSize);
    } else {
      ctx.beginPath();
      ctx.arc(centerX, barTop, iconHeadSize * 0.28, 0, Math.PI * 2);
      ctx.closePath();
      ctx.fillStyle = '#000000';
      ctx.globalAlpha = 0.92;
      ctx.fill();
      ctx.globalAlpha = 1;
    }

    ctx.restore();

    ctx.font = ESTIMATE_FONT;
    const qLabelY = (entry.signedQError || entry.qError) >= 0
      ? barTop - iconHeadSize / 2 - 10
      : barTop + iconHeadSize / 2 + 18;
    const arrowLabel = (entry.signedQError || entry.qError) >= 0 ? '↑ ' : '↓ ';
    const qErrorLabel = Number.isFinite(entry.qError)
      ? `${Math.round(entry.qError)}x`
      : String(entry.qError);
    const estimateLabel = Number.isFinite(entry.estimate)
      ? formatCardinality(entry.estimate)
      : String(entry.estimate);
    const estimateWithParens = ` (${estimateLabel})`;
    const arrowWidth = ctx.measureText(arrowLabel).width;
    const qErrorWidth = ctx.measureText(qErrorLabel).width;
    const estimateWidth = ctx.measureText(estimateWithParens).width;
    const labelStartX = centerX - (arrowWidth + qErrorWidth + estimateWidth) / 2;
    ctx.fillStyle = '#000000';
    ctx.fillText(arrowLabel, labelStartX, qLabelY);
    ctx.fillText(qErrorLabel, labelStartX + arrowWidth, qLabelY);
    ctx.fillStyle = '#808080';
    ctx.fillText(estimateWithParens, labelStartX + arrowWidth + qErrorWidth, qLabelY);

    const hasComparableBounds = Boolean(
      xboundOverlay &&
      !xboundOverlay.unsupported &&
      lpboundOverlay &&
      Number.isFinite(lpboundOverlay.estimate) &&
      (
        xboundOverlay.zeroLowerBound ||
        Number.isFinite(xboundOverlay.estimate)
      )
    );
    if (hasComparableBounds) {
      const inBounds = xboundOverlay.zeroLowerBound
        ? (entry.estimate <= lpboundOverlay.estimate)
        : (entry.estimate >= xboundOverlay.estimate && entry.estimate <= lpboundOverlay.estimate);
      const symbol = inBounds ? '👌' : '🤦';
      const symbolY = (entry.signedQError || entry.qError) >= 0 ? qLabelY - 22 : qLabelY + 22;
      ctx.font = `21px ${UI_FONT_FAMILY}`;
      const symbolWidth = ctx.measureText(symbol).width;
      ctx.fillText(symbol, centerX - symbolWidth / 2, symbolY);
      ctx.font = ESTIMATE_FONT;
    }
  });
}

function renderHtmlLegend() {
  if (!els.chartLegend) return;
  els.chartLegend.innerHTML = '';
  SYSTEMS.forEach((key) => {
    const label = SYSTEM_LABELS[key] || key;
    const icon = SYSTEM_ICON_PATHS[key] || '';
    const item = document.createElement('span');
    item.className = 'chart-legend-item';
    item.innerHTML = `<img src="${icon}" alt="${label}" /><span>${label}</span>`;
    els.chartLegend.appendChild(item);
  });
  const lowerItem = document.createElement('span');
  lowerItem.className = 'chart-legend-item';
  lowerItem.innerHTML = `<img src="${SYSTEM_ICON_PATHS.xbound || ''}" alt="xBound" /><span>xBound</span>`;
  els.chartLegend.appendChild(lowerItem);

  const upperItem = document.createElement('span');
  upperItem.className = 'chart-legend-item';
  upperItem.innerHTML = `<span aria-hidden="true">🛖</span><span>LpBound</span>`;
  els.chartLegend.appendChild(upperItem);
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
  const redraw = () => {
    if (iconRedrawRaf) return;
    iconRedrawRaf = window.requestAnimationFrame(() => {
      iconRedrawRaf = 0;
      if (currentMode === 'run') renderQErrorBarPlot(activeEntries());
    });
  };
  // Cache before assigning src to avoid recursive re-entry on cached images.
  img._xboundState = 'loading';
  systemIconCache[key] = img;
  img.onload = () => {
    img._xboundState = 'ready';
    redraw();
  };
  img.onerror = () => {
    // Keep rendering even when an icon cannot be loaded.
    img._xboundState = 'error';
    redraw();
  };
  img.src = SYSTEM_ICON_PATHS[key];
  return img;
}

function drawLegend(ctx, centerX, startY) {
  const keys = SYSTEMS;

  ctx.font = `${PLOT_FONT.legendPx}px ${UI_FONT_FAMILY}`;
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

    if (icon && icon._xboundState === 'ready' && icon.complete && icon.naturalWidth > 0) {
      ctx.drawImage(icon, x, startY + 1, iconSize, iconSize);
      x += iconSize + iconGap;
    }

    ctx.fillStyle = '#1f2a4d';
    ctx.font = `${PLOT_FONT.legendPx}px ${UI_FONT_FAMILY}`;
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

function median(nums) {
  if (!Array.isArray(nums) || nums.length === 0) return null;
  const sorted = [...nums].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

function estimateForSystem(queryData, system) {
  const estimate = Number(queryData?.estimates?.[system]);
  return Number.isFinite(estimate) && estimate > 0 ? estimate : null;
}

function lowerBoundForSystem(queryData, system) {
  const lb = Number(queryData?.xbound?.[system]);
  return Number.isFinite(lb) && lb > 0 ? lb : null;
}

function upperBoundForSystem(queryData, system) {
  const ub = Number(queryData?.lpbound?.[system]);
  return Number.isFinite(ub) && ub > 0 ? ub : null;
}

function buildBenchmarkLeaderboardMetrics(benchmark, boundMode = { xbound: true, lpbound: true }) {
  const useXbound = Boolean(boundMode?.xbound);
  const useLpbound = Boolean(boundMode?.lpbound);
  const hasAnyBounds = useXbound || useLpbound;
  const sanity = new Map(
    SYSTEMS.map((system) => [system, {
      system,
      lowerChecks: 0,
      lowerViolations: 0,
      lowerViolationRatios: [],
      upperChecks: 0,
      upperViolations: 0,
      upperViolationRatios: []
    }])
  );
  const quality = new Map(
    SYSTEMS.flatMap((system) => {
      const rows = [[system, { system, clipped: false, qErrors: [] }]];
      if (hasAnyBounds) rows.push([`${system}::xbounded`, { system, clipped: true, qErrors: [] }]);
      return rows;
    })
  );

  Object.values(queryStore[benchmark] || {}).forEach((queryData) => {
    const actual = Number(queryData?.actual);
    if (!Number.isFinite(actual) || actual <= 0) return;

    SYSTEMS.forEach((system) => {
      const estimate = estimateForSystem(queryData, system);
      if (!Number.isFinite(estimate)) return;
      const lb = lowerBoundForSystem(queryData, system);
      const ub = upperBoundForSystem(queryData, system);

      if (Number.isFinite(lb)) {
        const sanityRow = sanity.get(system);
        sanityRow.lowerChecks += 1;
        if (estimate < lb) {
          sanityRow.lowerViolations += 1;
          sanityRow.lowerViolationRatios.push(lb / estimate);
        }
      }
      if (Number.isFinite(ub)) {
        const sanityRow = sanity.get(system);
        sanityRow.upperChecks += 1;
        if (estimate > ub) {
          sanityRow.upperViolations += 1;
          sanityRow.upperViolationRatios.push(estimate / ub);
        }
      }

      const rawQ = Math.max(estimate / actual, actual / estimate);
      const rawRow = quality.get(system);
      rawRow.qErrors.push(rawQ);

      let clippedEstimate = estimate;
      if (useLpbound) {
        clippedEstimate = Number.isFinite(ub) ? Math.min(clippedEstimate, ub) : clippedEstimate;
      }
      if (useXbound) {
        clippedEstimate = Number.isFinite(lb) ? Math.max(clippedEstimate, lb) : clippedEstimate;
      }
      if (hasAnyBounds) {
        const clippedQ = Math.max(clippedEstimate / actual, actual / clippedEstimate);
        const clippedRow = quality.get(`${system}::xbounded`);
        clippedRow.qErrors.push(clippedQ);
      }
    });
  });

  const sanityRows = [...sanity.values()]
    .filter((row) => row.lowerChecks > 0 || row.upperChecks > 0)
    .map((row) => {
      const lowerViolationRate = row.lowerChecks > 0 ? row.lowerViolations / row.lowerChecks : 0;
      const upperViolationRate = row.upperChecks > 0 ? row.upperViolations / row.upperChecks : 0;
      const totalChecks = row.lowerChecks + row.upperChecks;
      const totalViolations = row.lowerViolations + row.upperViolations;
      const combinedViolationRate = totalChecks > 0 ? totalViolations / totalChecks : 0;
      const lowerMedianSeverity = row.lowerViolationRatios.length ? median(row.lowerViolationRatios) : 1;
      const upperMedianSeverity = row.upperViolationRatios.length ? median(row.upperViolationRatios) : 1;
      return {
        system: row.system,
        label: SYSTEM_LABELS[row.system] || row.system,
        icon: SYSTEM_ICON_PATHS[row.system] || null,
        lowerChecks: row.lowerChecks,
        lowerViolations: row.lowerViolations,
        lowerViolationRate,
        lowerMedianSeverity,
        upperChecks: row.upperChecks,
        upperViolations: row.upperViolations,
        upperViolationRate,
        upperMedianSeverity,
        combinedViolationRate
      };
    })
    .sort((a, b) => (
      (a.combinedViolationRate - b.combinedViolationRate) ||
      (a.lowerMedianSeverity - b.lowerMedianSeverity) ||
      (a.upperMedianSeverity - b.upperMedianSeverity)
    ));

  const qualityRows = [...quality.values()]
    .filter((row) => row.qErrors.length > 0)
    .map((row) => ({
      system: row.system,
      clipped: row.clipped,
      label: row.clipped
        ? `-ed ${SYSTEM_LABELS[row.system] || row.system}`
        : (SYSTEM_LABELS[row.system] || row.system),
      shortLabel: row.clipped ? '-ed' : 'raw',
      icon: SYSTEM_ICON_PATHS[row.system] || null,
      queries: row.qErrors.length,
      score: median(row.qErrors),
      medianQError: median(row.qErrors)
    }))
    .filter((row) => Number.isFinite(row.score))
    .sort((a, b) => a.score - b.score);

  return { sanityRows, qualityRows };
}

function boundModeBadgeMarkup(boundMode = { xbound: true, lpbound: true }) {
  const useXbound = Boolean(boundMode?.xbound);
  const useLpbound = Boolean(boundMode?.lpbound);
  const xboundBadge = `<img class="podium-icon icon-xbound" src="${SYSTEM_ICON_PATHS.xbound || ''}" alt="xBound" />`;
  const lpboundBadge = '<span class="icon-lpbound" aria-label="LpBound">🛖</span>';
  if (useXbound && !useLpbound) return xboundBadge;
  if (!useXbound && useLpbound) return lpboundBadge;
  if (!useXbound && !useLpbound) return '';
  return `<span class="leaderboard-icon-bounds">${xboundBadge}<span class="icon-plus">+</span>${lpboundBadge}</span>`;
}

function leaderboardVariantIconMarkup(system, clipped, label, boundMode = { xbound: true, lpbound: true }) {
  const systemIcon = SYSTEM_ICON_PATHS[system] || '';
  if (!clipped) {
    return `<img class="podium-icon" src="${systemIcon}" alt="${label}" />`;
  }
  return `
    <span class="leaderboard-icon-ed" aria-label="${label}">
      <img class="podium-icon icon-system" src="${systemIcon}" alt="${label}" />
      <span class="icon-plus">+</span>
      ${boundModeBadgeMarkup(boundMode)}
    </span>
  `;
}

function renderLeaderboard() {
  const benchmark = els.benchmarkSelect.value;
  const { sanityRows, qualityRows } = buildBenchmarkLeaderboardMetrics(benchmark, qerrorBoundMode);

  els.leaderboardList.innerHTML = '';
  if (sanityRows.length === 0 && qualityRows.length === 0) {
    els.leaderboardList.textContent = 'No benchmark-wide estimates available yet.';
    return;
  }

  const tabs = document.createElement('div');
  tabs.className = 'leaderboard-tabs';
  const sanityTabBtn = document.createElement('button');
  sanityTabBtn.className = `leaderboard-tab-btn${leaderboardTab === 'sanity' ? ' active' : ''}`;
  sanityTabBtn.textContent = 'Soundness';
  sanityTabBtn.addEventListener('click', () => {
    leaderboardTab = 'sanity';
    renderLeaderboard();
  });
  const qerrorTabBtn = document.createElement('button');
  qerrorTabBtn.className = `leaderboard-tab-btn${leaderboardTab === 'qerror' ? ' active' : ''}`;
  qerrorTabBtn.textContent = 'Q-error';
  qerrorTabBtn.addEventListener('click', () => {
    leaderboardTab = 'qerror';
    renderLeaderboard();
  });
  tabs.appendChild(sanityTabBtn);
  tabs.appendChild(qerrorTabBtn);
  els.leaderboardList.appendChild(tabs);

  if (leaderboardTab === 'sanity' && sanityRows.length > 0) {
    const sanityPodium = document.createElement('div');
    sanityPodium.className = 'leaderboard-podium';
    const sanityPodiumOrder = [1, 0, 2];
    sanityPodiumOrder.forEach((idx) => {
      const row = sanityRows[idx];
      if (!row) return;
      const card = document.createElement('div');
      card.className = `podium-card rank-${idx + 1}`;
      card.innerHTML = `
        <div class="podium-top">
          <img class="podium-icon" src="${row.icon || ''}" alt="${row.label}" />
          <div class="podium-score">${(row.combinedViolationRate * 100).toFixed(1)}% violations</div>
          <div class="podium-meta">LB ${row.lowerMedianSeverity.toFixed(2)}x | UB ${row.upperMedianSeverity.toFixed(2)}x</div>
        </div>
        <div class="podium-step">#${idx + 1}</div>
      `;
      sanityPodium.appendChild(card);
    });
    const sanityTable = document.createElement('table');
    sanityTable.className = 'leaderboard-soundness-table';
    sanityTable.innerHTML = `
      <thead>
        <tr>
          <th rowspan="2">#</th>
          <th rowspan="2">System</th>
          <th colspan="2">%violations</th>
          <th colspan="2">severity</th>
          <th rowspan="2">Total Violations</th>
        </tr>
        <tr>
          <th>Lower bound</th>
          <th>Upper bound</th>
          <th>Lower bound</th>
          <th>Upper bound</th>
        </tr>
      </thead>
    `;
    const tbody = document.createElement('tbody');
    sanityRows.forEach((row, idx) => {
      const totalChecks = row.lowerChecks + row.upperChecks;
      const totalViolations = row.lowerViolations + row.upperViolations;
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="rank">${idx + 1}</td>
        <td class="system">
          <img class="leaderboard-system-icon" src="${row.icon || ''}" alt="${row.label}" />
          <span>${row.label}</span>
        </td>
        <td>${(row.lowerViolationRate * 100).toFixed(1)}%</td>
        <td>${(row.upperViolationRate * 100).toFixed(1)}%</td>
        <td>${row.lowerMedianSeverity.toFixed(2)}x</td>
        <td>${row.upperMedianSeverity.toFixed(2)}x</td>
        <td>${totalChecks > 0 ? `${(row.combinedViolationRate * 100).toFixed(1)}% (${totalViolations}/${totalChecks})` : 'n/a'}</td>
      `;
      tbody.appendChild(tr);
    });
    sanityTable.appendChild(tbody);

    const sanityLayout = document.createElement('div');
    sanityLayout.className = 'leaderboard-split';
    const sanityPodiumPanel = document.createElement('div');
    sanityPodiumPanel.className = 'leaderboard-left-panel';
    sanityPodiumPanel.appendChild(sanityPodium);
    sanityLayout.appendChild(sanityPodiumPanel);
    const sanityPanel = document.createElement('div');
    sanityPanel.className = 'leaderboard-right-panel';
    sanityPanel.appendChild(sanityTable);
    sanityLayout.appendChild(sanityPanel);
    els.leaderboardList.appendChild(sanityLayout);
  }

  if (leaderboardTab === 'qerror' && qualityRows.length > 0) {
    const modeControls = document.createElement('div');
    modeControls.className = 'leaderboard-mode-controls';
    modeControls.innerHTML = `
      <span class="leaderboard-mode-label">Bounded system:</span>
      <label class="leaderboard-mode-option">
        <input type="checkbox" name="qerrorBoundModeXbound" ${qerrorBoundMode.xbound ? 'checked' : ''} />
        <span>+ <img class="leaderboard-mode-icon" src="${SYSTEM_ICON_PATHS.xbound || ''}" alt="xBound" /> xBound</span>
      </label>
      <label class="leaderboard-mode-option">
        <input type="checkbox" name="qerrorBoundModeLpbound" ${qerrorBoundMode.lpbound ? 'checked' : ''} />
        <span>+ 🛖 LpBound</span>
      </label>
    `;
    modeControls.querySelectorAll('input[type="checkbox"]').forEach((input) => {
      input.addEventListener('change', () => {
        const xboundInput = modeControls.querySelector('input[name="qerrorBoundModeXbound"]');
        const lpboundInput = modeControls.querySelector('input[name="qerrorBoundModeLpbound"]');
        qerrorBoundMode = {
          xbound: Boolean(xboundInput?.checked),
          lpbound: Boolean(lpboundInput?.checked)
        };
        renderLeaderboard();
      });
    });
    els.leaderboardList.appendChild(modeControls);

    const podium = document.createElement('div');
    podium.className = 'leaderboard-podium';
    const podiumOrder = [1, 0, 2];
    podiumOrder.forEach((idx) => {
      const row = qualityRows[idx];
      if (!row) return;
      const card = document.createElement('div');
      card.className = `podium-card rank-${idx + 1}`;
      card.innerHTML = `
        <div class="podium-top">
          ${leaderboardVariantIconMarkup(row.system, row.clipped, row.label, qerrorBoundMode)}
          <div class="podium-score">median Q-error: <span class="metric-value">${row.score.toFixed(1)}x</span></div>
          <div class="podium-meta">${row.queries} queries</div>
        </div>
        <div class="podium-step">#${idx + 1}</div>
      `;
      podium.appendChild(card);
    });
    const bySystem = new Map(SYSTEMS.map((system) => [system, { raw: null, bounded: null }]));
    qualityRows.forEach((row) => {
      const holder = bySystem.get(row.system);
      if (!holder) return;
      if (row.clipped) holder.bounded = row;
      else holder.raw = row;
    });

    const simpleRows = SYSTEMS
      .map((system) => {
        const pack = bySystem.get(system);
        if (!pack || (!pack.raw && !pack.bounded)) return null;
        const raw = pack.raw?.score ?? null;
        const bounded = pack.bounded?.score ?? null;
        return {
          system,
          label: SYSTEM_LABELS[system] || system,
          icon: SYSTEM_ICON_PATHS[system] || '',
          raw,
          bounded
        };
      })
      .filter(Boolean)
      .sort((a, b) => {
        const aScore = Number.isFinite(a.bounded) ? a.bounded : a.raw;
        const bScore = Number.isFinite(b.bounded) ? b.bounded : b.raw;
        if (!Number.isFinite(aScore) && !Number.isFinite(bScore)) return 0;
        if (!Number.isFinite(aScore)) return 1;
        if (!Number.isFinite(bScore)) return -1;
        return aScore - bScore;
      });

    const qualityTable = document.createElement('table');
    qualityTable.className = 'leaderboard-qerror-table';
    qualityTable.innerHTML = `
      <thead>
        <tr>
          <th>System</th>
          <th>Raw</th>
          <th>Bounded</th>
        </tr>
      </thead>
      <tbody>
        ${simpleRows.map((row) => `
          <tr>
            <td class="system">
              <img class="leaderboard-system-icon" src="${row.icon}" alt="${row.label}" />
              ${row.label}
            </td>
            <td>${Number.isFinite(row.raw) ? `<span class="metric-value">${row.raw.toFixed(1)}x</span>` : 'n/a'}</td>
            <td>${Number.isFinite(row.bounded) ? `<span class="metric-value metric-bounded">${row.bounded.toFixed(1)}x</span>` : 'n/a'}</td>
          </tr>
        `).join('')}
      </tbody>
    `;
    const qualityLayout = document.createElement('div');
    qualityLayout.className = 'leaderboard-split';
    const qualityPodiumPanel = document.createElement('div');
    qualityPodiumPanel.className = 'leaderboard-left-panel';
    qualityPodiumPanel.appendChild(podium);
    qualityLayout.appendChild(qualityPodiumPanel);
    const qualityPanel = document.createElement('div');
    qualityPanel.className = 'leaderboard-right-panel';
    qualityPanel.appendChild(qualityTable);
    qualityLayout.appendChild(qualityPanel);
    els.leaderboardList.appendChild(qualityLayout);
  }

  if (leaderboardTab === 'sanity' && sanityRows.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'leaderboard-summary';
    empty.textContent = 'No soundness data available for this benchmark.';
    els.leaderboardList.appendChild(empty);
  }
  if (leaderboardTab === 'qerror' && qualityRows.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'leaderboard-summary';
    empty.textContent = 'No q-error data available for this benchmark.';
    els.leaderboardList.appendChild(empty);
  }
}

function activeEntries() {
  return buildEstimateEntries(true);
}

function getSqlText() {
  if (sqlEditor) return sqlEditor.getValue();
  return els.sqlInput.value;
}

function alignRunButtonToSqlText() {
  if (!els.runBtn) return;
  els.runBtn.style.transform = 'translateX(0px)';
}

function syncRailButtonSizing() {
  if (!els.navRail) return;
  const xboundRect = els.xboundPanel?.getBoundingClientRect?.();
  if (xboundRect && Number.isFinite(xboundRect.height) && xboundRect.height > 0) {
    syncedPanelHeight = xboundRect.height;
  }
  if (
    (!Number.isFinite(syncedPanelHeight) || syncedPanelHeight <= 0) &&
    currentMode !== 'leaderboard'
  ) {
    const sqlRect =
      els.sqlPanel?.getBoundingClientRect?.() ||
      els.sqlEditorWrap?.getBoundingClientRect?.();
    if (sqlRect && Number.isFinite(sqlRect.height) && sqlRect.height > 0) {
      syncedPanelHeight = sqlRect.height;
    }
  }
  if (
    (!Number.isFinite(syncedPanelHeight) || syncedPanelHeight <= 0) &&
    els.controlsPanel
  ) {
    const controlsRect = els.controlsPanel.getBoundingClientRect();
    if (Number.isFinite(controlsRect.height) && controlsRect.height > 0) {
      syncedPanelHeight = controlsRect.height;
    }
  }
  if (!Number.isFinite(syncedPanelHeight) || syncedPanelHeight <= 0) return;

  const railStyle = window.getComputedStyle(els.navRail);
  if (railStyle.flexDirection === 'row') {
    els.navRail.style.height = '';
    els.navRail.style.minHeight = '';
    els.navRail.style.maxHeight = '';
    if (els.controlsPanel) {
      els.controlsPanel.style.height = '';
      els.controlsPanel.style.minHeight = '';
      els.controlsPanel.style.maxHeight = '';
    }
    if (els.sqlPanel) {
      els.sqlPanel.style.height = '';
      els.sqlPanel.style.minHeight = '';
      els.sqlPanel.style.maxHeight = '';
    }
    [els.motivationBtn, els.dashboardTabBtn, els.leaderboardBtn].filter(Boolean).forEach((btn) => {
      btn.style.height = '';
      btn.style.minHeight = '';
      btn.style.maxHeight = '';
    });
    return;
  }

  const syncedHeight = Math.min(syncedPanelHeight, MAX_SYNCED_PANEL_HEIGHT);
  const paddingTop = parseFloat(railStyle.paddingTop) || 0;
  const paddingBottom = parseFloat(railStyle.paddingBottom) || 0;
  const gap = parseFloat(railStyle.rowGap || railStyle.gap) || 0;
  const buttons = [els.motivationBtn, els.dashboardTabBtn, els.leaderboardBtn].filter(Boolean);
  if (buttons.length === 0) return;
  const totalGap = gap * Math.max(0, buttons.length - 1);
  const innerHeight = Math.max(0, syncedHeight - paddingTop - paddingBottom - totalGap);
  const buttonHeightPx = `${innerHeight / buttons.length}px`;
  const railHeightPx = `${syncedHeight}px`;

  els.navRail.style.height = railHeightPx;
  els.navRail.style.minHeight = railHeightPx;
  els.navRail.style.maxHeight = railHeightPx;
  if (els.controlsPanel) {
    els.controlsPanel.style.height = railHeightPx;
    els.controlsPanel.style.minHeight = railHeightPx;
    els.controlsPanel.style.maxHeight = railHeightPx;
  }
  if (els.sqlPanel && currentMode !== 'leaderboard' && currentMode !== 'motivation') {
    els.sqlPanel.style.height = railHeightPx;
    els.sqlPanel.style.minHeight = railHeightPx;
    els.sqlPanel.style.maxHeight = railHeightPx;
  }
  buttons.forEach((btn) => {
    btn.style.height = buttonHeightPx;
    btn.style.minHeight = buttonHeightPx;
    btn.style.maxHeight = buttonHeightPx;
  });
}

function renderMotivationSlide() {
  const slides = els.motivationSlides || [];
  if (slides.length === 0) return;
  const clampedIdx = Math.max(0, Math.min(slides.length - 1, motivationSlideIndex));
  motivationSlideIndex = clampedIdx;
  slides.forEach((slide, idx) => {
    slide.classList.toggle('is-active', idx === clampedIdx);
  });
  if (els.motivationPrevBtn) {
    els.motivationPrevBtn.classList.toggle('hidden', clampedIdx === 0);
  }
  if (els.motivationNextBtn) {
    els.motivationNextBtn.classList.toggle('hidden', clampedIdx === slides.length - 1);
  }
}

function setMode(mode) {
  currentMode = mode;
  if (els.motivationBtn) els.motivationBtn.classList.toggle('active', mode === 'motivation');
  if (els.dashboardTabBtn) els.dashboardTabBtn.classList.toggle('active', mode === 'run' || mode === 'plan');
  if (els.planViewBtn) els.planViewBtn.classList.toggle('active', mode === 'plan');
  els.leaderboardBtn.classList.toggle('active', mode === 'leaderboard');

  if (els.motivationPanel) els.motivationPanel.classList.toggle('hidden', mode !== 'motivation');
  els.runPanel.classList.toggle('hidden', mode !== 'run');
  els.planPanel.classList.toggle('hidden', mode !== 'plan');
  els.leaderboardPanel.classList.toggle('hidden', mode !== 'leaderboard');
  if (els.sqlPanel) els.sqlPanel.classList.toggle('hidden', mode === 'leaderboard' || mode === 'motivation');
  if (els.controlsPanel) els.controlsPanel.classList.toggle('hidden', mode === 'motivation');
  if (els.xboundPanel) els.xboundPanel.classList.toggle('hidden', mode === 'motivation');
  if (els.appShell) els.appShell.classList.toggle('leaderboard-mode', mode === 'leaderboard');

  els.planControls.classList.toggle('hidden', mode !== 'plan');
  if (els.queryControls) els.queryControls.classList.toggle('hidden', mode === 'leaderboard' || mode === 'motivation');

  const entries = activeEntries();
  if (mode === 'motivation') renderMotivationSlide();
  if (mode === 'run') renderQErrorBarPlot(entries);
  if (mode === 'plan') renderPlanTree(els.planSystemSelect.value);
  if (mode === 'leaderboard') renderLeaderboard();
  window.requestAnimationFrame(() => {
    alignRunButtonToSqlText();
    syncRailButtonSizing();
  });
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

function updateQuerySelector(forceFirst = false) {
  const benchmark = els.benchmarkSelect.value;
  const queries = Object.keys(queryStore[benchmark] || {});
  const previousSelection = els.querySelect.value;

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
  } else if (forceFirst) {
    const preferred = queries.find((q) => String(q).toUpperCase() === 'Q1');
    els.querySelect.value = preferred || queries[0];
  } else if (previousSelection && queries.includes(previousSelection)) {
    els.querySelect.value = previousSelection;
  }

  syncSqlInput();
}

function syncSqlInput() {
  const queryData = getCurrentQueryData();
  const sqlText = formatSql(queryData?.sql || '-- select a query');
  if (sqlEditor) sqlEditor.setValue(sqlText);
  else els.sqlInput.value = sqlText;

  const benchmarkWarning = benchmarkLoadWarnings.get(els.benchmarkSelect.value);
  const xboundWarning = currentXboundAvailabilityWarning();
  if (els.xboundWarning) {
    const queryData = getCurrentQueryData();
    const showXboundBanner = Boolean(queryData) || Boolean(xboundWarning);
    if (showXboundBanner && xboundWarning) {
      els.xboundWarning.textContent = `⚠️\u00A0\u00A0${xboundWarning}`;
      els.xboundWarning.classList.remove('is-loaded');
      els.xboundWarning.classList.add('is-warning');
      els.xboundWarning.classList.remove('hidden');
    } else if (showXboundBanner) {
      els.xboundWarning.textContent = '✅\u00A0\u00A0xBound estimates loaded for current benchmark/query.';
      els.xboundWarning.classList.remove('is-warning');
      els.xboundWarning.classList.add('is-loaded');
      els.xboundWarning.classList.remove('hidden');
    } else {
      els.xboundWarning.textContent = '';
      els.xboundWarning.classList.remove('is-warning', 'is-loaded');
      els.xboundWarning.classList.add('hidden');
    }
  }
  if (benchmarkWarning) {
    els.statusText.textContent = `Warning: ${benchmarkWarning}`;
    return;
  }
  if (xboundWarning) {
    els.statusText.textContent = `Warning: ${xboundWarning}`;
    return;
  }
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
    xbound: raw.xbound && typeof raw.xbound === 'object' ? raw.xbound : {},
    lpbound: raw.lpbound && typeof raw.lpbound === 'object' ? raw.lpbound : {}
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
      parts: discreteSliderValue(els.xboundParts, 16),
      l0Theta: discreteSliderValue(els.xboundL0Theta, 8),
      hhTheta: discreteSliderValue(els.xboundHhTheta, 12)
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

function cloneQueryMap(queries) {
  return JSON.parse(JSON.stringify(queries || {}));
}

function discreteSliderValue(el, fallback) {
  if (!el) return fallback;
  const rawValues = String(el.dataset.values || '')
    .split(',')
    .map((v) => Number(v.trim()))
    .filter((v) => Number.isFinite(v));
  if (rawValues.length === 0) {
    const direct = Number(el.value);
    return Number.isFinite(direct) ? direct : fallback;
  }
  const idx = Number(el.value);
  if (!Number.isFinite(idx)) return fallback;
  const clamped = Math.max(0, Math.min(rawValues.length - 1, Math.trunc(idx)));
  return rawValues[clamped];
}

function syncXboundSliderLabels() {
  if (els.xboundPartsValue) {
    els.xboundPartsValue.textContent = String(discreteSliderValue(els.xboundParts, 16));
  }
  if (els.xboundL0ThetaValue) {
    els.xboundL0ThetaValue.textContent = String(discreteSliderValue(els.xboundL0Theta, 8));
  }
  if (els.xboundHhThetaValue) {
    els.xboundHhThetaValue.textContent = String(discreteSliderValue(els.xboundHhTheta, 12));
  }
}

async function ensureBenchmarkLoaded(benchmark) {
  const cacheKey = xboundParamCacheKey(benchmark);
  if (benchmarkDataCache.has(cacheKey)) {
    queryStore[benchmark] = cloneQueryMap(benchmarkDataCache.get(cacheKey));
    benchmarkLoadWarnings.delete(benchmark);
    return;
  }
  if (loadedBenchmarks.has(cacheKey)) return;
  loadedBenchmarks.add(cacheKey);

  const estimateLoader = window.xbound?.loadPrecomputedEstimates;
  const workloadLoader = window.xbound?.loadWorkloadQueries;
  const workloadSource = typeof workloadLoader === 'function' ? workloadLoader : loadWorkloadQueriesWeb;
  const estimateSource = typeof estimateLoader === 'function' ? estimateLoader : loadPrecomputedEstimatesWeb;
  const nextQueries = {};

  try {
    const workloadResult = await workloadSource(benchmark);
    if (workloadResult?.ok && workloadResult.queries && typeof workloadResult.queries === 'object') {
      Object.entries(workloadResult.queries).forEach(([queryName, queryData]) => {
        const canonicalName = canonicalQueryName(benchmark, queryName);
        nextQueries[canonicalName] ||= { sql: '', actual: 0, estimates: {}, xbound: {}, lpbound: {} };
        if (typeof queryData?.sql === 'string' && queryData.sql) {
          nextQueries[canonicalName].sql = queryData.sql;
        }
      });
    }

    const estimateResult = await estimateSource(benchmark, xboundParamOptionsForBenchmark(benchmark));
    if (estimateResult?.ok && estimateResult.queries && typeof estimateResult.queries === 'object') {
      Object.entries(estimateResult.queries).forEach(([queryName, queryData]) => {
        const canonicalName = canonicalQueryName(benchmark, queryName);
        const loaded = normalizeLoadedQuery(queryData);
        if (!loaded) return;

        nextQueries[canonicalName] ||= { sql: '', actual: 0, estimates: {}, xbound: {}, lpbound: {} };
        const current = nextQueries[canonicalName];
        nextQueries[canonicalName] = {
          ...current,
          sql: loaded.sql || current.sql,
          actual: loaded.actual || current.actual,
          estimates: { ...(current.estimates || {}), ...(loaded.estimates || {}) },
          xbound: { ...(current.xbound || {}), ...(loaded.xbound || {}) },
          lpbound: { ...(current.lpbound || {}), ...(loaded.lpbound || {}) }
        };
      });
    }
    benchmarkDataCache.set(cacheKey, cloneQueryMap(nextQueries));
    if (xboundParamCacheKey(benchmark) !== cacheKey) return;
    queryStore[benchmark] = cloneQueryMap(nextQueries);
    benchmarkLoadWarnings.delete(benchmark);
    if (benchmark === els.benchmarkSelect.value) {
      els.statusText.textContent = `Loaded precomputed estimates from ${estimateResult.sourcePath}`;
    }
  } catch (err) {
    loadedBenchmarks.delete(cacheKey);
    if (xboundParamCacheKey(benchmark) !== cacheKey) return;
    queryStore[benchmark] = {};
    if (err?.code === 'MISSING_XBOUND_FILE') {
      benchmarkLoadWarnings.set(benchmark, `missing xBound file for ${err.benchmark} (${err.fileName})`);
    } else {
      benchmarkLoadWarnings.set(benchmark, `failed to load benchmark data for ${benchmark}`);
    }
    if (benchmark === els.benchmarkSelect.value) {
      els.statusText.textContent = `Warning: ${benchmarkLoadWarnings.get(benchmark)}.`;
    }
  }
}

async function refreshForXboundSliderChange() {
  const benchmark = els.benchmarkSelect.value;
  if (!benchmarkSupportsXboundParamVariants(benchmark)) {
    syncSqlInput();
    const entries = activeEntries();
    if (currentMode === 'run') renderQErrorBarPlot(entries);
    if (currentMode === 'leaderboard') renderLeaderboard();
    return;
  }
  const refreshSeq = ++xboundSliderRefreshSeq;
  customQueryData = null;
  els.statusText.textContent = 'Updating xBound estimates for current parameters...';
  await ensureBenchmarkLoaded(benchmark);
  if (refreshSeq !== xboundSliderRefreshSeq) return;
  updateQuerySelector();
  const entries = activeEntries();
  if (currentMode === 'run') renderQErrorBarPlot(entries);
  if (currentMode === 'leaderboard') renderLeaderboard();
}

function scheduleXboundSliderRefresh(delayMs) {
  if (xboundSliderRefreshTimer) window.clearTimeout(xboundSliderRefreshTimer);
  xboundSliderRefreshTimer = window.setTimeout(() => {
    xboundSliderRefreshTimer = 0;
    refreshForXboundSliderChange();
  }, delayMs);
}

function bindEvents() {
  els.benchmarkSelect.addEventListener('change', async () => {
    customQueryData = null;
    updateXboundParamsState();
    await ensureBenchmarkLoaded(els.benchmarkSelect.value);
    updateQuerySelector(true);
    const entries = activeEntries();
    if (currentMode === 'run') renderQErrorBarPlot(entries);
    if (currentMode === 'leaderboard') renderLeaderboard();
  });

  els.querySelect.addEventListener('change', () => {
    customQueryData = null;
    syncSqlInput();
    const entries = activeEntries();
    if (currentMode === 'run') renderQErrorBarPlot(entries);
    if (currentMode === 'leaderboard') renderLeaderboard();
  });

  [els.xboundParts, els.xboundL0Theta, els.xboundHhTheta].forEach((el) => {
    if (!el) return;
    el.addEventListener('input', () => {
      syncXboundSliderLabels();
      scheduleXboundSliderRefresh(120);
    });
    el.addEventListener('change', async () => {
      if (xboundSliderRefreshTimer) {
        window.clearTimeout(xboundSliderRefreshTimer);
        xboundSliderRefreshTimer = 0;
      }
      await refreshForXboundSliderChange();
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
    if (isCustom && !supportsCustomQuery) {
      els.statusText.textContent = 'Custom query estimation is only available in the Electron app.';
      return;
    }

    customQueryData = null;
    renderQErrorBarPlot(activeEntries());
    els.statusText.textContent = 'Q-error bars refreshed';
  });

  if (els.planViewBtn) {
    els.planViewBtn.addEventListener('click', () => {
      setMode('plan');
      renderPlanTree(els.planSystemSelect.value);
      els.statusText.textContent = 'Plan view generated from JSON';
    });
  }

  if (els.dashboardTabBtn) {
    els.dashboardTabBtn.addEventListener('click', () => {
      setMode('run');
      renderQErrorBarPlot(activeEntries());
      els.statusText.textContent = 'Dashboard opened';
    });
  }

  if (els.motivationBtn) {
    els.motivationBtn.addEventListener('click', () => {
      setMode('motivation');
      els.statusText.textContent = 'Motivation opened';
    });
  }

  if (els.motivationNextBtn) {
    els.motivationNextBtn.addEventListener('click', () => {
      motivationSlideIndex += 1;
      renderMotivationSlide();
    });
  }
  if (els.motivationPrevBtn) {
    els.motivationPrevBtn.addEventListener('click', () => {
      motivationSlideIndex -= 1;
      renderMotivationSlide();
    });
  }

  els.leaderboardBtn.addEventListener('click', () => {
    setMode('leaderboard');
    renderLeaderboard();
    els.statusText.textContent = 'Leaderboard updated';
  });

  window.addEventListener('resize', () => {
    if (currentMode === 'run') renderQErrorBarPlot(activeEntries());
    alignRunButtonToSqlText();
    syncRailButtonSizing();
  });
}

async function init() {
  try {
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
  } catch (err) {
    console.error('[init][codemirror-failed]', err);
    sqlEditor = null;
  }

  try {
    await ensureBenchmarkLoaded('JOBlight');
    await ensureBenchmarkLoaded('SO-CEB');
    await ensureBenchmarkLoaded('STATS-CEB');
    populateSelectors();
    syncXboundSliderLabels();
    updateXboundParamsState();
    bindEvents();
    renderHtmlLegend();
    // Capture the baseline rail height while dashboard panels are still visible.
    syncRailButtonSizing();
    setMode('motivation');
    window.requestAnimationFrame(() => {
      alignRunButtonToSqlText();
      syncRailButtonSizing();
    });
  } catch (err) {
    console.error('[init][failed]', err);
    els.statusText.textContent = 'Render failed in this browser. Check console logs.';
    renderQErrorBarPlot([]);
  }
}

init();
