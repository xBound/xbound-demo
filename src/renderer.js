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
  planSystemSelect: document.getElementById('planSystemSelect'),
  planControls: document.getElementById('planControls'),
  dashboardTabBtn: document.getElementById('dashboardTabBtn'),
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
const supportsCustomQuery = typeof window.xbound?.estimateCustomQuery === 'function';
let leaderboardTab = 'sanity';
const benchmarkLoadWarnings = new Map();
let xboundSliderRefreshTimer = 0;
let xboundSliderRefreshSeq = 0;

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
      'xbound::joblight-queries_host=hausberg_parts=16_ns=1_ub=0_l0-theta=8_hh-theta=12_mcv=1024.jsonl'
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
    queries[key] ||= { estimates: {}, xbound: {} };
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
  if (!ctx) return;
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

  const xStep = width / Math.max(1, SYSTEMS.length);
  const stemWidth = 2;
  const iconHeadSize = Math.max(28, Math.min(44, xStep * 0.42));
  const entryBySystem = new Map(entries.map((entry) => [systemKeyForEntry(entry.system), entry]));
  const benchmarkWarning = benchmarkLoadWarnings.get(els.benchmarkSelect.value) || '';

  ctx.clearRect(0, 0, cssWidth, cssHeight);
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, cssWidth, cssHeight);

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
  drawLegend(ctx, cssWidth / 2, 18);
  if (benchmarkWarning) {
    ctx.fillStyle = '#a33a3a';
    ctx.font = `${PLOT_FONT.warningPx}px ${UI_FONT_FAMILY}`;
    ctx.fillText(`⚠️ ${benchmarkWarning}`, margin.left + 8, margin.top - 12);
  }

  if (xboundOverlay) {
    if (xboundOverlay.unsupported) {
      ctx.fillStyle = '#a33a3a';
      ctx.font = `${PLOT_FONT.warningPx}px ${UI_FONT_FAMILY}`;
      const warningText = '⚠️ Query not supported in xBound';
      const textWidth = ctx.measureText(warningText).width;
      ctx.fillText(warningText, cssWidth - margin.right - textWidth - 4, margin.top - 12);
    } else if (xboundOverlay.zeroLowerBound) {
      ctx.fillStyle = '#a36a00';
      ctx.font = `${PLOT_FONT.warningPx}px ${UI_FONT_FAMILY}`;
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
    if (icon && icon._xboundState === 'ready' && icon.complete && icon.naturalWidth > 0) {
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

function buildBenchmarkLeaderboardMetrics(benchmark) {
  const sanity = new Map(
    SYSTEMS.map((system) => [system, { system, checks: 0, violations: 0, violationRatios: [] }])
  );
  const quality = new Map(
    SYSTEMS.flatMap((system) => ([
      [system, { system, clipped: false, qErrors: [] }],
      [`${system}::xbounded`, { system, clipped: true, qErrors: [] }]
    ]))
  );

  Object.values(queryStore[benchmark] || {}).forEach((queryData) => {
    const actual = Number(queryData?.actual);
    if (!Number.isFinite(actual) || actual <= 0) return;

    SYSTEMS.forEach((system) => {
      const estimate = estimateForSystem(queryData, system);
      if (!Number.isFinite(estimate)) return;
      const lb = lowerBoundForSystem(queryData, system);

      if (Number.isFinite(lb)) {
        const sanityRow = sanity.get(system);
        sanityRow.checks += 1;
        if (estimate < lb) {
          sanityRow.violations += 1;
          sanityRow.violationRatios.push(lb / estimate);
        }
      }

      const rawQ = Math.max(estimate / actual, actual / estimate);
      const rawRow = quality.get(system);
      rawRow.qErrors.push(rawQ);

      const clippedEstimate = Number.isFinite(lb) ? Math.max(estimate, lb) : estimate;
      const clippedQ = Math.max(clippedEstimate / actual, actual / clippedEstimate);
      const clippedRow = quality.get(`${system}::xbounded`);
      clippedRow.qErrors.push(clippedQ);
    });
  });

  const sanityRows = [...sanity.values()]
    .filter((row) => row.checks > 0)
    .map((row) => {
      const violationRate = row.checks > 0 ? row.violations / row.checks : 0;
      const medianSeverity = row.violationRatios.length ? median(row.violationRatios) : 1;
      return {
        system: row.system,
        label: SYSTEM_LABELS[row.system] || row.system,
        icon: SYSTEM_ICON_PATHS[row.system] || null,
        checks: row.checks,
        violations: row.violations,
        violationRate,
        medianSeverity
      };
    })
    .sort((a, b) => (a.violationRate - b.violationRate) || (a.medianSeverity - b.medianSeverity));

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

function leaderboardVariantIconMarkup(system, clipped, label) {
  const systemIcon = SYSTEM_ICON_PATHS[system] || '';
  if (!clipped) {
    return `<img class="podium-icon" src="${systemIcon}" alt="${label}" />`;
  }
  const xboundIcon = SYSTEM_ICON_PATHS.xbound || '';
  return `
    <span class="leaderboard-icon-ed" aria-label="${label}">
      <img class="podium-icon icon-system" src="${systemIcon}" alt="${label}" />
      <span class="icon-plus">+</span>
      <img class="podium-icon icon-xbound" src="${xboundIcon}" alt="xBound" />
    </span>
  `;
}

function renderLeaderboard() {
  const benchmark = els.benchmarkSelect.value;
  const { sanityRows, qualityRows } = buildBenchmarkLeaderboardMetrics(benchmark);

  els.leaderboardList.innerHTML = '';
  if (sanityRows.length === 0 && qualityRows.length === 0) {
    els.leaderboardList.textContent = 'No benchmark-wide estimates available yet.';
    return;
  }

  const tabs = document.createElement('div');
  tabs.className = 'leaderboard-tabs';
  const sanityTabBtn = document.createElement('button');
  sanityTabBtn.className = `leaderboard-tab-btn${leaderboardTab === 'sanity' ? ' active' : ''}`;
  sanityTabBtn.textContent = 'Sanity';
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
          <div class="podium-score">${(row.violationRate * 100).toFixed(1)}% violations</div>
          <div class="podium-meta">severity ${row.medianSeverity.toFixed(2)}x</div>
        </div>
        <div class="podium-step">#${idx + 1}</div>
      `;
      sanityPodium.appendChild(card);
    });
    const sanityCards = document.createElement('div');
    sanityCards.className = 'quality-cards sanity-cards';
    sanityRows.forEach((row, idx) => {
      const card = document.createElement('div');
      card.className = 'quality-card';
      card.innerHTML = `
        <div class="quality-card-header">
          <span class="leaderboard-col rank">${idx + 1}</span>
          <img class="leaderboard-system-icon" src="${row.icon || ''}" alt="${row.label}" />
          <span>${row.label}</span>
        </div>
        <div class="quality-variants">
          <div class="quality-variant">
            <span class="variant-chip">violations</span>
            <span>${(row.violationRate * 100).toFixed(1)}% (${row.violations}/${row.checks})</span>
          </div>
          <div class="quality-variant">
            <span class="variant-chip">severity</span>
            <span>median ${row.medianSeverity.toFixed(2)}x</span>
          </div>
        </div>
      `;
      sanityCards.appendChild(card);
    });

    const sanityLayout = document.createElement('div');
    sanityLayout.className = 'leaderboard-split';
    const sanityPodiumPanel = document.createElement('div');
    sanityPodiumPanel.className = 'leaderboard-left-panel';
    sanityPodiumPanel.appendChild(sanityPodium);
    sanityLayout.appendChild(sanityPodiumPanel);
    const sanityPanel = document.createElement('div');
    sanityPanel.className = 'leaderboard-right-panel';
    sanityPanel.appendChild(sanityCards);
    sanityLayout.appendChild(sanityPanel);
    els.leaderboardList.appendChild(sanityLayout);
  }

  if (leaderboardTab === 'qerror' && qualityRows.length > 0) {
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
          ${leaderboardVariantIconMarkup(row.system, row.clipped, row.label)}
          <div class="podium-score">median Q-error: <span class="metric-value">${row.score.toFixed(1)}x</span></div>
          <div class="podium-meta">${row.queries} queries</div>
        </div>
        <div class="podium-step">#${idx + 1}</div>
      `;
      podium.appendChild(card);
    });
    const bySystem = new Map(SYSTEMS.map((system) => [system, { raw: null, clipped: null }]));
    qualityRows.forEach((row) => {
      const holder = bySystem.get(row.system);
      if (!holder) return;
      if (row.clipped) holder.clipped = row;
      else holder.raw = row;
    });

    const qualityCards = document.createElement('div');
    qualityCards.className = 'quality-cards';
    SYSTEMS.forEach((system) => {
      const pack = bySystem.get(system);
      if (!pack || (!pack.raw && !pack.clipped)) return;
      const label = SYSTEM_LABELS[system] || system;
      const icon = SYSTEM_ICON_PATHS[system] || '';
      const rawText = pack.raw
        ? `median Q-error: <span class="metric-value">${pack.raw.medianQError.toFixed(1)}x</span>`
        : 'no data';
      const clippedText = pack.clipped
        ? `median Q-error: <span class="metric-value">${pack.clipped.medianQError.toFixed(1)}x</span>`
        : 'no data';
      const improvement = (pack.raw && pack.clipped)
        ? `${(((pack.raw.medianQError - pack.clipped.medianQError) / pack.raw.medianQError) * 100).toFixed(1)}% improvement`
        : 'insufficient data';

      const card = document.createElement('div');
      card.className = 'quality-card';
      card.innerHTML = `
        <div class="quality-card-header">
          <img class="leaderboard-system-icon" src="${icon}" alt="${label}" />
          <span>${label}</span>
        </div>
        <div class="quality-variants">
          <div class="quality-variant">
            <span class="variant-chip">raw</span>
            <span>${rawText}</span>
          </div>
          <div class="quality-variant">
            <span class="variant-chip variant-chip-combo">
              <img class="variant-chip-icon" src="${SYSTEM_ICON_PATHS.xbound || ''}" alt="xBound" />
              <span>-ed</span>
              <img class="variant-chip-icon" src="${icon}" alt="${label}" />
            </span>
            <span class="metric-down">↓ ${clippedText}</span>
          </div>
        </div>
        <div class="quality-improvement">${improvement}</div>
      `;
      qualityCards.appendChild(card);
    });
    const qualityLayout = document.createElement('div');
    qualityLayout.className = 'leaderboard-split';
    const qualityPodiumPanel = document.createElement('div');
    qualityPodiumPanel.className = 'leaderboard-left-panel';
    qualityPodiumPanel.appendChild(podium);
    qualityLayout.appendChild(qualityPodiumPanel);
    const qualityPanel = document.createElement('div');
    qualityPanel.className = 'leaderboard-right-panel';
    qualityPanel.appendChild(qualityCards);
    qualityLayout.appendChild(qualityPanel);
    els.leaderboardList.appendChild(qualityLayout);
  }

  if (leaderboardTab === 'sanity' && sanityRows.length === 0) {
    const empty = document.createElement('p');
    empty.className = 'leaderboard-summary';
    empty.textContent = 'No sanity data available for this benchmark.';
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
  if (!els.navRail || !els.dashboardTabBtn || !els.leaderboardBtn) return;
  const referenceEl =
    currentMode === 'leaderboard' && els.controlsPanel
      ? els.controlsPanel
      : els.sqlEditorWrap;
  if (!referenceEl) return;

  const referenceRect = referenceEl.getBoundingClientRect();
  if (!Number.isFinite(referenceRect.height) || referenceRect.height <= 0) return;
  const railStyle = window.getComputedStyle(els.navRail);
  if (railStyle.flexDirection === 'row') {
    els.navRail.style.height = '';
    els.navRail.style.minHeight = '';
    els.navRail.style.maxHeight = '';
    [els.dashboardTabBtn, els.leaderboardBtn].forEach((btn) => {
      btn.style.height = '';
      btn.style.minHeight = '';
      btn.style.maxHeight = '';
    });
    return;
  }

  const syncedHeight = referenceRect.height;
  const paddingTop = parseFloat(railStyle.paddingTop) || 0;
  const paddingBottom = parseFloat(railStyle.paddingBottom) || 0;
  const gap = parseFloat(railStyle.rowGap || railStyle.gap) || 0;
  const buttons = [els.dashboardTabBtn, els.leaderboardBtn];
  const totalGap = gap * Math.max(0, buttons.length - 1);
  const innerHeight = Math.max(0, syncedHeight - paddingTop - paddingBottom - totalGap);
  const buttonHeightPx = `${innerHeight / buttons.length}px`;
  const railHeightPx = `${syncedHeight}px`;

  els.navRail.style.height = railHeightPx;
  els.navRail.style.minHeight = railHeightPx;
  els.navRail.style.maxHeight = railHeightPx;
  buttons.forEach((btn) => {
    btn.style.height = buttonHeightPx;
    btn.style.minHeight = buttonHeightPx;
    btn.style.maxHeight = buttonHeightPx;
  });
}

function setMode(mode) {
  currentMode = mode;
  if (els.dashboardTabBtn) els.dashboardTabBtn.classList.toggle('active', mode === 'run' || mode === 'plan');
  if (els.planViewBtn) els.planViewBtn.classList.toggle('active', mode === 'plan');
  els.leaderboardBtn.classList.toggle('active', mode === 'leaderboard');

  els.runPanel.classList.toggle('hidden', mode !== 'run');
  els.planPanel.classList.toggle('hidden', mode !== 'plan');
  els.leaderboardPanel.classList.toggle('hidden', mode !== 'leaderboard');
  if (els.sqlPanel) els.sqlPanel.classList.toggle('hidden', mode === 'leaderboard');
  if (els.appShell) els.appShell.classList.toggle('leaderboard-mode', mode === 'leaderboard');

  els.planControls.classList.toggle('hidden', mode !== 'plan');
  if (els.queryControls) els.queryControls.classList.toggle('hidden', mode === 'leaderboard');

  const entries = activeEntries();
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

  const benchmarkWarning = benchmarkLoadWarnings.get(els.benchmarkSelect.value);
  if (benchmarkWarning) {
    els.statusText.textContent = `Warning: ${benchmarkWarning}`;
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
        nextQueries[canonicalName] ||= { sql: '', actual: 0, estimates: {}, xbound: {} };
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

        nextQueries[canonicalName] ||= { sql: '', actual: 0, estimates: {}, xbound: {} };
        const current = nextQueries[canonicalName];
        nextQueries[canonicalName] = {
          ...current,
          sql: loaded.sql || current.sql,
          actual: loaded.actual || current.actual,
          estimates: { ...(current.estimates || {}), ...(loaded.estimates || {}) },
          xbound: { ...(current.xbound || {}), ...(loaded.xbound || {}) }
        };
      });
    }
    queryStore[benchmark] = nextQueries;
    benchmarkLoadWarnings.delete(benchmark);
    if (benchmark === els.benchmarkSelect.value) {
      els.statusText.textContent = `Loaded precomputed estimates from ${estimateResult.sourcePath}`;
    }
  } catch (err) {
    loadedBenchmarks.delete(cacheKey);
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
  if (els.benchmarkSelect.value !== 'SO-CEB') return;
  const refreshSeq = ++xboundSliderRefreshSeq;
  customQueryData = null;
  await ensureBenchmarkLoaded('SO-CEB');
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
    updateQuerySelector();
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
  els.appName.textContent = window.xbound?.appName || 'xBound (Web)';
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
    setMode('run');
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
