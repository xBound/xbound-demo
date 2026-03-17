const SYSTEMS = ['duckdb', 'postgres', 'fabric dw'];
const XBOUND_SUFFIX = ' xBound-ed';

const MOCK_QUERIES = {
  'TPC-H': {
    Q1: {
      sql: 'select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty from lineitem group by 1,2;',
      actual: 1475320,
      estimates: {
        duckdb: 1600000,
        postgres: 1254000,
        'fabric dw': 1805000
      },
      xbound: {
        duckdb: 1503000,
        postgres: 1438000,
        'fabric dw': 1491000
      }
    },
    Q3: {
      sql: 'select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue from lineitem group by 1;',
      actual: 603110,
      estimates: {
        duckdb: 522000,
        postgres: 790000,
        'fabric dw': 580000
      },
      xbound: {
        duckdb: 590000,
        postgres: 640000,
        'fabric dw': 610500
      }
    },
    Q6: {
      sql: 'select sum(l_extendedprice * l_discount) as revenue from lineitem where l_discount between 0.05 and 0.07;',
      actual: 92450,
      estimates: {
        duckdb: 130000,
        postgres: 77000,
        'fabric dw': 102500
      },
      xbound: {
        duckdb: 98100,
        postgres: 93000,
        'fabric dw': 91500
      }
    }
  },
  JOB: {
    Q2: {
      sql: 'select t.title, mi.info from title t join movie_info mi on t.id = mi.movie_id where t.production_year > 2015;',
      actual: 315220,
      estimates: {
        duckdb: 402100,
        postgres: 280000,
        'fabric dw': 468000
      },
      xbound: {
        duckdb: 331000,
        postgres: 304000,
        'fabric dw': 322200
      }
    },
    Q9: {
      sql: 'select count(*) from cast_info ci join name n on n.id = ci.person_id where n.gender = "m";',
      actual: 1600450,
      estimates: {
        duckdb: 1200000,
        postgres: 2450000,
        'fabric dw': 1770000
      },
      xbound: {
        duckdb: 1520000,
        postgres: 1720000,
        'fabric dw': 1662000
      }
    },
    Q17: {
      sql: 'select t.title from title t join movie_keyword mk on t.id = mk.movie_id where mk.keyword_id in (12,31,77);',
      actual: 81030,
      estimates: {
        duckdb: 130400,
        postgres: 62000,
        'fabric dw': 72000
      },
      xbound: {
        duckdb: 86000,
        postgres: 79000,
        'fabric dw': 80400
      }
    }
  }
};

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

function getCurrentQueryData() {
  const benchmark = els.benchmarkSelect.value;
  const queryName = els.querySelect.value;
  return MOCK_QUERIES[benchmark]?.[queryName];
}

function buildEstimateEntries(includeXBound) {
  const queryData = getCurrentQueryData();
  if (!queryData) return [];

  const entries = SYSTEMS.map((system) => ({
    system,
    estimate: queryData.estimates[system],
    actual: queryData.actual,
    qError: queryData.estimates[system] / queryData.actual
  }));

  if (includeXBound) {
    SYSTEMS.forEach((system) => {
      entries.push({
        system: `${system}${XBOUND_SUFFIX}`,
        estimate: queryData.xbound[system],
        actual: queryData.actual,
        qError: queryData.xbound[system] / queryData.actual
      });
    });
  }

  return entries;
}

function renderQErrorBarPlot(entries) {
  const canvas = els.chartCanvas;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = canvas.clientWidth || canvas.width;
  const cssHeight = canvas.clientHeight || canvas.height;

  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

  const margin = { top: 20, right: 26, bottom: 76, left: 68 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;
  const baselineY = margin.top + height / 2;

  const maxDeviation = Math.max(0.5, ...entries.map((e) => Math.abs(e.qError - 1)));
  const domainMin = 1 - maxDeviation * 1.15;
  const domainMax = 1 + maxDeviation * 1.15;

  const y = (value) => {
    const ratio = (value - domainMin) / (domainMax - domainMin);
    return margin.top + height - ratio * height;
  };

  const xStep = width / Math.max(1, entries.length);

  ctx.clearRect(0, 0, cssWidth, cssHeight);
  ctx.fillStyle = '#ffffff';
  ctx.fillRect(0, 0, cssWidth, cssHeight);

  const ticks = [domainMin, (domainMin + 1) / 2, 1, (domainMax + 1) / 2, domainMax];

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
    ctx.fillText(`${tick.toFixed(2)}x`, 8, yy + 4);
  });

  ctx.strokeStyle = '#1e2b54';
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.moveTo(margin.left, baselineY);
  ctx.lineTo(cssWidth - margin.right, baselineY);
  ctx.stroke();

  ctx.fillStyle = '#1e2b54';
  ctx.font = '12px IBM Plex Sans, sans-serif';
  ctx.fillText('y = 1 (actual)', margin.left + 8, baselineY - 8);

  entries.forEach((entry, idx) => {
    const centerX = margin.left + xStep * idx + xStep / 2;
    const barWidth = Math.min(42, xStep * 0.58);
    const barTop = y(entry.qError);
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
    const qLabelY = entry.qError >= 1 ? barTop - 6 : barTop + 15;
    ctx.fillText(label, centerX - barWidth / 2 - 6, cssHeight - 36);
    ctx.fillText(`${entry.qError.toFixed(2)}x`, centerX - barWidth / 2 - 2, qLabelY);
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
  Object.keys(MOCK_QUERIES).forEach((benchmark) => {
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
  const queries = Object.keys(MOCK_QUERIES[benchmark]);

  els.querySelect.innerHTML = '';
  queries.forEach((q) => {
    const option = document.createElement('option');
    option.value = q;
    option.textContent = q;
    els.querySelect.appendChild(option);
  });

  syncSqlInput();
}

function syncSqlInput() {
  const queryData = getCurrentQueryData();
  els.sqlInput.value = queryData?.sql || '-- select a query';

  if (queryData) {
    els.statusText.textContent = `actual cardinality: ${queryData.actual.toLocaleString()}`;
  }
}

function bindEvents() {
  els.benchmarkSelect.addEventListener('change', () => {
    updateQuerySelector();
    const entries = activeEntries();
    if (currentMode === 'run') renderQErrorBarPlot(entries);
    if (currentMode === 'leaderboard') renderLeaderboard(entries);
  });

  els.querySelect.addEventListener('change', () => {
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

  els.runBtn.addEventListener('click', () => {
    setMode('run');
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

function init() {
  els.appName.textContent = window.xbound?.appName || 'xBound';
  populateSelectors();
  bindEvents();
  setMode('run');
}

init();
