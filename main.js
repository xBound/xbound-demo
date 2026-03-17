const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');
const fs = require('fs/promises');

const ESTIMATION_SUFFIX = path.join('LpBound', 'benchmarks', 'est');
const WORKLOAD_SUFFIX = path.join('LpBound', 'benchmarks', 'workloads');

function estimationRoots() {
  const roots = [];
  if (process.env.XBOUND_EST_ROOT) roots.push(process.env.XBOUND_EST_ROOT);
  roots.push(path.join(__dirname, '..', 'robust_memory_estimation'));
  roots.push(path.join(__dirname, 'robust_memory_estimation'));
  return Array.from(new Set(roots.map((root) => path.resolve(root, ESTIMATION_SUFFIX))));
}

function workloadRoots() {
  const roots = [];
  if (process.env.XBOUND_WORKLOAD_ROOT) roots.push(process.env.XBOUND_WORKLOAD_ROOT);
  roots.push(path.join(__dirname, '..', 'robust_memory_estimation'));
  roots.push(path.join(__dirname, 'robust_memory_estimation'));
  return Array.from(new Set(roots.map((root) => path.resolve(root, WORKLOAD_SUFFIX))));
}

function benchmarkAliases(benchmarkName) {
  const benchmark = String(benchmarkName || '').trim().toLowerCase();
  return Array.from(new Set([
    benchmark,
    benchmark.replace(/[^a-z0-9]+/g, ''),
    benchmark === 'job' ? 'joblight' : null,
    benchmark === 'joblight' ? 'job' : null,
    benchmark === 'tpc-h' ? 'tpch' : null,
    benchmark === 'tpch' ? 'tpc-h' : null
  ].filter(Boolean)));
}

function systemKey(systemName) {
  const normalized = String(systemName || '').trim().toLowerCase();
  if (normalized.includes('duckdb')) return 'duckdb';
  if (normalized.includes('postgres')) return 'postgres';
  if (normalized.includes('fabric')) return 'fabric dw';
  return null;
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

function loadFromJsonlLines(lines) {
  const queryMap = {};

  lines.forEach((rawLine) => {
    const line = rawLine.trim();
    if (!line) return;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      return;
    }

    const qKey = queryKey(obj);
    if (!qKey) return;

    queryMap[qKey] ||= { estimates: {}, xbound: {} };
    const entry = queryMap[qKey];
    if (!entry.sql && typeof obj.sql === 'string') entry.sql = obj.sql;
    if (!entry.actual) {
      entry.actual = numericValue(obj.actual, obj.act, obj.actual_cardinality, obj.true_cardinality, obj.ground_truth);
    }

    if (obj.estimates && typeof obj.estimates === 'object') {
      Object.entries(obj.estimates).forEach(([name, estimate]) => {
        const system = systemKey(name);
        const value = numericValue(estimate);
        if (system && value !== null) entry.estimates[system] = value;
      });
    }

    if (obj.xbound && typeof obj.xbound === 'object') {
      Object.entries(obj.xbound).forEach(([name, estimate]) => {
        const system = systemKey(name);
        const value = numericValue(estimate);
        if (system && value !== null) entry.xbound[system] = value;
      });
    }

    const system = systemKey(obj.system || obj.dbms || obj.engine);
    const estimate = numericValue(obj.estimate, obj.estimated_cardinality, obj.predicted_cardinality);
    const isXBound = Boolean(obj.is_xbound || obj.xbounded || String(obj.variant || '').toLowerCase().includes('xbound'));
    if (system && estimate !== null) {
      if (isXBound) entry.xbound[system] = estimate;
      else entry.estimates[system] = estimate;
    }
  });

  return queryMap;
}

async function readBenchmarkEstimates(benchmarkName) {
  const benchmark = String(benchmarkName || '').trim().toLowerCase();
  if (!benchmark) return { ok: false, error: 'invalid benchmark', queries: {} };

  const aliases = benchmarkAliases(benchmarkName);

  const searchedPaths = [];

  for (const root of estimationRoots()) {
    for (const alias of aliases) {
      const filePath = path.join(root, alias, `system::${alias}-queries.jsonl`);
      searchedPaths.push(filePath);
      try {
        const content = await fs.readFile(filePath, 'utf8');
        const lines = content.split(/\r?\n/);
        return {
          ok: true,
          sourcePath: filePath,
          queries: loadFromJsonlLines(lines)
        };
      } catch {
        // Try next candidate.
      }
    }
  }

  // Fallback: per-system estimate files, e.g. duckdb::..., postgres::..., dw::..., xbound::...
  for (const root of estimationRoots()) {
    for (const alias of aliases) {
      const dirPath = path.join(root, alias);
      try {
        const files = await fs.readdir(dirPath);
        const queries = {};

        const upsert = (obj) => {
          const key = queryKey(obj);
          if (!key) return;
          queries[key] ||= { estimates: {}, xbound: {} };
          const entry = queries[key];
          if (!entry.sql && typeof obj.sql === 'string') entry.sql = obj.sql;
          if (!entry.actual) {
            entry.actual = numericValue(obj.actual, obj.act, obj.actual_cardinality, obj.true_cardinality, obj.ground_truth);
          }
        };

        for (const file of files) {
          const fullPath = path.join(dirPath, file);
          if (!file.endsWith(`::${alias}-queries.jsonl`) && !file.startsWith(`xbound::${alias}-queries`)) continue;
          const content = await fs.readFile(fullPath, 'utf8');
          const lines = content.split(/\r?\n/);

          for (const rawLine of lines) {
            const line = rawLine.trim();
            if (!line) continue;
            let obj;
            try {
              obj = JSON.parse(line);
            } catch {
              continue;
            }

            upsert(obj);
            const key = queryKey(obj);
            if (!key) continue;
            const entry = queries[key];

            if (file.startsWith('duckdb::')) {
              const val = numericValue(obj.duckdb);
              if (val !== null) entry.estimates.duckdb = val;
            } else if (file.startsWith('postgres::')) {
              const val = numericValue(obj.postgres);
              if (val !== null) entry.estimates.postgres = val;
            } else if (file.startsWith('dw::')) {
              const val = numericValue(obj.dw);
              if (val !== null) entry.estimates['fabric dw'] = val;
            } else if (file.startsWith('xbound::')) {
              const val = numericValue(obj.xbound, obj?.meta?.best?.val);
              if (val !== null) {
                entry.xbound.duckdb = val;
                entry.xbound.postgres = val;
                entry.xbound['fabric dw'] = val;
              }
            }
          }
        }

        if (Object.keys(queries).length > 0) {
          return {
            ok: true,
            sourcePath: dirPath,
            queries
          };
        }
      } catch {
        // Try next candidate.
      }
    }
  }

  return {
    ok: false,
    error: `No file found for benchmark ${benchmarkName}`,
    searchedPaths,
    queries: {}
  };
}

async function readWorkloadQueries(benchmarkName) {
  const aliases = benchmarkAliases(benchmarkName);
  const searchedPaths = [];

  for (const root of workloadRoots()) {
    for (const alias of aliases) {
      const filePath = path.join(root, alias, `${alias}-queries.jsonl`);
      searchedPaths.push(filePath);
      try {
        const content = await fs.readFile(filePath, 'utf8');
        const queries = {};
        content.split(/\r?\n/).forEach((rawLine) => {
          const line = rawLine.trim();
          if (!line) return;
          let obj;
          try {
            obj = JSON.parse(line);
          } catch {
            return;
          }
          const key = queryKey(obj);
          if (!key) return;
          queries[key] ||= {};
          if (typeof obj.sql === 'string') queries[key].sql = obj.sql;
        });

        return {
          ok: true,
          sourcePath: filePath,
          queries
        };
      } catch {
        // Try next candidate.
      }
    }
  }

  return {
    ok: false,
    error: `No workload file found for benchmark ${benchmarkName}`,
    searchedPaths,
    queries: {}
  };
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1100,
    minHeight: 700,
    backgroundColor: '#f4f6fb',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  win.loadFile(path.join(__dirname, 'src', 'index.html'));
}

app.whenReady().then(() => {
  ipcMain.handle('xbound:load-precomputed-estimates', async (_event, benchmark) => {
    return readBenchmarkEstimates(benchmark);
  });
  ipcMain.handle('xbound:load-workload-queries', async (_event, benchmark) => {
    return readWorkloadQueries(benchmark);
  });

  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
