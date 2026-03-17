const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');
const fs = require('fs/promises');

const ESTIMATION_SUFFIX = path.join('LpBound', 'benchmarks', 'est');
const WORKLOAD_SUFFIX = path.join('LpBound', 'benchmarks', 'workloads');
const { execFile } = require('child_process');
const { promisify } = require('util');
const execFileAsync = promisify(execFile);
const APP_ICON_PATH = path.join(__dirname, 'icons', 'xbound-icon.png');

const BENCHMARK_CANONICAL_MAP = {
  joblight: 'joblight',
  job: 'joblight',
  'job-light': 'joblight',
  'so-ceb': 'so_full_ceb',
  so_ceb: 'so_full_ceb',
  sofullceb: 'so_full_ceb',
  so_full_ceb: 'so_full_ceb',
  'stats-ceb': 'stats_ceb',
  stats_ceb: 'stats_ceb',
  statsceb: 'stats_ceb'
};
const XBOUND_FILE_PREFERENCE = {
  joblight: '_host=hausberg_parts=16_ns=1_ub=0_l0-theta=8_hh-theta=12_mcv=1024.jsonl',
  so_full_ceb: '_host=hausberg_parts=16_ns=1_ub=0_l0-theta=8_hh-theta=12_mcv=1024.jsonl',
  stats_ceb: '_host=hausberg_parts=16_ns=1_ub=0_l0-theta=8_hh-theta=12_mcv=1024.jsonl'
};
const WORKLOAD_FILE_PREFERENCE = {
  so_full_ceb: 'so_full_ceb-queries00.jsonl',
  joblight: 'joblight-queries.jsonl',
  stats_ceb: 'stats_ceb-queries.jsonl'
};

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
  const canonical = BENCHMARK_CANONICAL_MAP[benchmark] || BENCHMARK_CANONICAL_MAP[benchmark.replace(/[^a-z0-9_]+/g, '')] || benchmark;
  return Array.from(new Set([
    canonical,
    canonical.replace(/[^a-z0-9]+/g, ''),
    canonical === 'joblight' ? 'job' : null,
    canonical === 'so_full_ceb' ? 'so-ceb' : null,
    canonical === 'stats_ceb' ? 'stats-ceb' : null
  ].filter(Boolean)));
}

function canonicalBenchmark(benchmarkName) {
  const benchmark = String(benchmarkName || '').trim().toLowerCase();
  return BENCHMARK_CANONICAL_MAP[benchmark] || BENCHMARK_CANONICAL_MAP[benchmark.replace(/[^a-z0-9_]+/g, '')] || benchmark;
}

function isPreferredEstimateFile(alias, fileName) {
  if (alias === 'so_full_ceb') {
    return fileName.includes('queries00');
  }
  return true;
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

  for (const root of estimationRoots()) {
    for (const alias of aliases) {
      const dirPath = path.join(root, alias);
      try {
        const files = await fs.readdir(dirPath);
        const systemFile = files.find((name) =>
          name.startsWith('system::') &&
          name.includes(`::${alias}-queries`) &&
          name.endsWith('.jsonl') &&
          isPreferredEstimateFile(alias, name)
        );
        if (!systemFile) continue;
        const filePath = path.join(dirPath, systemFile);
        searchedPaths.push(filePath);
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
          if (!(file.includes(`::${alias}-queries`) && file.endsWith('.jsonl'))) continue;
          if (!isPreferredEstimateFile(alias, file)) continue;
          if (file.startsWith('xbound::')) continue;
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

        // Handle xbound with deterministic file choice.
        const xboundFiles = files
          .filter((name) => name.startsWith('xbound::') && name.includes(`::${alias}-queries`) && name.endsWith('.jsonl') && isPreferredEstimateFile(alias, name))
          .sort();
        if (xboundFiles.length) {
          const preferredSuffix = XBOUND_FILE_PREFERENCE[alias];
          const chosen = (preferredSuffix && xboundFiles.find((name) => name.endsWith(preferredSuffix))) || xboundFiles[0];
          const content = await fs.readFile(path.join(dirPath, chosen), 'utf8');
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
            const val = numericValue(obj.xbound, obj?.meta?.best?.val);
            if (val !== null) {
              entry.xbound.duckdb = val;
              entry.xbound.postgres = val;
              entry.xbound['fabric dw'] = val;
            }
          }
          console.error(`[xbound] using ${path.join(dirPath, chosen)}`);
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

      try {
        const dirPath = path.join(root, alias);
        const files = await fs.readdir(dirPath);
        const preferred = WORKLOAD_FILE_PREFERENCE[alias];
        const workloadFile = (preferred && files.includes(preferred))
          ? preferred
          : files
            .filter((name) => name.startsWith(`${alias}-queries`) && name.endsWith('.jsonl'))
            .sort()[0];
        if (!workloadFile) continue;
        const fullPath = path.join(dirPath, workloadFile);
        searchedPaths.push(fullPath);
        console.error(`[workload] using ${fullPath}`);
        const content = await fs.readFile(fullPath, 'utf8');
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
          sourcePath: fullPath,
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

async function estimateCustomQuery(benchmarkName, sql) {
  const benchmark = canonicalBenchmark(benchmarkName);
  const scriptPath = path.join(__dirname, 'scripts', 'custom_query_estimator.py');
  const robustRoot = process.env.XBOUND_ROBUST_ROOT
    ? path.resolve(process.env.XBOUND_ROBUST_ROOT)
    : path.resolve(__dirname, '..', 'robust_memory_estimation');
  const pythonBin = process.env.XBOUND_PYTHON || 'python3';
  try {
    const { stdout, stderr } = await execFileAsync(
      pythonBin,
      [scriptPath, '--benchmark', benchmark, '--sql', sql, '--robust-root', robustRoot],
      { maxBuffer: 1024 * 1024 * 4 }
    );
    if (stderr && stderr.trim()) {
      console.error('[estimate-custom-query][stderr]', stderr.trim());
    }
    const parsed = JSON.parse(stdout);
    if (parsed?.errors && Object.keys(parsed.errors).length) {
      console.error('[estimate-custom-query][system-errors]', parsed.errors);
    }
    return parsed;
  } catch (error) {
    console.error('[estimate-custom-query][failed]', {
      benchmark,
      message: error?.message,
      stdout: error?.stdout,
      stderr: error?.stderr
    });
    throw error;
  }
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1100,
    minHeight: 700,
    backgroundColor: '#f4f6fb',
    icon: APP_ICON_PATH,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  win.loadFile(path.join(__dirname, 'src', 'index.html'));
}

app.whenReady().then(() => {
  if (process.platform === 'darwin') {
    app.dock.setIcon(APP_ICON_PATH);
  }

  ipcMain.handle('xbound:load-precomputed-estimates', async (_event, benchmark) => {
    return readBenchmarkEstimates(benchmark);
  });
  ipcMain.handle('xbound:load-workload-queries', async (_event, benchmark) => {
    return readWorkloadQueries(benchmark);
  });
  ipcMain.handle('xbound:estimate-custom-query', async (_event, benchmark, sql) => {
    return estimateCustomQuery(benchmark, sql);
  });

  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
