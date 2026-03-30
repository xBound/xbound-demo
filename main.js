const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');
const fsSync = require('fs');
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
const SIZE_DIR_MAP = {
  joblight: { relDir: path.join('imdb', 'joblight'), prefix: 'joblight' },
  so_full_ceb: { relDir: path.join('so_full', 'so_full_ceb'), prefix: 'so_full_ceb' },
  stats_ceb: { relDir: path.join('stats', 'stats_ceb'), prefix: 'stats_ceb' }
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

function sizeRoots() {
  const roots = [];
  if (process.env.XBOUND_SIZE_ROOT) roots.push(process.env.XBOUND_SIZE_ROOT);
  roots.push(path.join(__dirname, '..', 'robust_memory_estimation', 'src', 'xbound', 'results', 'size'));
  roots.push(path.join(__dirname, 'robust_memory_estimation', 'src', 'xbound', 'results', 'size'));
  return Array.from(new Set(roots.map((root) => path.resolve(root))));
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

    queryMap[qKey] ||= { estimates: {}, xbound: {}, lpbound: {} };
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

    if (obj.lpbound && typeof obj.lpbound === 'object') {
      Object.entries(obj.lpbound).forEach(([name, estimate]) => {
        const system = systemKey(name);
        const value = numericValue(estimate);
        if (system && value !== null) entry.lpbound[system] = value;
      });
    }

    const lpboundKey = Object.keys(obj).find((k) => /^lpbound/i.test(String(k)));
    const lpboundValue = numericValue(lpboundKey ? obj[lpboundKey] : undefined, obj.upper_bound, obj.ub);
    if (lpboundValue !== null) {
      entry.lpbound.duckdb = lpboundValue;
      entry.lpbound.postgres = lpboundValue;
      entry.lpbound['fabric dw'] = lpboundValue;
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

function xboundPreferredSuffix(alias, xboundParams) {
  if (alias === 'so_full_ceb' && xboundParams && typeof xboundParams === 'object') {
    const parts = Number(xboundParams.parts);
    const l0Theta = Number(xboundParams.l0Theta);
    const hhTheta = Number(xboundParams.hhTheta);
    if (Number.isFinite(parts) && Number.isFinite(l0Theta) && Number.isFinite(hhTheta)) {
      return `_host=hausberg_parts=${Math.trunc(parts)}_ns=1_ub=0_l0-theta=${Math.trunc(l0Theta)}_hh-theta=${Math.trunc(hhTheta)}_mcv=1024.jsonl`;
    }
  }
  return XBOUND_FILE_PREFERENCE[alias];
}

function parseParquetBytes(csvText) {
  const lines = String(csvText || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length <= 1) return null;
  const header = lines[0].split(',').map((s) => s.trim().toLowerCase());
  const parquetIdx = header.indexOf('parquet_bytes');
  let total = 0;
  let hasValue = false;
  for (let i = 1; i < lines.length; i += 1) {
    const cols = lines[i].split(',').map((s) => s.trim());
    const raw = parquetIdx >= 0 ? cols[parquetIdx] : cols[0];
    const n = Number(raw);
    if (!Number.isFinite(n)) continue;
    total += n;
    hasValue = true;
  }
  return hasValue ? total : null;
}

async function readCsvParquetBytes(filePath) {
  const content = await fs.readFile(filePath, 'utf8');
  return parseParquetBytes(content);
}

async function readXboundStatsSize(benchmarkName, xboundParams = null) {
  const benchmark = canonicalBenchmark(benchmarkName);
  const cfg = SIZE_DIR_MAP[benchmark];
  if (!cfg) {
    return { ok: false, error: `Unsupported benchmark for size lookup: ${benchmarkName}` };
  }

  const parts = Math.trunc(Number(xboundParams?.parts) || 16);
  const l0Theta = Math.trunc(Number(xboundParams?.l0Theta) || 8);
  const hhTheta = Math.trunc(Number(xboundParams?.hhTheta) || 12);
  const lightName = `${cfg.prefix}-hausberg-heavy_theta_l0_norms-p=${parts}-th=${l0Theta}.csv`;
  const heavyName = `${cfg.prefix}-hausberg-hh_info-th=${hhTheta}.csv`;

  const tried = [];
  for (const root of sizeRoots()) {
    const dirPath = path.join(root, cfg.relDir);
    const lightPath = path.join(dirPath, lightName);
    const heavyPath = path.join(dirPath, heavyName);
    tried.push(lightPath, heavyPath);
    try {
      const [lightBytes, heavyBytes] = await Promise.all([
        readCsvParquetBytes(lightPath),
        readCsvParquetBytes(heavyPath)
      ]);
      if (!Number.isFinite(lightBytes) || !Number.isFinite(heavyBytes)) {
        return {
          ok: false,
          error: 'parquet_bytes not found in one or both size files',
          files: { lightPath, heavyPath }
        };
      }
      const totalBytes = lightBytes + heavyBytes;
      return {
        ok: true,
        benchmark,
        params: { parts, l0Theta, hhTheta },
        bytes: totalBytes,
        mb: totalBytes / (1024 * 1024),
        files: { lightPath, heavyPath },
        components: { lightBytes, heavyBytes }
      };
    } catch {
      // Try next root.
    }
  }

  return {
    ok: false,
    error: `No matching size files found for ${benchmarkName} with parts=${parts}, l0-theta=${l0Theta}, hh-theta=${hhTheta}`,
    tried
  };
}

async function readBenchmarkEstimates(benchmarkName, xboundParams = null) {
  const benchmark = String(benchmarkName || '').trim().toLowerCase();
  if (!benchmark) return { ok: false, error: 'invalid benchmark', queries: {} };

  const aliases = benchmarkAliases(benchmarkName);

  const searchedPaths = [];
  let missingXboundVariant = null;

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
          queries[key] ||= { estimates: {}, xbound: {}, lpbound: {} };
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
            } else if (file.startsWith('lpbound')) {
              const lpKey = Object.keys(obj).find((k) => /^lpbound/i.test(String(k)));
              const val = numericValue(lpKey ? obj[lpKey] : undefined, obj.lpbound, obj.upper_bound, obj.ub);
              if (val !== null) {
                entry.lpbound.duckdb = val;
                entry.lpbound.postgres = val;
                entry.lpbound['fabric dw'] = val;
              }
            }
          }
        }

        // Handle xbound with deterministic file choice.
        const xboundFiles = files
          .filter((name) => name.startsWith('xbound::') && name.includes(`::${alias}-queries`) && name.endsWith('.jsonl') && isPreferredEstimateFile(alias, name))
          .sort();
        if (xboundFiles.length) {
          const preferredSuffix = xboundPreferredSuffix(alias, xboundParams);
          let chosen = xboundFiles[0];
          if (preferredSuffix) {
            const exact = xboundFiles.find((name) => name.endsWith(preferredSuffix));
            if (!exact) {
              const baseName = xboundFiles[0].replace(/_(host|hostname)=.*$/, '');
              missingXboundVariant = {
                benchmark: benchmarkName,
                fileName: `${baseName}${preferredSuffix}`
              };
              continue;
            }
            chosen = exact;
          }
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

  if (missingXboundVariant) {
    return {
      ok: false,
      code: 'MISSING_XBOUND_FILE',
      benchmark: missingXboundVariant.benchmark,
      fileName: missingXboundVariant.fileName,
      error: `missing xBound file for ${missingXboundVariant.benchmark} (${missingXboundVariant.fileName})`,
      searchedPaths,
      queries: {}
    };
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

async function estimateCustomQuery(benchmarkName, sql, xboundParams = null, queryTag = null) {
  const benchmark = canonicalBenchmark(benchmarkName);
  const scriptPath = path.join(__dirname, 'scripts', 'custom_query_estimator.py');
  const robustRoot = process.env.XBOUND_ROBUST_ROOT
    ? path.resolve(process.env.XBOUND_ROBUST_ROOT)
    : path.resolve(__dirname, '..', 'robust_memory_estimation');
  const venvPython = process.env.VIRTUAL_ENV
    ? path.join(process.env.VIRTUAL_ENV, 'bin', 'python')
    : null;
  const localVenvPython = path.join(__dirname, '.venv', 'bin', 'python');
  const pythonBin = process.env.XBOUND_PYTHON
    || (venvPython && fsSync.existsSync(venvPython) ? venvPython : null)
    || (fsSync.existsSync(localVenvPython) ? localVenvPython : null);
  if (!pythonBin) {
    return {
      benchmark,
      sql,
      actual: null,
      estimates: {},
      xbound: {},
      lpbound: {},
      errors: {
        estimator: 'No allowed Python env found. Activate your demo env (VIRTUAL_ENV) or set XBOUND_PYTHON.'
      },
      debug: {}
    };
  }
  console.error('[estimate-custom-query][python]', pythonBin);
  try {
    const { stdout, stderr } = await execFileAsync(
      pythonBin,
      [
        scriptPath,
        '--benchmark', benchmark,
        '--sql', sql,
        '--query-tag', String(queryTag || ''),
        '--robust-root', robustRoot,
        '--parts', String(Number(xboundParams?.parts) || 16),
        '--l0-theta', String(Number(xboundParams?.l0Theta) || 8),
        '--hh-theta', String(Number(xboundParams?.hhTheta) || 12)
      ],
      { maxBuffer: 1024 * 1024 * 4 }
    );
    if (stdout && stdout.trim()) {
      console.error('[estimate-custom-query][raw-stdout]\n' + stdout.trim());
    }
    if (stderr && stderr.trim()) {
      console.error('[estimate-custom-query][stderr]', stderr.trim());
    }
    let parsed = null;
    const raw = String(stdout || '').trim();
    try {
      parsed = JSON.parse(raw);
    } catch {
      // xBound/run.py can emit plain logs to stdout; try last JSON-looking line.
      const lines = raw.split(/\r?\n/).map((line) => line.trim()).filter(Boolean).reverse();
      const candidate = lines.find((line) => line.startsWith('{') && line.endsWith('}'));
      if (candidate) {
        parsed = JSON.parse(candidate);
      } else {
        throw new Error('No JSON payload found in estimator stdout');
      }
    }
    if (parsed?.debug?.xbound_runpy_workload_dump) {
      console.error('[estimate-custom-query][xbound-runpy-workload]', parsed.debug.xbound_runpy_workload_dump);
    }
    if (parsed?.debug?.xbound_runpy_stdout) {
      console.error('[estimate-custom-query][xbound-runpy-stdout]\n' + parsed.debug.xbound_runpy_stdout);
    }
    if (parsed?.debug?.xbound_runpy_stderr) {
      console.error('[estimate-custom-query][xbound-runpy-stderr]\n' + parsed.debug.xbound_runpy_stderr);
    }
    if (parsed?.errors?.xbound) {
      console.error('[estimate-custom-query][xbound-runpy-error]', parsed.errors.xbound);
    }
    if (parsed?.debug?.lpbound_run_estimator_workload_dump) {
      console.error('[estimate-custom-query][lpbound-run-estimator-workload]', parsed.debug.lpbound_run_estimator_workload_dump);
    }
    if (parsed?.debug?.lpbound_run_estimator_stdout) {
      console.error('[estimate-custom-query][lpbound-run-estimator-stdout]\n' + parsed.debug.lpbound_run_estimator_stdout);
    }
    if (parsed?.debug?.lpbound_run_estimator_stderr) {
      console.error('[estimate-custom-query][lpbound-run-estimator-stderr]\n' + parsed.debug.lpbound_run_estimator_stderr);
    }
    if (parsed?.errors?.lpbound) {
      console.error('[estimate-custom-query][lpbound-run-estimator-error]', parsed.errors.lpbound);
    }
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
    return {
      benchmark,
      sql,
      actual: null,
      estimates: {},
      xbound: {},
      lpbound: {},
      errors: {
        estimator: error?.message || 'unknown estimator failure'
      },
      debug: {
        raw_stdout: error?.stdout || null,
        raw_stderr: error?.stderr || null
      }
    };
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

  ipcMain.handle('xbound:load-precomputed-estimates', async (_event, benchmark, xboundParams) => {
    return readBenchmarkEstimates(benchmark, xboundParams);
  });
  ipcMain.handle('xbound:load-workload-queries', async (_event, benchmark) => {
    return readWorkloadQueries(benchmark);
  });
  ipcMain.handle('xbound:estimate-custom-query', async (_event, benchmark, sql, xboundParams, queryTag) => {
    return estimateCustomQuery(benchmark, sql, xboundParams, queryTag);
  });
  ipcMain.handle('xbound:load-stats-size', async (_event, benchmark, xboundParams) => {
    return readXboundStatsSize(benchmark, xboundParams);
  });

  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
