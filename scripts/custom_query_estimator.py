#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import tempfile
import shutil
from datetime import datetime
from glob import glob
from pathlib import Path


DB_FILE_CANDIDATES = {
  "joblight": ["imdb::joblight.duckdb", "imdb::base.duckdb"],
  "so_full_ceb": ["imdb::base.duckdb", "imdb::joblight.duckdb"],
  "stats_ceb": ["stats::stats_ceb.duckdb", "stats::base.duckdb"],
}

PG_DB_MAP = {
  "joblight": "imdb",
  "so_full_ceb": "so_full",
  "stats_ceb": "stats",
}


def _find_duckdb_est(plan_node):
  if isinstance(plan_node, dict):
    extra = plan_node.get("extra_info")
    if isinstance(extra, dict) and "Estimated Cardinality" in extra:
      try:
        return float(extra["Estimated Cardinality"])
      except Exception:
        pass
    for value in plan_node.values():
      est = _find_duckdb_est(value)
      if est is not None:
        return est
  if isinstance(plan_node, list):
    for child in plan_node:
      est = _find_duckdb_est(child)
      if est is not None:
        return est
  return None


def _find_pg_est(plan_node):
  if isinstance(plan_node, dict):
    if "Join Type" in plan_node and "Plan Rows" in plan_node:
      try:
        return float(plan_node["Plan Rows"])
      except Exception:
        pass
    for child in plan_node.get("Plans", []) or []:
      est = _find_pg_est(child)
      if est is not None:
        return est
  return None


def _resolve_duckdb_path(robust_root, benchmark):
  candidates = DB_FILE_CANDIDATES.get(benchmark, [])
  search_dirs = [
    Path(robust_root) / "src" / "xbound" / "dbs",
    Path(robust_root) / "dbs",
  ]

  tried = []
  for base_dir in search_dirs:
    for db_file in candidates:
      db_path = base_dir / db_file
      tried.append(str(db_path))
      if db_path.exists():
        return db_path

  raise FileNotFoundError(
    "No DuckDB file found for benchmark "
    f"{benchmark}. Tried: {', '.join(tried)}"
  )


def estimate_duckdb(robust_root, benchmark, sql):
  import duckdb
  db_path = _resolve_duckdb_path(robust_root, benchmark)
  con = duckdb.connect(str(db_path), read_only=True)
  row = con.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
  plan = json.loads(row[1])
  est = _find_duckdb_est(plan)
  con.close()
  return est


def actual_duckdb(robust_root, benchmark, sql):
  import duckdb
  db_path = _resolve_duckdb_path(robust_root, benchmark)
  con = duckdb.connect(str(db_path), read_only=True)
  try:
    # SQL is enforced in UI to be SELECT COUNT(*)..., so read that scalar directly.
    row = con.execute(sql).fetchone()
    if not row:
      return None
    return float(row[0])
  finally:
    con.close()


def estimate_postgres(benchmark, sql):
  import psycopg2
  db_name = PG_DB_MAP[benchmark]
  user = os.getenv("USER") or os.getenv("USERNAME") or "postgres"
  conn = psycopg2.connect(dbname=db_name, port=5432, host="localhost", user=user)
  conn.set_session(autocommit=True)
  cur = conn.cursor()
  cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
  plan = cur.fetchone()[0][0]["Plan"]
  est = _find_pg_est(plan)
  cur.close()
  conn.close()
  return est


def estimate_xbound(robust_root, benchmark, sql):
  root = Path(robust_root)
  sys.path.insert(0, str(root / "src"))
  sys.path.insert(0, str(root / "LpBound" / "src"))
  from global_utils.global_config import GlobalConfig
  from global_utils.common import SQLMode
  from global_utils.conn_utils import ConnectionWrapper
  from global_utils.schema_config import load_benchmark_schema
  from lpbound.acyclic.lpbound import estimate

  cfg = GlobalConfig(benchmark_name=benchmark)
  schema = load_benchmark_schema(cfg)
  conn = ConnectionWrapper(cfg, SQLMode.DUCKDB, read_only=True)
  try:
    return float(estimate(conn_wrapper=conn, schema_data=schema, input_query_sql=sql, config=cfg))
  finally:
    try:
      conn.close()
    except Exception:
      pass


def _normalize_sql(sql):
  return " ".join(str(sql or "").strip().rstrip(";").lower().split())


def _compact_sql(sql):
  return " ".join(str(sql or "").strip().rstrip(";").split()) + ";"


def _workload_name_for_benchmark(benchmark):
  if benchmark == "so_full_ceb":
    return "so_full_ceb-queries00"
  if benchmark == "joblight":
    return "joblight-queries"
  if benchmark == "stats_ceb":
    return "stats_ceb-queries"
  return f"{benchmark}-queries"


def _normalize_tag(tag):
  if tag is None:
    return None
  t = str(tag).strip().lower()
  if t.startswith("q"):
    t = t[1:]
  return t


def _load_matching_workload_query(robust_root, benchmark, sql, query_tag=None):
  workload_name = _workload_name_for_benchmark(benchmark)
  workload_path = Path(robust_root) / "LpBound" / "benchmarks" / "workloads" / benchmark / f"{workload_name}.jsonl"
  if not workload_path.exists():
    return None

  normalized_tag = _normalize_tag(query_tag)
  target = _normalize_sql(sql)
  fallback = None
  with workload_path.open("r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      try:
        obj = json.loads(line)
      except Exception:
        continue
      if normalized_tag is not None and _normalize_tag(obj.get("tag")) == normalized_tag:
        return obj
      if _normalize_sql(obj.get("sql")) == target:
        fallback = obj
  return fallback


def _dump_generated_workload(benchmark, query_obj):
  demo_root = Path(__file__).resolve().parents[1]
  dump_dir = demo_root / "dump" / "xbound-runpy-workloads"
  dump_dir.mkdir(parents=True, exist_ok=True)
  ts = datetime.now().strftime("%Y%m%d-%H%M%S")
  tag = str(query_obj.get("tag", "custom")).replace("/", "_")
  out_path = dump_dir / f"{benchmark}-{tag}-{ts}.jsonl"
  with out_path.open("w", encoding="utf-8") as f:
    f.write(json.dumps(query_obj) + "\n")
  return out_path


def _cleanup_stale_custom_artifacts(root, benchmark):
  workload_dir = Path(root) / "LpBound" / "benchmarks" / "workloads" / benchmark
  est_dir = Path(root) / "LpBound" / "benchmarks" / "est" / benchmark
  patterns = [
    str(workload_dir / f"{benchmark}-custom-*.jsonl"),
    str(est_dir / f"xbound::{benchmark}-custom-*_host=*.jsonl"),
  ]
  for pattern in patterns:
    for p in glob(pattern):
      try:
        Path(p).unlink(missing_ok=True)
      except Exception:
        pass


def estimate_xbound_via_runpy(robust_root, benchmark, sql, parts, l0_theta, hh_theta, query_tag=None):
  root = Path(robust_root)
  _cleanup_stale_custom_artifacts(root, benchmark)
  query_obj = _load_matching_workload_query(robust_root, benchmark, sql, query_tag=query_tag)
  if not query_obj:
    raise RuntimeError("No matching workload query found for run.py mode (need tag/config).")

  query_obj = dict(query_obj)
  # run.py's parser is sensitive to some multiline layouts; pass compact SQL.
  query_obj["sql"] = _compact_sql(sql)
  query_obj.setdefault("tag", "custom_q")
  query_obj.setdefault("config", [])
  dump_path = _dump_generated_workload(benchmark, query_obj)

  workload_dir = root / "LpBound" / "benchmarks" / "workloads" / benchmark
  workload_dir.mkdir(parents=True, exist_ok=True)
  # Keep the generated workload in demo dump, then stage a short-lived copy for run.py.
  stage_token = next(tempfile._get_candidate_names())
  tmp_workload_path = workload_dir / f"{benchmark}-custom-{stage_token}.jsonl"
  shutil.copyfile(dump_path, tmp_workload_path)

  workload_name = tmp_workload_path.name
  run_py = root / "src" / "xbound" / "run.py"
  cmd = [
    sys.executable,
    str(run_py),
    workload_name,
    str(int(parts)),
    str(int(l0_theta)),
    str(int(hh_theta)),
  ]

  generated_est_files = []
  try:
    proc = subprocess.run(
      cmd,
      cwd=str(root / "src" / "xbound"),
      check=False,
      capture_output=True,
      text=True,
      timeout=300,
    )
    trace = (
      f"cmd={cmd}\n"
      f"cwd={root / 'src' / 'xbound'}\n"
      f"returncode={proc.returncode}\n"
      f"staged_workload={tmp_workload_path}\n"
      f"dump={dump_path}\n"
      f"stdout:\n{proc.stdout or '<empty>'}\n"
      f"stderr:\n{proc.stderr or '<empty>'}"
    )
    if proc.returncode != 0:
      combined = f"{proc.stderr or ''}\n{proc.stdout or ''}".lower()
      if "datasketches.duckdb_extension" in combined or "load datasketches" in combined:
        raise RuntimeError(
          "run.py failed because DuckDB extension `datasketches` is missing. "
          "Install/load it in the same Python env used by the demo. "
          "In DuckDB run:\n"
          "  INSTALL datasketches FROM community;\n"
          "  LOAD datasketches;\n"
          f"{trace}"
        )
      raise RuntimeError(
        "run.py failed "
        f"(exit={proc.returncode}). "
        f"trace:\n{trace}"
      )
    out_prefix = tmp_workload_path.stem
    est_dir = root / "LpBound" / "benchmarks" / "est" / benchmark
    pattern = str(est_dir / f"xbound::{out_prefix}_host=*")
    matches = sorted(glob(pattern))
    generated_est_files = [Path(p) for p in matches]
    if not matches:
      raise RuntimeError(f"run.py finished but no xbound output matched {pattern}")
    out_file = Path(matches[-1])
    with out_file.open("r", encoding="utf-8") as f:
      for line in f:
        line = line.strip()
        if not line:
          continue
        obj = json.loads(line)
        val = obj.get("xbound")
        if val is None:
          val = obj.get("meta", {}).get("best", {}).get("val")
        if val is not None:
          return float(val), str(dump_path), (proc.stdout or ""), (proc.stderr or "")
    raise RuntimeError(f"No xbound value found in {out_file}")
  finally:
    try:
      tmp_workload_path.unlink(missing_ok=True)
    except Exception:
      pass
    for p in generated_est_files:
      try:
        p.unlink(missing_ok=True)
      except Exception:
        pass


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--benchmark", required=True)
  parser.add_argument("--sql", required=True)
  parser.add_argument("--query-tag", default="")
  parser.add_argument("--robust-root", required=True)
  parser.add_argument("--parts", type=int, default=16)
  parser.add_argument("--l0-theta", type=int, default=8)
  parser.add_argument("--hh-theta", type=int, default=12)
  args = parser.parse_args()

  benchmark = args.benchmark.strip().lower()
  sql = args.sql.strip().rstrip(";") + ";"
  result = {
    "benchmark": benchmark,
    "sql": sql,
    "actual": None,
    "estimates": {},
    "xbound": {},
    "errors": {},
    "debug": {},
  }

  try:
    result["actual"] = actual_duckdb(args.robust_root, benchmark, sql)
  except Exception as e:
    result["errors"]["actual"] = str(e)

  try:
    duckdb_est = estimate_duckdb(args.robust_root, benchmark, sql)
    if duckdb_est is not None:
      result["estimates"]["duckdb"] = duckdb_est
  except Exception as e:
    result["errors"]["duckdb"] = str(e)

  try:
    pg_est = estimate_postgres(benchmark, sql)
    if pg_est is not None:
      result["estimates"]["postgres"] = pg_est
  except Exception as e:
    result["errors"]["postgres"] = str(e)

  try:
    xbound_est, dump_path, runpy_stdout, runpy_stderr = estimate_xbound_via_runpy(
      args.robust_root,
      benchmark,
      sql,
      args.parts,
      args.l0_theta,
      args.hh_theta,
      query_tag=args.query_tag,
    )
    result["debug"]["xbound_runpy_workload_dump"] = dump_path
    result["debug"]["xbound_runpy_stdout"] = runpy_stdout
    result["debug"]["xbound_runpy_stderr"] = runpy_stderr
    if xbound_est is not None:
      result["xbound"]["duckdb"] = xbound_est
      result["xbound"]["postgres"] = xbound_est
      result["xbound"]["fabric dw"] = xbound_est
  except Exception as e:
    result["errors"]["xbound"] = f"run.py mode failed: {e}"
    try:
      xbound_est = estimate_xbound(args.robust_root, benchmark, sql)
      if xbound_est is not None:
        result["xbound"]["duckdb"] = xbound_est
        result["xbound"]["postgres"] = xbound_est
        result["xbound"]["fabric dw"] = xbound_est
    except Exception as e2:
      result["errors"]["xbound"] = f"{result['errors']['xbound']} | fallback failed: {e2}"

  print(json.dumps(result))


if __name__ == "__main__":
  main()
