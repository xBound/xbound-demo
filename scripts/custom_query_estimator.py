#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path


DB_PATH_MAP = {
  "joblight": "imdb::joblight.duckdb",
  "so_full_ceb": "so_full::so_full_ceb.duckdb",
  "stats_ceb": "stats::stats_ceb.duckdb",
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


def estimate_duckdb(robust_root, benchmark, sql):
  import duckdb
  db_file = DB_PATH_MAP[benchmark]
  db_path = Path(robust_root) / "dbs" / db_file
  con = duckdb.connect(str(db_path), read_only=True)
  row = con.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
  plan = json.loads(row[1])
  est = _find_duckdb_est(plan)
  con.close()
  return est


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


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--benchmark", required=True)
  parser.add_argument("--sql", required=True)
  parser.add_argument("--robust-root", required=True)
  args = parser.parse_args()

  benchmark = args.benchmark.strip().lower()
  sql = args.sql.strip().rstrip(";") + ";"
  result = {
    "benchmark": benchmark,
    "sql": sql,
    "actual": 1,
    "estimates": {},
    "xbound": {},
    "errors": {},
  }

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
    xbound_est = estimate_xbound(args.robust_root, benchmark, sql)
    if xbound_est is not None:
      result["xbound"]["duckdb"] = xbound_est
      result["xbound"]["postgres"] = xbound_est
      result["xbound"]["fabric dw"] = xbound_est
  except Exception as e:
    result["errors"]["xbound"] = str(e)

  print(json.dumps(result))


if __name__ == "__main__":
  main()
