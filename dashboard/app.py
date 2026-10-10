"""Local web dashboard for the disease surveillance warehouse.

Serves index.html and /api/data (aggregates read live from the PostgreSQL
warehouse on every request), so the page reflects the latest pipeline run.
Standard library HTTP server + psycopg2; runs as the `dashboard` Compose service.
"""

from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg2

INDEX = Path(__file__).resolve().parent / "index.html"

# One round trip. The star schema itself (dims + the aggregated fact, ~50k
# rows of counts, no personal data) so the page can cross-filter any view —
# year, disease, district — and every chart sums the same rows.
DATA_SQL = """
SELECT json_build_object(
  'fact', (
    SELECT json_agg(json_build_array(
      date_key, district_key, disease_key, age_group_key, sex, total_cases))
    FROM mart.fact_disease_cases),
  'dim_date', (
    SELECT json_agg(json_build_object(
      'k', date_key, 'm', to_char(month_start, 'YYYY-MM'), 'label', month_label, 'y', year_be)
      ORDER BY date_key)
    FROM mart.dim_date),
  'dim_district', (
    SELECT json_agg(json_build_object('k', district_key, 'name', district_name) ORDER BY district_key)
    FROM mart.dim_district WHERE is_bangkok_district),
  'dim_disease', (
    SELECT json_agg(json_build_object('k', disease_key, 'name', disease_name) ORDER BY disease_key)
    FROM mart.dim_disease),
  'dim_age', (
    SELECT json_agg(json_build_object('k', age_group_key, 'label', age_group) ORDER BY age_group_key)
    FROM mart.dim_age_group),
  'dim_sex', (
    SELECT json_agg(json_build_object('k', sex, 'label', sex_label) ORDER BY sex)
    FROM mart.dim_sex),
  'population', (
    SELECT json_agg(json_build_object('y', year_be, 'k', district_key, 'p', population))
    FROM mart.fact_population),
  'period', (
    SELECT json_agg(json_build_object('y', year_be, 'months', months_covered, 'label', period_label))
    FROM mart.vw_year_period),
  'dq', (
    SELECT json_agg(json_build_object(
      'layer', layer, 'rule', rule, 'observed', observed,
      'threshold', threshold, 'status', status))
    FROM mart.data_quality),
  'loaded_at', (
    SELECT to_char(max(loaded_at) AT TIME ZONE 'Asia/Bangkok', 'YYYY-MM-DD HH24:MI')
    FROM mart.load_audit)
)
"""


def connection_settings() -> dict:
    return {
        "host": os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        "port": os.getenv("WAREHOUSE_DB_PORT", "5433"),
        "dbname": os.getenv("WAREHOUSE_DB_NAME", "surveillance_dw"),
        "user": os.getenv("WAREHOUSE_DB_USER", "dw_user"),
        "password": os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse-local-only"),
        "connect_timeout": 5,
    }


def fetch_data() -> bytes:
    connection = psycopg2.connect(**connection_settings())
    try:
        with connection.cursor() as cursor:
            cursor.execute(DATA_SQL)
            payload = cursor.fetchone()[0]
    finally:
        connection.close()
    if not payload or not payload.get("fact"):
        raise LookupError("warehouse is empty: run the pipeline (spark_processing DAG) first")
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    server_version = "SurveillanceDashboard/1.0"

    def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (http.server naming)
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._send(HTTPStatus.OK, INDEX.read_bytes(), "text/html; charset=utf-8")
        elif path == "/api/data":
            try:
                self._send(HTTPStatus.OK, fetch_data(), "application/json; charset=utf-8")
            except (psycopg2.Error, LookupError) as error:
                message = json.dumps({"error": str(error).strip()}, ensure_ascii=False)
                self._send(HTTPStatus.SERVICE_UNAVAILABLE, message.encode("utf-8"),
                           "application/json; charset=utf-8")
        elif path == "/health":
            self._send(HTTPStatus.OK, b"ok", "text/plain; charset=utf-8")
        else:
            self._send(HTTPStatus.NOT_FOUND, b"not found", "text/plain; charset=utf-8")

    def log_message(self, format: str, *args) -> None:  # quieter access log
        if not self.path.startswith("/health"):
            super().log_message(format, *args)


def main() -> None:
    port = int(os.getenv("DASHBOARD_PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Dashboard on http://0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
