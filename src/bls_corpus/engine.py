"""Generic LABSTAT processor.

Reads raw TSV files with DuckDB and writes one JSON file per series_id.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from .schema import ProgramSchema, infer_schema, required_missing


@dataclass
class ProcessResult:
    snapshot_id: str
    program: str
    series: int = 0
    observations: int = 0
    rows_in: int = 0
    duplicate_identical: int = 0
    duplicate_conflicts: int = 0
    malformed_rows: int = 0
    missing_values: int = 0
    non_numeric_values: int = 0
    unknown_periods: int = 0
    elapsed_s: float = 0.0
    extras: dict = field(default_factory=dict)

    def parse_stats(self) -> dict:
        return {
            "rows_in": self.rows_in, "rows_written": self.observations,
            "malformed_rows": self.malformed_rows, "malformed_samples": [],
            "non_numeric_values": self.non_numeric_values,
            "missing_values": self.missing_values,
            "unknown_periods": self.unknown_periods,
            "duplicate_identical": self.duplicate_identical,
            "duplicate_conflicts": self.duplicate_conflicts,
            "duplicate_conflict_samples": [], "unknown_period_samples": [],
        }


def _ident(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    if not cleaned or cleaned[0].isdigit():
        cleaned = "c_" + cleaned
    return cleaned


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def _lines_sql(path: Path, encoding: str) -> str:
    return (
        f"read_csv({_sql_str(str(path))}, columns={{'line':'VARCHAR'}}, "
        f"delim='\\x1f', quote='', escape='', header=false, auto_detect=false, "
        f"encoding={_sql_str(encoding)}, store_rejects=true, ignore_errors=true) "
        f"WITH ORDINALITY AS t(line, rec)"
    )


def _split_sql(path: Path, encoding: str, skip_header: bool = True) -> str:
    where = "rec > 1 AND " if skip_header else ""
    return (
        f"SELECT rec, string_split(line, chr(9)) AS f FROM {_lines_sql(path, encoding)} "
        f"WHERE {where}line IS NOT NULL AND trim(line) <> ''"
    )


class Engine:
    def __init__(self, raw_dir: Path, out_dir: Path, program: str,
                 snapshot_id: str | None = None):
        self.raw_dir = Path(raw_dir)
        self.out_dir = Path(out_dir)
        self.program = program.upper()
        self.snapshot_id = snapshot_id or self.program.lower()
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.out_dir / "work.duckdb"
        if self.db_path.exists():
            self.db_path.unlink()
        self.con = duckdb.connect(str(self.db_path))
        self.con.execute("SET preserve_insertion_order=true")
        spill = self.out_dir / "spill"
        spill.mkdir(exist_ok=True)
        self.con.execute(f"SET temp_directory={_sql_str(str(spill))}")
        self.con.execute("""
            CREATE MACRO period_start(y, p) AS (
              CASE
                WHEN p IS NULL OR y IS NULL THEN NULL
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$')
                  THEN make_date(y, CAST(substr(p, 2, 2) AS INTEGER), 1)
                WHEN p IN ('M13', 'Q05', 'S03', 'A01', 'ANN', 'A') THEN make_date(y, 1, 1)
                WHEN regexp_matches(p, '^Q0[1-4]$')
                  THEN make_date(y, (CAST(substr(p, 3, 1) AS INTEGER) - 1) * 3 + 1, 1)
                WHEN p = 'S01' THEN make_date(y, 1, 1)
                WHEN p = 'S02' THEN make_date(y, 7, 1)
                ELSE NULL
              END
            );
            CREATE MACRO period_finish(y, p) AS (
              CASE
                WHEN p IS NULL OR y IS NULL THEN NULL
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$')
                  THEN last_day(make_date(y, CAST(substr(p, 2, 2) AS INTEGER), 1))
                WHEN p IN ('M13', 'Q05', 'S03', 'A01', 'ANN', 'A') THEN make_date(y, 12, 31)
                WHEN regexp_matches(p, '^Q0[1-4]$')
                  THEN last_day(make_date(y, CAST(substr(p, 3, 1) AS INTEGER) * 3, 1))
                WHEN p = 'S01' THEN make_date(y, 6, 30)
                WHEN p = 'S02' THEN make_date(y, 12, 31)
                ELSE NULL
              END
            );
            CREATE MACRO period_kind(p) AS (
              CASE
                WHEN p IS NULL THEN 'unknown_period'
                WHEN regexp_matches(p, '^M(0[1-9]|1[0-2])$') THEN 'monthly'
                WHEN p IN ('M13', 'Q05', 'S03') THEN 'annual_average'
                WHEN regexp_matches(p, '^Q0[1-4]$') THEN 'quarterly'
                WHEN p IN ('S01', 'S02') THEN 'semiannual'
                WHEN p IN ('A01', 'ANN', 'A') THEN 'annual'
                ELSE 'unknown_period'
              END
            );
        """)

    def close(self) -> None:
        self.con.close()

    def process(self, schema: ProgramSchema | None = None) -> ProcessResult:
        import time

        started = time.perf_counter()
        schema = schema or infer_schema(self.raw_dir, self.program)
        missing = required_missing(schema)
        if missing:
            raise RuntimeError(f"{self.program}: missing required pieces: {', '.join(missing)}")
        self._load_catalog(schema)
        self._load_mappings(schema)
        self._load_footnotes(schema)
        self._load_observations(schema)
        self._enrich_catalog(schema)
        stats = self._stats()
        self.stats = stats
        stats.elapsed_s = time.perf_counter() - started
        return stats

    def export_series(self, schema: ProgramSchema,
                      extra: dict | None = None) -> tuple[Path, Path]:
        extra = extra or {}
        series_dir = self.out_dir / "series"
        if series_dir.exists():
            for old in series_dir.glob("*.json"):
                old.unlink()
        series_dir.mkdir(parents=True, exist_ok=True)

        label_selects = []
        used = {_ident(c) for c in schema.catalog_columns}
        for m in schema.mappings:
            if not m.links_to or not m.label_col:
                continue
            alias = _ident(m.label_col)
            if alias in used:
                continue
            used.add(alias)
            code_col = _ident(m.links_to[0])
            label_selects.append(
                f"(SELECT label FROM mappings WHERE table_name = {_sql_str(m.table)} "
                f"AND code = c.{code_col} LIMIT 1) AS {alias}"
            )
        catalog_selects = [f"c.{_ident(c)} AS {_ident(c)}" for c in schema.catalog_columns]
        select_list = ", ".join(catalog_selects + label_selects) or "c.series_id"
        self.con.execute("CREATE OR REPLACE TABLE catalog_flat AS "
                         f"SELECT {select_list} FROM catalog_raw c "
                         f"WHERE c.{_ident('series_id')} IS NOT NULL "
                         f"AND c.{_ident('series_id')} <> ''")
        self.con.execute("""
            CREATE OR REPLACE TABLE obs_grouped AS
            SELECT series_id,
                   to_json(list(json_object(
                       'year', CAST(year AS VARCHAR),
                       'period', period,
                       'value', value_raw,
                       'footnote_codes', coalesce(footnote_codes, '')
                   ) ORDER BY year, period)) AS observations
            FROM observations
            GROUP BY 1
        """)
        tmp = self.out_dir / "_series.jsonl"
        self.con.execute(f"""
            COPY (
              SELECT f.*, coalesce(o.observations, '[]'::JSON) AS observations
              FROM catalog_flat f
              LEFT JOIN obs_grouped o ON o.series_id = f.series_id
            ) TO {_sql_str(str(tmp))} (FORMAT JSON)
        """)
        index: dict[str, dict] = {}
        with tmp.open(encoding="utf-8") as handle:
            for line in handle:
                rec = json.loads(line)
                if rec.get("observations") is None:
                    rec["observations"] = []
                sid = rec.get("series_id") or ""
                if not sid:
                    continue
                dest = series_dir / f"{sid}.json"
                dest.write_text(json.dumps(rec, indent=2), encoding="utf-8")
                title = rec.get("series_title") or rec.get("series_name") or ""
                index[sid] = {
                    "path": f"series/{sid}.json",
                    "series_title": title,
                    "observation_count": len(rec["observations"]),
                }
        tmp.unlink(missing_ok=True)

        joins = self._join_stats(schema)
        coverage = self.con.execute("""
            SELECT (SELECT count(*) FROM catalog),
                   (SELECT count(*) FROM observations),
                   (SELECT count(*) FROM catalog WHERE observation_count = 0),
                   (SELECT min(year) FROM observations),
                   (SELECT max(year) FROM observations)
        """).fetchone()
        card = {
            "program": self.program,
            "listing_sha256": extra.get("listing_sha256"),
            "files": extra.get("files", []),
            "catalog_fields": schema.catalog_columns,
            "joins": joins,
            "coverage": {
                "series": coverage[0],
                "observations": coverage[1],
                "catalog_without_observations": coverage[2],
                "year_min": coverage[3],
                "year_max": coverage[4],
            },
            "validation": self.stats.parse_stats() if getattr(self, "stats", None) else {},
        }
        card_path = self.out_dir / "catalog.json"
        index_path = self.out_dir / "index.json"
        _write_json(card_path, card)
        _write_json(index_path, index)
        return card_path, index_path

    def _join_stats(self, schema: ProgramSchema) -> list[dict]:
        stats = []
        for m in schema.mappings:
            if not m.links_to or not m.label_col:
                continue
            col = _ident(m.links_to[0])
            row = self.con.execute(f"""
                SELECT
                  count(*) FILTER (WHERE c.{col} IS NOT NULL AND c.{col} <> ''
                    AND m.label IS NOT NULL) AS matched,
                  count(*) FILTER (WHERE c.{col} IS NULL OR c.{col} = '') AS blank,
                  count(*) FILTER (WHERE c.{col} IS NOT NULL AND c.{col} <> ''
                    AND m.label IS NULL) AS unmatched
                FROM catalog_raw c
                LEFT JOIN mappings m
                  ON m.table_name = {_sql_str(m.table)} AND m.code = c.{col}
            """).fetchone()
            stats.append({
                "file": m.path,
                "catalog_column": m.links_to[0],
                "label_column": m.label_col,
                "matched": int(row[0] or 0),
                "blank": int(row[1] or 0),
                "unmatched": int(row[2] or 0),
            })
        return stats

    def export_json(self, schema: ProgramSchema) -> tuple[Path, Path]:
        return self.export_series(schema)

    # -- loaders -----------------------------------------------------------
    def _load_catalog(self, schema: ProgramSchema) -> None:
        path = self.raw_dir / schema.catalog_path
        n = len(schema.catalog_columns)
        selects = ", ".join(
            f"trim(f[{i}]) AS {_ident(col)}"
            for i, col in enumerate(schema.catalog_columns, start=1)
        )
        self.con.execute(f"""
            CREATE TABLE catalog_raw AS
            SELECT rec, {selects}, len(f) AS n_fields,
                   len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) AS extra
            FROM ({_split_sql(path, schema.encoding)})
        """)

    def _load_mappings(self, schema: ProgramSchema) -> None:
        self.con.execute(
            "CREATE TABLE mappings (table_name VARCHAR, code VARCHAR, label VARCHAR, source_file VARCHAR)")
        for m in schema.mappings:
            path = self.raw_dir / m.path
            code_i = m.columns.index(m.code_col) + 1
            label_expr = (f"trim(f[{m.columns.index(m.label_col)+1}])"
                          if m.label_col in m.columns else "NULL")
            self.con.execute(f"""
                INSERT INTO mappings
                SELECT {_sql_str(m.table)}, trim(f[{code_i}]), {label_expr}, {_sql_str(m.path)}
                FROM ({_split_sql(path, schema.encoding)})
                WHERE len(f) >= 1
            """)

    def _load_footnotes(self, schema: ProgramSchema) -> None:
        self.con.execute(
            "CREATE TABLE footnotes (footnote_code VARCHAR, footnote_text VARCHAR, source_file VARCHAR)")
        if not schema.footnote_path:
            return
        path = self.raw_dir / schema.footnote_path
        from .schema import read_header
        header, _ = read_header(path)
        code_i = 1
        text_i = 2 if len(header) > 1 else 1
        self.con.execute(f"""
            INSERT INTO footnotes
            SELECT trim(f[{code_i}]), trim(f[{text_i}]), {_sql_str(schema.footnote_path)}
            FROM ({_split_sql(path, schema.encoding)})
        """)

    def _load_observations(self, schema: ProgramSchema) -> None:
        cols = {c.lower(): i + 1 for i, c in enumerate(schema.data_columns)}
        sid, year, period = cols["series_id"], cols["year"], cols["period"]
        value, n = cols["value"], len(schema.data_columns)
        fn = cols.get("footnote_codes")
        fn_expr = f"nullif(trim(f[{fn}]), '')" if fn else "NULL"
        parts = []
        for item in schema.data_files:
            path = self.raw_dir / item["path"]
            parts.append(f"""
                SELECT trim(f[{sid}]) AS series_id,
                       TRY_CAST(trim(f[{year}]) AS INTEGER) AS year,
                       trim(f[{period}]) AS period,
                       trim(f[{value}]) AS value_raw,
                       {fn_expr} AS footnote_codes,
                       {int(item['rank'])} AS file_rank,
                       {_sql_str(item['path'])} AS source_file,
                       rec AS source_row,
                       len(f) AS n_fields,
                       len(list_filter(f[{n}+1:], lambda x: trim(x) <> '')) AS extra
                FROM ({_split_sql(path, schema.encoding)})
            """)
        self.con.execute("CREATE TABLE obs_raw AS " + " UNION ALL ".join(parts))
        self.con.execute("""
            CREATE TABLE observations AS
            SELECT series_id, year, period,
                   CAST(period_start(year, period) AS VARCHAR) AS period_start,
                   CAST(period_finish(year, period) AS VARCHAR) AS period_end,
                   period_kind(period) AS period_kind,
                   value_raw,
                   TRY_CAST(replace(value_raw, ',', '') AS DOUBLE) AS value_numeric,
                   CASE
                     WHEN value_raw IS NULL OR trim(value_raw) IN ('', '-', '--', '.', 'NA', 'N/A', '(NA)', 'null')
                       THEN 'missing_token'
                     WHEN TRY_CAST(replace(value_raw, ',', '') AS DOUBLE) IS NULL THEN 'non_numeric'
                     ELSE 'numeric'
                   END AS value_status,
                   footnote_codes, source_file, source_row
            FROM obs_raw
            WHERE year IS NOT NULL AND series_id <> '' AND period <> ''
              AND n_fields >= 4 AND extra = 0
            QUALIFY row_number() OVER (
                PARTITION BY series_id, year, period
                ORDER BY file_rank, source_file, source_row
            ) = 1
        """)

    def _enrich_catalog(self, schema: ProgramSchema) -> None:
        sid = _ident("series_id")
        title = _ident(schema.title_column) if schema.title_column else "NULL"
        title_sql = title if schema.title_column else "NULL"
        seasonal = _ident(schema.seasonal_column) if schema.seasonal_column else None
        periodicity = _ident(schema.periodicity_column) if schema.periodicity_column else None
        seasonal_sql = f"nullif(c.{seasonal}, '')" if seasonal else "NULL"
        periodicity_sql = f"nullif(c.{periodicity}, '')" if periodicity else "NULL"
        skip = {"series_id", schema.title_column, "begin_year", "begin_period",
                "end_year", "end_period", "footnote_codes"}
        meta_cols = [c for c in schema.catalog_columns if c not in skip]
        if meta_cols:
            pairs = ", ".join(f"{_sql_str(c)}, c.{_ident(c)}" for c in meta_cols)
            meta_sql = f"json_object({pairs})"
        else:
            meta_sql = "'{}'"
        self.con.execute(f"""
            CREATE TABLE series_stats AS
            SELECT series_id,
                   min(year || '-' || period) AS first_period,
                   max(year || '-' || period) AS last_period,
                   count(*)::BIGINT AS observation_count
            FROM observations GROUP BY 1
        """)
        self.con.execute(f"""
            CREATE TABLE catalog AS
            SELECT {_sql_str(self.snapshot_id)} AS snapshot_id,
                   {_sql_str(self.program)} AS program_id,
                   c.{sid} AS series_id,
                   c.{sid} AS series_id_raw,
                   {title_sql if schema.title_column else 'NULL'} AS series_title,
                   {seasonal_sql} AS seasonal_code,
                   {periodicity_sql} AS periodicity_code,
                   s.first_period, s.last_period,
                   coalesce(s.observation_count, 0) AS observation_count,
                   {_sql_str(schema.catalog_path)} AS source_catalog_file,
                   {meta_sql} AS metadata_json
            FROM catalog_raw c
            LEFT JOIN series_stats s ON s.series_id = c.{sid}
            WHERE c.{sid} IS NOT NULL AND c.{sid} <> ''
              AND c.n_fields >= {len(schema.catalog_columns)} AND c.extra = 0
        """)

    def _stats(self) -> ProcessResult:
        rows_in = self.con.execute("SELECT count(*) FROM obs_raw").fetchone()[0]
        kept = self.con.execute("SELECT count(*) FROM observations").fetchone()[0]
        malformed = self.con.execute(
            "SELECT count(*) FROM obs_raw WHERE year IS NULL OR series_id = '' OR period = '' "
            "OR n_fields < 4 OR extra > 0").fetchone()[0]
        keys = self.con.execute("""
            SELECT count(*), sum(n) - count(*),
                   count(*) FILTER (WHERE nd > 1)
            FROM (
              SELECT series_id, year, period, count(*) n,
                     count(DISTINCT coalesce(value_raw,'') || '|' || coalesce(footnote_codes,'')) nd
              FROM obs_raw
              WHERE year IS NOT NULL AND series_id <> '' AND period <> ''
                AND n_fields >= 4 AND extra = 0
              GROUP BY 1,2,3
            )
        """).fetchone()
        missing, nonnum, unknown = self.con.execute("""
            SELECT count(*) FILTER (WHERE value_status = 'missing_token'),
                   count(*) FILTER (WHERE value_status = 'non_numeric'),
                   count(*) FILTER (WHERE period_kind IN ('unknown_period', 'invalid_date'))
            FROM observations
        """).fetchone()
        series = self.con.execute("SELECT count(*) FROM catalog").fetchone()[0]
        return ProcessResult(
            snapshot_id=self.snapshot_id, program=self.program,
            series=series, observations=kept, rows_in=rows_in,
            duplicate_identical=int(keys[1] or 0),
            duplicate_conflicts=int(keys[2] or 0),
            malformed_rows=malformed, missing_values=missing,
            non_numeric_values=nonnum,             unknown_periods=unknown,
        )


def process_catalog(raw_dir: Path, out_dir: Path, program: str,
                    extra: dict | None = None) -> tuple[ProcessResult, ProgramSchema]:
    schema = infer_schema(raw_dir, program)
    engine = Engine(raw_dir, out_dir, program)
    try:
        stats = engine.process(schema)
        engine.export_series(schema, extra=extra)
        return stats, schema
    finally:
        engine.close()
        engine.db_path.unlink(missing_ok=True)
        spill = Path(out_dir) / "spill"
        if spill.exists():
            for p in spill.iterdir():
                p.unlink()
            spill.rmdir()


def process_snapshot(raw_dir: Path, out_dir: Path, snapshot_id: str,
                     program: str) -> tuple[ProcessResult, ProgramSchema]:
    return process_catalog(raw_dir, out_dir, program,
                           extra={"snapshot_id": snapshot_id})
