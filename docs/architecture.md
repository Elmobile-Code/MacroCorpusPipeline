# Architecture

One machine. `python run.py` lists every two-letter survey on `https://download.bls.gov/pub/time.series/`, then downloads and writes each catalog.

```text
python run.py
        │
        ▼
  list LABSTAT root
        │
        ▼
  for each prefix (cu, ce, bd, …)
        │
        ▼
  download ──▶ data/<prefix>/raw/
        │
        ▼
  schema.py reads headers and joins names
        │
        ▼
  engine.py (DuckDB)
        │
        ▼
  data/<prefix>/series/<series_id>.json
  data/<prefix>/catalog.json
  data/<prefix>/index.json
  data/run.json
```

## Modules

| Module | Job |
|---|---|
| `run.py` / `pipeline.py` | List every catalog, download, process, write, record `data/run.json` |
| `config.py` | Reads `config/default.toml`. Paths are `data/<prefix>/raw` and `data/<prefix>/series`. |
| `discovery.py` | Root listing and per-catalog file roles |
| `downloader.py` | Streaming download, retries, checksums |
| `schema.py` | Header-based layout and join rules |
| `engine.py` | Load, dedupe, write one JSON file per `series_id` |

`scripts/sample_catalog.py` is a metadata-only peek. It is not part of `run.py`.

A complete data file (`AllItems`, `AllData`, `AllCESSeries`) outranks a `Current` file. Codes stay text. A name table is joined when its code and label name the same thing.
