# Architecture

`python run.py` lists every two-letter survey on `https://download.bls.gov/pub/time.series/`, then downloads and writes each catalog. All of that lives in `src/bls_corpus/pipeline.py`.

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
  join code labels, load with DuckDB
        │
        ▼
  data/<prefix>/series/<series_id>.json
  data/<prefix>/catalog.json
  data/<prefix>/index.json
  data/run.json
```

A complete data file (`AllItems`, `AllData`, `AllCESSeries`) outranks a `Current` file. Codes stay text. A name table is joined when its code and label name the same thing.
