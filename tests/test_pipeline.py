import json
from pathlib import Path

from bls_corpus.config import AppConfig
from bls_corpus.pipeline import process_program, run_all

FIX = Path(__file__).parent / "fixtures" / "bd"


def test_offline_process_program_writes_series_files(tmp_path):
    raw = tmp_path / "data" / "bd" / "raw"
    raw.mkdir(parents=True)
    for p in FIX.iterdir():
        (raw / p.name).write_bytes(p.read_bytes())
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.program = "BD"
    result = process_program(cfg, offline=True)
    assert result["status"] == "ok"
    assert result["series"] == 40
    first = tmp_path / "data" / "bd" / "series" / "BDS0000000000000000110001LQ5.json"
    assert first.exists()
    row = json.loads(first.read_text())
    assert row["series_id"].startswith("BD")
    assert "snapshot_id" not in row


def test_run_all_only_offline(tmp_path):
    raw = tmp_path / "data" / "bd" / "raw"
    raw.mkdir(parents=True)
    for p in FIX.iterdir():
        (raw / p.name).write_bytes(p.read_bytes())
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    log = run_all(cfg, only=["BD"], force=True, offline=True, prefixes=["bd"])
    assert log["catalogs"][0]["status"] == "ok"
    run_path = tmp_path / "data" / "run.json"
    assert run_path.exists()
    saved = json.loads(run_path.read_text())
    assert saved["catalogs"][0]["program"] == "BD"
