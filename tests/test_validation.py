"""Synthetic end-to-end (offline): raw fixtures -> parse -> validate -> rerun."""
import json
from pathlib import Path

from bls_corpus import cli
from bls_corpus.config import AppConfig
from bls_corpus.discovery import parse_listing
from bls_corpus.manifest import Manifest, new_manifest

FIX = Path(__file__).parent / "fixtures" / "bd"


def _manifest_for_fixtures(cfg: AppConfig) -> Manifest:
    names = sorted(p.name for p in FIX.iterdir())
    lines = "".join(
        f' 7/29/2026 10:00 AM  {1000 + i} <A HREF="/pub/time.series/bd/{n}">{n}</A><br>\n'
        for i, n in enumerate(names))
    disc = parse_listing(f"<pre>{lines}</pre>", "BD", "https://fixture/bd/")
    manifest = new_manifest(disc, "bd", {})
    raw = cfg.raw_root
    raw.mkdir(parents=True, exist_ok=True)
    for p in FIX.iterdir():
        (raw / p.name).write_bytes(p.read_bytes())
    for rec in manifest.files:
        rec.download_status = "downloaded"
        rec.local_size = (raw / rec.relative_path).stat().st_size
    return manifest


def _run_parse_validate(cfg: AppConfig, manifest: Manifest):
    mpath = cfg.catalog_dir / "download.json"
    manifest.save(mpath)
    class A:
        program = "BD"; data_dir = None; concurrency = None; source_root = None
        snapshot = "bd"; offline = True; sample_size = 5
    assert cli.cmd_parse(cfg, A()) == 0
    return cli.cmd_validate(cfg, A())


def test_end_to_end_synthetic_and_idempotent_rerun(tmp_path):
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.program = "BD"
    cfg.min_interval_s = 0
    manifest = _manifest_for_fixtures(cfg)
    out = cfg.catalog_dir

    assert _run_parse_validate(cfg, manifest) == 0
    index = json.loads((out / "index.json").read_text())
    assert len(index) == 40
    assert sum(v["observation_count"] for v in index.values()) > 0
    report = json.loads((out / "validation-bd.json").read_text())
    assert report["series_in_catalog"] == 40
    assert report["files_failed"] == 0
    first_count = sum(v["observation_count"] for v in index.values())

    class A:
        program = "BD"; data_dir = None; concurrency = None; source_root = None
        snapshot = "bd"; offline = True; sample_size = 5
    assert cli.cmd_parse(cfg, A()) == 0
    again = json.loads((out / "index.json").read_text())
    assert sum(v["observation_count"] for v in again.values()) == first_count


def test_missing_metadata_file_reported_not_fatal(tmp_path):
    cfg = AppConfig()
    cfg.data_dir = tmp_path / "data"
    cfg.program = "BD"
    manifest = _manifest_for_fixtures(cfg)
    raw = cfg.raw_root
    (raw / "bd.industry").unlink()
    for rec in manifest.files:
        if rec.relative_path == "bd.industry":
            rec.download_status = "failed"
    mpath = cfg.catalog_dir / "download.json"
    manifest.save(mpath)
    class A:
        program = "BD"; data_dir = None; concurrency = None; source_root = None
        snapshot = "bd"; offline = True; sample_size = 5
    assert cli.cmd_parse(cfg, A()) == 0
    rec = manifest.record_for("bd.industry")
    assert rec is not None
