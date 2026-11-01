"""One-command catalog run: list every LABSTAT prefix, then download and write."""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from . import __version__, engine as engine_mod
from .config import AppConfig, load_config
from .discovery import DiscoveryError, discover_remote, fetch_root_listing
from .downloader import DownloadError, Downloader, sha256_file
from .manifest import Manifest, new_manifest


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _for_program(cfg: AppConfig, program: str) -> AppConfig:
    return replace(cfg, program=program.upper(), data_dir=Path(cfg.data_dir))


def _file_receipt(raw_dir: Path, manifest: Manifest) -> list[dict]:
    rows = []
    for rec in manifest.files:
        path = raw_dir / rec.relative_path
        sha = rec.sha256
        if path.exists() and not sha:
            sha = sha256_file(path)
        rows.append({
            "name": rec.relative_path,
            "sha256": sha,
            "size": rec.local_size or (path.stat().st_size if path.exists() else None),
            "role": rec.file_role,
            "download_status": rec.download_status,
        })
    return rows


def _unchanged(catalog_json: Path, listing_sha256: str) -> bool:
    if not catalog_json.exists():
        return False
    try:
        card = json.loads(catalog_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    return bool(card.get("listing_sha256") and card["listing_sha256"] == listing_sha256)


def process_program(cfg: AppConfig, force: bool = False,
                    offline: bool = False) -> dict:
    program = cfg.program.upper()
    raw_dir = cfg.raw_root
    out_dir = cfg.catalog_dir
    started = time.perf_counter()
    result = {
        "program": program,
        "status": "pending",
        "series": 0,
        "observations": 0,
        "error": None,
        "elapsed_s": 0.0,
    }
    try:
        if offline:
            if not raw_dir.exists():
                raise FileNotFoundError(f"no local raw files at {raw_dir}")
            listing_sha = None
            extra = {"listing_sha256": None, "files": []}
        else:
            disc = discover_remote(cfg.program, cfg.program_url, cfg.user_agent,
                                   timeout_s=cfg.read_timeout_s)
            listing_sha = disc.listing_sha256
            if not force and _unchanged(out_dir / "catalog.json", listing_sha):
                card = json.loads((out_dir / "catalog.json").read_text(encoding="utf-8"))
                cov = card.get("coverage") or {}
                result.update({
                    "status": "skipped",
                    "series": cov.get("series", 0),
                    "observations": cov.get("observations", 0),
                    "elapsed_s": round(time.perf_counter() - started, 2),
                })
                return result
            manifest_path = out_dir / "download.json"
            prior = Manifest.load(manifest_path) if manifest_path.exists() else None
            manifest = new_manifest(
                disc, program.lower(),
                config_summary={"concurrency": cfg.concurrency, "version": __version__},
            )
            Downloader(cfg).download_all(manifest, raw_dir, prior=prior)
            manifest.save(manifest_path)
            extra = {
                "listing_sha256": listing_sha,
                "files": _file_receipt(raw_dir, manifest),
            }
        stats, _schema = engine_mod.process_catalog(raw_dir, out_dir, program, extra=extra)
        result.update({
            "status": "ok",
            "series": stats.series,
            "observations": stats.observations,
            "duplicate_identical": stats.duplicate_identical,
            "duplicate_conflicts": stats.duplicate_conflicts,
            "elapsed_s": round(time.perf_counter() - started, 2),
        })
    except (DiscoveryError, DownloadError, RuntimeError, FileNotFoundError, OSError) as exc:
        result.update({
            "status": "failed",
            "error": str(exc),
            "elapsed_s": round(time.perf_counter() - started, 2),
        })
    return result


def run_all(cfg: AppConfig, only: list[str] | None = None, force: bool = False,
            offline: bool = False, prefixes: list[str] | None = None) -> dict:
    if prefixes is None:
        if only:
            prefixes = [p.lower() for p in only]
        else:
            _html, prefixes = fetch_root_listing(
                cfg.source_root, cfg.user_agent, timeout_s=cfg.read_timeout_s)
    log = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "prefixes": prefixes,
        "catalogs": [],
    }
    for prefix in prefixes:
        print(f"[{prefix}] start", flush=True)
        one = process_program(_for_program(cfg, prefix), force=force, offline=offline)
        log["catalogs"].append(one)
        print(f"[{prefix}] {one['status']} series={one['series']} "
              f"obs={one['observations']} {one.get('error') or ''}", flush=True)
        _write_json(Path(cfg.data_dir) / "run.json", log)
    log["finished_utc"] = datetime.now(timezone.utc).isoformat()
    failed = sum(1 for c in log["catalogs"] if c["status"] == "failed")
    log["failed"] = failed
    _write_json(Path(cfg.data_dir) / "run.json", log)
    return log


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download every BLS LABSTAT catalog and write one JSON file per series.")
    p.add_argument("--only", default=None,
                   help="Comma-separated prefixes for tests (WS,CU). Default: all catalogs.")
    p.add_argument("--force", action="store_true",
                   help="Rebuild a catalog even if its listing has not changed.")
    p.add_argument("--offline", action="store_true",
                   help="Process local raw files only. Requires --only.")
    p.add_argument("--data-dir", default=None)
    p.add_argument("--source-root", default=None)
    p.add_argument("--config", default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    if args.data_dir:
        cfg.data_dir = Path(args.data_dir)
    if args.source_root:
        cfg.source_root = args.source_root
    only = [p.strip().upper() for p in args.only.split(",") if p.strip()] if args.only else None
    if args.offline and not only:
        print("--offline requires --only", file=sys.stderr)
        return 2
    log = run_all(cfg, only=only, force=args.force, offline=args.offline)
    print(json.dumps({k: log[k] for k in ("prefixes", "failed", "finished_utc") if k in log},
                     indent=2))
    return 1 if log.get("failed") else 0


__all__ = ["main", "run_all", "process_program", "list_catalog_prefixes"]
