from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class AppConfig:
    program: str = ""
    source_root: str = "https://download.bls.gov/pub/time.series"
    data_dir: Path = Path("data")
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; BLS-Corpus-Research/1.0; "
        "+https://www.bls.gov/data/) bls-corpus/0.1"
    )
    concurrency: int = 2
    min_interval_s: float = 0.35
    connect_timeout_s: float = 15.0
    read_timeout_s: float = 120.0
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0

    @property
    def program_dir(self) -> str:
        return self.program.lower()

    @property
    def program_url(self) -> str:
        return f"{self.source_root.rstrip('/')}/{self.program_dir}/"

    @property
    def catalog_dir(self) -> Path:
        return self.data_dir / self.program_dir

    @property
    def raw_root(self) -> Path:
        return self.catalog_dir / "raw"

    @property
    def series_root(self) -> Path:
        return self.catalog_dir / "series"


def load_config(path: str | Path | None = None) -> AppConfig:
    return AppConfig()
