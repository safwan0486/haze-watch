"""Settings and city configuration.

Paths are resolved from environment variables so the same code runs on a laptop,
in Docker, and inside Airflow without changes:

* ``HAZEWATCH_HOME``     project root holding ``config/`` and ``sql/`` (default: cwd)
* ``HAZEWATCH_DATA_DIR`` where the data lake and warehouse live (default: $HOME/data)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class City:
    id: str
    name: str
    country: str
    latitude: float
    longitude: float
    utc_offset_hours: int


@dataclass(frozen=True)
class Settings:
    home: Path
    data_dir: Path

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "hazewatch.duckdb"

    @property
    def sql_dir(self) -> Path:
        return self.home / "sql"

    @property
    def cities_file(self) -> Path:
        return self.home / "config" / "cities.yaml"


def get_settings() -> Settings:
    home = Path(os.environ.get("HAZEWATCH_HOME", Path.cwd())).resolve()
    data_dir = Path(os.environ.get("HAZEWATCH_DATA_DIR", home / "data")).resolve()
    return Settings(home=home, data_dir=data_dir)


def load_cities(path: Path) -> list[City]:
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    cities = [City(**entry) for entry in raw["cities"]]
    ids = [c.id for c in cities]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate city ids in {path}")
    return cities
