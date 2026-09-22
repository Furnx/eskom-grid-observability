"""Configuration loading — the one place where the environment meets the code.

``eskom_grid.extract`` and ``eskom_grid.sinks`` never read environment
variables or the filesystem for configuration; callers resolve configuration
here and pass plain values in.
"""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path

import yaml

# Override the packaged portfolio with a file of your own (tests, ad-hoc runs).
AREAS_CONFIG_ENV = "ESKOM_AREAS_CONFIG"

REQUIRED_AREA_KEYS = ("area_id", "area_name", "municipality", "province")


def load_areas(path: str | Path | None = None) -> list[dict]:
    """Return the monitored-area portfolio as a list of dicts.

    Resolution order:
      1. ``path`` argument, if given.
      2. ``ESKOM_AREAS_CONFIG`` environment variable, if set.
      3. ``areas_config.yml`` shipped inside the package (the default).

    The packaged default is what makes the installed package self-contained:
    a Lambda that ``pip install``s this project needs no extra file.

    Raises:
        ValueError: if the file has no ``areas`` list, or an entry is missing
            one of the required keys.
    """
    if path is None:
        path = os.getenv(AREAS_CONFIG_ENV)

    if path is not None:
        text = Path(path).read_text(encoding="utf-8")
        source = str(path)
    else:
        text = (
            resources.files("eskom_grid")
            .joinpath("areas_config.yml")
            .read_text(encoding="utf-8")
        )
        source = "eskom_grid/areas_config.yml (packaged)"

    config = yaml.safe_load(text) or {}
    areas = config.get("areas")
    if not isinstance(areas, list) or not areas:
        raise ValueError(f"{source}: expected a non-empty 'areas' list.")

    for index, area in enumerate(areas):
        missing = [key for key in REQUIRED_AREA_KEYS if not area.get(key)]
        if missing:
            raise ValueError(
                f"{source}: areas[{index}] is missing required key(s): {missing}"
            )

    return areas
