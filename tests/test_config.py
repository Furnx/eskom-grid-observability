"""Tests for portfolio loading, and a guard that the two copies of the portfolio agree."""

import csv
from pathlib import Path

import pytest

from eskom_grid.config import AREAS_CONFIG_ENV, REQUIRED_AREA_KEYS, load_areas

REPO_ROOT = Path(__file__).resolve().parents[1]
SEED_CSV = REPO_ROOT / "dbt_project" / "seeds" / "areas_config.csv"


def test_packaged_portfolio_loads_with_required_keys():
    areas = load_areas()
    assert len(areas) >= 1
    for area in areas:
        for key in REQUIRED_AREA_KEYS:
            assert area[key], f"{area.get('area_id')} missing {key}"


def test_explicit_path_overrides_packaged_default(tmp_path):
    custom = tmp_path / "areas.yml"
    custom.write_text(
        "areas:\n"
        "  - area_id: za_test\n"
        "    area_name: Test\n"
        "    municipality: Test Muni\n"
        "    province: Test Prov\n",
        encoding="utf-8",
    )
    areas = load_areas(custom)
    assert [a["area_id"] for a in areas] == ["za_test"]


def test_env_var_overrides_packaged_default(tmp_path, monkeypatch):
    custom = tmp_path / "areas.yml"
    custom.write_text(
        "areas:\n  - {area_id: za_env, area_name: E, municipality: M, province: P}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(AREAS_CONFIG_ENV, str(custom))
    assert [a["area_id"] for a in load_areas()] == ["za_env"]


def test_missing_required_key_is_reported_clearly(tmp_path):
    bad = tmp_path / "areas.yml"
    bad.write_text("areas:\n  - {area_id: za_bad, area_name: B}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="municipality"):
        load_areas(bad)


def test_empty_portfolio_is_rejected(tmp_path):
    empty = tmp_path / "areas.yml"
    empty.write_text("areas: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match="non-empty"):
        load_areas(empty)


@pytest.mark.skipif(not SEED_CSV.exists(), reason="dbt seed not present (installed package, not repo)")
def test_packaged_portfolio_matches_dbt_seed():
    """The extraction portfolio (YAML) and dim_area's seed (CSV) are two copies of
    one list. Until they share a single source of truth, this test fails the
    moment they drift on the fields both carry."""
    yaml_rows = {
        tuple(area[key] for key in REQUIRED_AREA_KEYS) for area in load_areas()
    }
    with SEED_CSV.open(encoding="utf-8", newline="") as f:
        csv_rows = {
            tuple(row[key] for key in REQUIRED_AREA_KEYS) for row in csv.DictReader(f)
        }
    assert yaml_rows == csv_rows, (
        "areas_config.yml and dbt_project/seeds/areas_config.csv disagree.\n"
        f"only in yaml: {yaml_rows - csv_rows}\nonly in csv: {csv_rows - yaml_rows}"
    )
