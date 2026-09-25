"""Static checks on the dbt profile template.

The transform Lambda's image copies ``profiles.yml.example`` as its
``profiles.yml``, so the ``prod`` target is production configuration even
though it lives in an example file.

Some of its mistakes cannot be reproduced on a laptop. The one guarded here: a
DuckDB setting placed under ``settings:`` is applied per cursor *after*
dbt-duckdb has connected and installed the extensions, whereas ``config_options:``
is passed into ``duckdb.connect()`` itself. ``extension_directory`` under
``settings:`` therefore arrives too late - the extensions have already been
installed into DuckDB's default ``~/.duckdb``, which is read-only in Lambda. A
laptop has a writable home folder and a network connection, so the mistake
works fine here and fails only in the cloud. Reading the file is the only way to
catch it before deploying.
"""

from pathlib import Path

import yaml

PROFILES_EXAMPLE = (
    Path(__file__).resolve().parents[1] / "dbt_project" / "profiles.yml.example"
)


def _prod_target() -> dict:
    profiles = yaml.safe_load(PROFILES_EXAMPLE.read_text(encoding="utf-8"))
    return profiles["eskom_grid_observability"]["outputs"]["prod"]


def test_prod_sets_extension_directory_before_extensions_install():
    prod = _prod_target()
    assert "extension_directory" in (prod.get("config_options") or {}), (
        "prod must set extension_directory under config_options, which dbt-duckdb "
        "passes to duckdb.connect() - before it installs the extensions."
    )


def test_prod_has_no_settings_block():
    """``settings:`` is applied after connecting; nothing in prod needs that timing."""
    assert "settings" not in _prod_target(), (
        "prod has a settings: block. dbt-duckdb applies those after the extensions "
        "are installed; anything the install depends on belongs in config_options:."
    )
