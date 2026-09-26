"""Eskom Grid Observability — application package.

This module deliberately imports nothing from the package itself. Callers import
the submodule they need (``eskom_grid.extract``, ``eskom_grid.sinks``,
``eskom_grid.config``), so that the extraction logic can run in environments
where Dagster is not installed — such as AWS Lambda. ``eskom_grid.assets`` is
the only module that imports Dagster, and nothing else in the package imports it.
"""

from importlib.metadata import PackageNotFoundError, version

# Read from the installed package's metadata, which pip writes from
# pyproject.toml - the one place the version is set. A hand-typed copy here
# drifted: v0.3.2 reported itself as 0.1.0.
try:
    __version__ = version("eskom-grid")
except PackageNotFoundError:
    # Source files used without installing them. Looking up a version must
    # never be the reason the package fails to import.
    __version__ = "0+unknown"
