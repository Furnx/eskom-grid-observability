"""Eskom Grid Observability — application package.

This module deliberately imports nothing. Callers import the submodule they
need (``eskom_grid.extract``, ``eskom_grid.sinks``, ``eskom_grid.config``),
so that the extraction logic can run in environments where Dagster is not
installed — such as AWS Lambda. ``eskom_grid.assets`` is the only module that
imports Dagster, and nothing else in the package imports it.
"""

__version__ = "0.1.0"
