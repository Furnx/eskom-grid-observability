"""The Lambda contract, as a test.

The Lambda image installs this package with only its core dependencies —
Dagster is not there. If ``eskom_grid.extract`` (or anything it imports) ever
pulls Dagster in, the function fails on its first import, in production, long
after the commit that caused it. Locally the same mistake is invisible,
because Dagster *is* installed here and the import just works.

So we check what actually lands in ``sys.modules`` after importing the
Lambda-facing modules, in a fresh interpreter so nothing imported by pytest
or other tests can mask the result.
"""

import subprocess
import sys

LAMBDA_FACING_MODULES = ["eskom_grid", "eskom_grid.extract", "eskom_grid.sinks", "eskom_grid.config"]

# Heavy or environment-specific packages that must never be imported by the
# Lambda-facing modules. boto3 is allowed lazily (inside S3RawSink) but must
# not be imported at module import time either.
FORBIDDEN = ["dagster", "dagster_dbt", "dbt", "duckdb", "boto3", "dotenv"]


def _imported_forbidden(module: str, forbidden: list[str] | None = None) -> list[str]:
    forbidden = forbidden or FORBIDDEN
    code = (
        "import sys, importlib\n"
        f"importlib.import_module({module!r})\n"
        f"hits = [m for m in {forbidden!r} if m in sys.modules "
        "or any(k.startswith(m + '.') for k in sys.modules)]\n"
        "print(','.join(hits))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    return [m for m in result.stdout.strip().split(",") if m]


def test_lambda_facing_modules_do_not_import_orchestration_or_warehouse_packages():
    for module in LAMBDA_FACING_MODULES:
        hits = _imported_forbidden(module)
        assert not hits, f"{module} transitively imports {hits} — it must stay Lambda-safe."


def test_importing_transform_does_not_import_dbt():
    """``eskom_grid.transform`` must import dbt only when it is actually run.

    The extraction Lambda installs this package without the transform extra, so
    a dbt import at module scope would break it on its very first import. The
    deferred import inside ``run_transform`` is exactly the kind of thing a
    later tidy-up moves to the top of the file, where it would still work in
    every environment that happens to have dbt installed — including this one.
    """
    hits = _imported_forbidden(
        "eskom_grid.transform", forbidden=["dbt", "duckdb", "dagster"]
    )
    assert not hits, (
        f"importing eskom_grid.transform pulled in {hits}. dbt must be imported "
        "inside run_transform(), not at module scope."
    )
