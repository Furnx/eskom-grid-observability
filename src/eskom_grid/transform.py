"""Run the dbt transformation, from anywhere.

The counterpart to :mod:`eskom_grid.extract`: it knows how to build the dbt
project and nothing about who asked. Callers - the Lambda handler in
eskom-grid-cloud, a test, or a person at a terminal - resolve configuration and
pass it in.

dbt is imported *inside* :func:`run_transform`, not at module scope. Importing
this module therefore costs nothing and pulls in no dbt, so the extraction
Lambda can install the package without it. ``tests/test_import_boundary.py``
enforces that.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


class TransformError(Exception):
    """A dbt build did not complete successfully.

    Carries the failing node names so an orchestrator, or a log reader, sees
    *what* failed without having to parse dbt's output.
    """

    def __init__(self, message: str, failed_nodes: list[str] | None = None) -> None:
        super().__init__(message)
        self.failed_nodes = failed_nodes or []


class LogLike(Protocol):
    """Anything with ``info``/``warning``/``error`` - stdlib or Dagster's logger."""

    def info(self, msg: str, *args, **kwargs) -> None: ...
    def warning(self, msg: str, *args, **kwargs) -> None: ...
    def error(self, msg: str, *args, **kwargs) -> None: ...


@dataclass
class TransformSummary:
    """What a build did. Small enough to return from a Lambda as JSON."""

    success: bool
    total_nodes: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    failed_nodes: list[str] = field(default_factory=list)


# dbt statuses that mean "this node did not do its job".
_FAILURE_STATUSES = {"error", "fail", "runtime error"}
_SKIPPED_STATUSES = {"skipped"}


def run_transform(
    project_dir: str | Path,
    *,
    profiles_dir: str | Path | None = None,
    target: str = "prod",
    target_path: str | Path | None = None,
    log_path: str | Path | None = None,
    full_refresh: bool = False,
    log: LogLike | None = None,
) -> TransformSummary:
    """Run ``dbt build`` (models *and* their tests) and summarise the result.

    Args:
        project_dir: the dbt project directory (the one holding dbt_project.yml).
        profiles_dir: where profiles.yml lives. Defaults to ``project_dir``.
        target: profile target to use - ``dev`` locally, ``prod`` in the cloud.
        target_path: where dbt writes compiled SQL and artefacts. Must be
            writable; in Lambda only ``/tmp`` is, and the project directory is
            not, so this is passed explicitly rather than left to default
            inside the project.
        log_path: where dbt writes its log file. Same reasoning.
        full_refresh: rebuild incremental models from scratch. Used for the
            first cloud run and for recovery, never routinely.
        log: optional logger-like object.

    Returns:
        TransformSummary describing the run.

    Raises:
        TransformError: if the build failed, listing the failing nodes.
    """
    log = log or logging.getLogger(__name__)

    # Imported here, not at module scope: the extraction Lambda installs this
    # package without dbt, and merely importing eskom_grid.transform must not
    # require it.
    from dbt.cli.main import dbtRunner

    project_dir = Path(project_dir)
    if not (project_dir / "dbt_project.yml").is_file():
        raise TransformError(f"No dbt_project.yml in {project_dir}.")

    args: list[str] = [
        "build",
        "--project-dir", os.fspath(project_dir),
        "--profiles-dir", os.fspath(profiles_dir or project_dir),
        "--target", target,
    ]
    if target_path is not None:
        args += ["--target-path", os.fspath(target_path)]
    if log_path is not None:
        args += ["--log-path", os.fspath(log_path)]
    if full_refresh:
        args.append("--full-refresh")

    log.info(f"dbt build starting (target={target}, project={project_dir}).")

    result = dbtRunner().invoke(args)
    summary = _summarise(result)

    if summary.failed_nodes:
        log.error(f"dbt build failed: {summary.failed_nodes}")
    else:
        log.info(
            f"dbt build finished: {summary.passed} passed, "
            f"{summary.failed} failed, {summary.skipped} skipped."
        )

    if not summary.success:
        # An exception raised before any node ran (bad profile, unparseable
        # project) arrives as result.exception with no node results at all.
        if result.exception is not None and not summary.failed_nodes:
            raise TransformError(f"dbt build could not run: {result.exception}") from result.exception
        raise TransformError(
            f"dbt build failed for {len(summary.failed_nodes)} node(s): "
            f"{', '.join(summary.failed_nodes) or 'unknown'}",
            failed_nodes=summary.failed_nodes,
        )

    return summary


def _summarise(result) -> TransformSummary:
    """Turn a dbtRunnerResult into a TransformSummary.

    ``result.result`` is a RunExecutionResult when nodes ran, and something else
    (or nothing) when dbt failed before that - hence the defensive access.
    """
    node_results = getattr(result.result, "results", None) or []

    failed_nodes: list[str] = []
    passed = failed = skipped = 0

    for node_result in node_results:
        status = str(getattr(node_result, "status", "")).lower()
        name = getattr(getattr(node_result, "node", None), "unique_id", "unknown")
        if status in _FAILURE_STATUSES:
            failed += 1
            failed_nodes.append(name)
        elif status in _SKIPPED_STATUSES:
            skipped += 1
        else:
            passed += 1

    return TransformSummary(
        success=bool(result.success),
        total_nodes=len(node_results),
        passed=passed,
        failed=failed,
        skipped=skipped,
        failed_nodes=failed_nodes,
    )
