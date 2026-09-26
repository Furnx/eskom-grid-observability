"""End-to-end tests for the dbt transformation.

These build the real dbt project against fixture files in a temporary directory,
so they exercise the landing model, the incremental cutoff, the star schema and
every dbt test at once. They are marked ``slow``: ``pytest -m "not slow"`` skips
them.

The incrementality assertions are the reason this file exists. The landing
model's filter can be rewritten in ways that still return exactly the right rows
while making DuckDB re-read every file in the landing zone - correct results,
hundreds of thousands of needless S3 requests a month in the cloud. ``loaded_at``
is the evidence: a row that was not re-read keeps the timestamp it was first
landed with.

A dbt build costs about fifteen seconds, so the read-only assertions share one
module-scoped build and only the tests that need a second build pay for one.

The Lambda uploads the warehouse file the moment ``run_transform()`` returns, from
a process that stays alive between invocations. So "the build succeeded" is not
enough: the file on disk must be complete and released by then. Checks of that
read from a *separate* process, because inside this one ``duckdb.connect()`` to
an already-open file returns the open database - including data that exists only
in its write-ahead log and was never written to the file.
"""

import json
import multiprocessing.synchronize as mp_sync
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from eskom_grid.transform import TransformError, run_transform

pytestmark = pytest.mark.slow

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT = REPO_ROOT / "dbt_project"
FIXTURE_RAW = Path(__file__).parent / "fixtures" / "raw"

JHB = "za_gt_jhb_johannesburg_9hfs"
CPT = "za_wc_cpt_capetowncbd_utix"

RUN_1 = "20260924_100000"  # both areas, no events
RUN_2 = "20260924_110000"  # Johannesburg has a Stage 2 event
RUN_3 = "20260924_120000"  # added mid-test, to prove only new files are read

# Captured at import, before any test can swap them.
ORIGINAL_MP_LOCK = mp_sync.Lock
ORIGINAL_MP_RLOCK = mp_sync.RLock

AREA_META = {
    JHB: {"area_id": JHB, "area_name": "Johannesburg",
          "municipality": "City of Johannesburg", "province": "Gauteng"},
    CPT: {"area_id": CPT, "area_name": "Cape Town CBD",
          "municipality": "City of Cape Town", "province": "Western Cape"},
}


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_project(tmp_path: Path, monkeypatch) -> dict:
    """A throwaway copy of the dbt project, pointed at fixture raw files."""
    if not (DBT_PROJECT / "dbt_packages").is_dir():
        pytest.skip("dbt_packages missing - run `dbt deps` in dbt_project first.")

    project_dir = tmp_path / "dbt_project"
    shutil.copytree(
        DBT_PROJECT,
        project_dir,
        ignore=shutil.ignore_patterns("target", "logs", "profiles.yml", "*.duckdb"),
    )

    raw_dir = tmp_path / "raw"
    shutil.copytree(FIXTURE_RAW, raw_dir)

    # In a folder of its own, as in Lambda (/tmp/...), so a test can delete and
    # recreate the folder the way the handler does between warm invocations.
    warehouse = tmp_path / "warehouse" / "test.duckdb"
    warehouse.parent.mkdir()

    # A profile of our own, so the test never depends on the developer's local
    # profiles.yml and never touches the real warehouse.
    (project_dir / "profiles.yml").write_text(
        "eskom_grid_observability:\n"
        "  target: test\n"
        "  outputs:\n"
        "    test:\n"
        "      type: duckdb\n"
        f"      path: '{warehouse.as_posix()}'\n"
        "      extensions:\n"
        "        - json\n",
        encoding="utf-8",
    )

    # The source's external_location reads this; posix separators keep DuckDB's
    # glob happy on Windows.
    monkeypatch.setenv("ESKOM_RAW_GLOB", f"{raw_dir.as_posix()}/**/*.json")

    return {"project_dir": project_dir, "raw_dir": raw_dir,
            "warehouse": warehouse, "tmp": tmp_path}


def build(project, **kwargs):
    return run_transform(
        project["project_dir"],
        target="test",
        target_path=project["tmp"] / "target",
        log_path=project["tmp"] / "logs",
        **kwargs,
    )


def query(warehouse: Path, sql: str):
    """Read from the warehouse, read-only.

    ``read_only=True`` is also a tripwire. If ``run_transform()`` ever returns
    with dbt's read-write connection still open, DuckDB refuses a second
    connection to the same file under a different configuration, and every test
    using this helper fails. (v0.3.0 dropped ``read_only`` here to get past exactly
    that error - which hid the bug the Lambda then hit.)
    """
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        return con.sql(sql).fetchall()
    finally:
        con.close()


def query_from_another_process(warehouse: Path, sql: str):
    """Read the warehouse the way the S3 upload effectively does: from outside.

    A fresh interpreter sees only what is in the file on disk. Data still sitting
    in the write-ahead log, or a file locked by a connection this process forgot
    to close, both show up here - and neither shows up in ``query()`` above while
    that connection is alive.
    """
    code = (
        "import duckdb, json, sys\n"
        "con = duckdb.connect(sys.argv[1], read_only=True)\n"
        "print(json.dumps(con.sql(sys.argv[2]).fetchall(), default=str))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(warehouse), sql],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"another process could not read {warehouse.name}: "
        f"{result.stderr.strip().splitlines()[-1] if result.stderr.strip() else 'no stderr'}"
    )
    return [tuple(row) for row in json.loads(result.stdout)]


def wal_of(warehouse: Path) -> Path:
    return warehouse.with_name(warehouse.name + ".wal")


def add_raw_file(raw_dir: Path, area: str, run_ts: str, events: list) -> None:
    path = raw_dir / area / f"{run_ts}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"events": events, "_meta": AREA_META[area]}, indent=4),
        encoding="utf-8",
    )


# ── fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def built_once(tmp_path_factory):
    """One project, built once. Shared by every read-only assertion below."""
    monkeypatch = pytest.MonkeyPatch()
    try:
        project = _make_project(tmp_path_factory.mktemp("built_once"), monkeypatch)
        project["summary"] = build(project)
        yield project
    finally:
        monkeypatch.undo()


@pytest.fixture
def fresh_project(tmp_path, monkeypatch):
    """An unbuilt project, for tests that build more than once or mutate inputs."""
    return _make_project(tmp_path, monkeypatch)


# ── a single build ────────────────────────────────────────────────────────────


def test_build_succeeds_with_every_dbt_test_passing(built_once):
    summary = built_once["summary"]
    assert summary.success
    assert summary.failed == 0
    assert summary.failed_nodes == []
    assert summary.passed == summary.total_nodes


def test_every_fixture_file_is_landed_exactly_once(built_once):
    rows = query(
        built_once["warehouse"],
        "SELECT COUNT(*) AS n_rows, COUNT(DISTINCT source_file) AS n_files "
        "FROM stg_eskom__raw_payloads",
    )
    assert rows[0] == (4, 4)


def test_event_reaches_the_fact_table_with_expected_values(built_once):
    """The case live data has not produced yet: a real loadshedding event."""
    rows = query(
        built_once["warehouse"],
        "SELECT area_id, event_classification, event_stage, duration_hours, start_time "
        "FROM fct_grid_events",
    )
    assert len(rows) == 1
    area_id, classification, stage, duration, start_time = rows[0]
    assert area_id == JHB
    assert classification == "Loadshedding"
    assert stage == 2
    assert duration == pytest.approx(2.5)
    # The API's +02:00 offset is dropped by the cast: SAST wall-clock, not UTC.
    assert start_time.hour == 16


def test_dimensions_stay_populated_although_most_areas_had_no_events(built_once):
    warehouse = built_once["warehouse"]
    assert query(warehouse, "SELECT COUNT(*) FROM dim_area")[0][0] >= 2, (
        "dim_area is derived from the seed precisely so a zero-event day cannot empty it"
    )
    # Cape Town contributed no events at all, yet must still be joinable.
    assert query(
        warehouse, f"SELECT COUNT(*) FROM dim_area WHERE area_id = '{CPT}'"
    )[0][0] == 1


def test_pipeline_runs_records_one_row_per_run_with_honest_counts(built_once):
    rows = query(
        built_once["warehouse"],
        "SELECT run_timestamp, areas_processed, total_events_found, zero_event_areas "
        "FROM fct_pipeline_runs ORDER BY run_timestamp",
    )
    assert len(rows) == 2

    first, second = rows
    assert (first[1], first[2]) == (2, 0)
    assert "Johannesburg" in first[3] and "Cape Town CBD" in first[3]

    assert (second[1], second[2]) == (2, 1)
    # Johannesburg had the event, so only Cape Town is listed as zero-event.
    assert second[3] == "Cape Town CBD"


# ── two builds: the reason this file exists ───────────────────────────────────


def test_second_build_reads_only_new_files_and_adds_no_duplicates(fresh_project):
    """Rows from strictly older runs must keep their original ``loaded_at``.

    The cutoff uses ``>=``, so the newest already-loaded run is deliberately
    re-read - that is how a run which landed only some of its areas gets
    completed later. Everything older than it must be left untouched.
    """
    build(fresh_project)
    warehouse = fresh_project["warehouse"]

    before = dict(query(warehouse, "SELECT source_file, loaded_at FROM stg_eskom__raw_payloads"))
    assert len(before) == 4
    runs_before = query(warehouse, "SELECT COUNT(*) FROM fct_pipeline_runs")[0][0]

    add_raw_file(fresh_project["raw_dir"], JHB, RUN_3, [])
    add_raw_file(fresh_project["raw_dir"], CPT, RUN_3, [])

    build(fresh_project)

    after = dict(query(warehouse, "SELECT source_file, loaded_at FROM stg_eskom__raw_payloads"))
    assert len(after) == 6, "the two new files should have been landed"

    untouched = [f for f in before if f"{RUN_2}.json" not in f]
    assert untouched, "expected fixtures older than the boundary run"
    for source_file in untouched:
        assert after[source_file] == before[source_file], (
            f"{source_file} was re-read on the second build. The landing model's "
            "cutoff must stay a literal, directly on the file read - see ADR 0006 "
            "in eskom-grid-cloud."
        )

    assert query(
        warehouse,
        "SELECT source_file FROM stg_eskom__raw_payloads GROUP BY 1 HAVING COUNT(*) > 1",
    ) == []
    assert query(
        warehouse, "SELECT event_id FROM fct_grid_events GROUP BY 1 HAVING COUNT(*) > 1"
    ) == []

    runs_after = query(warehouse, "SELECT COUNT(*) FROM fct_pipeline_runs")[0][0]
    assert runs_after == runs_before + 1


def test_files_without_a_run_timestamp_are_ignored(fresh_project):
    """The pre-2026-09 landing zone wrote flat files with no timestamp in the name."""
    (fresh_project["raw_dir"] / "za_legacy_flat.json").write_text(
        json.dumps({"events": [], "_meta": AREA_META[JHB]}), encoding="utf-8"
    )

    build(fresh_project)

    warehouse = fresh_project["warehouse"]
    assert query(warehouse, "SELECT COUNT(*) FROM stg_eskom__raw_payloads")[0][0] == 4
    assert query(
        warehouse,
        "SELECT COUNT(*) FROM stg_eskom__raw_payloads WHERE run_ts IS NULL OR run_ts = ''",
    )[0][0] == 0


# ── the file on disk: what the Lambda actually uploads ────────────────────────


def test_database_is_closed_and_complete_when_run_transform_returns(built_once):
    """The handler uploads the file immediately after ``run_transform()`` returns.

    If dbt's connection were still open, the tables would be in the ``.wal`` and
    the file itself would be a near-empty shell - uploaded hourly, looking like
    success.
    """
    warehouse = built_once["warehouse"]

    assert not wal_of(warehouse).exists(), (
        "a .wal file remains after run_transform(): the database was not closed, "
        "so the file on disk does not contain the build."
    )
    assert query_from_another_process(
        warehouse, "SELECT COUNT(*) FROM stg_eskom__raw_payloads"
    ) == [(4,)]


def test_warm_process_builds_into_a_replaced_file(fresh_project):
    """Two invocations in one process, with the file replaced in between.

    A warm Lambda reuses its process, and the handler starts each run from a
    clean /tmp and a freshly downloaded warehouse. dbt-duckdb keeps its database
    handle in a class-level variable and reuses it when the credentials have not
    changed, so without an explicit close the second build would write into the
    first run's file - deleted from the folder, still open by handle - and the
    file actually uploaded would be missing the new rows.

    Before the fix this fails at a different step per platform: on Linux at the
    final assertion; on Windows at the delete, because Windows refuses to remove
    a file another handle still holds open.
    """
    raw_dir, warehouse = fresh_project["raw_dir"], fresh_project["warehouse"]
    later = fresh_project["tmp"] / "later_run"

    # Hold back the 11:00 fixtures so the first build sees only the 10:00 run.
    for area in (JHB, CPT):
        (later / area).mkdir(parents=True)
        shutil.move(str(raw_dir / area / f"{RUN_2}.json"), str(later / area))

    build(fresh_project)

    # What the handler does between invocations: the file goes up to S3, /tmp is
    # wiped, and the next invocation downloads it to the same path.
    downloaded = fresh_project["tmp"] / "downloaded.duckdb"
    shutil.copy2(warehouse, downloaded)
    shutil.rmtree(warehouse.parent)
    warehouse.parent.mkdir()
    shutil.copy2(downloaded, warehouse)

    # The next extraction run lands.
    for area in (JHB, CPT):
        shutil.move(str(later / area / f"{RUN_2}.json"), str(raw_dir / area))

    build(fresh_project)

    assert not wal_of(warehouse).exists()
    assert query_from_another_process(
        warehouse, "SELECT COUNT(*) FROM stg_eskom__raw_payloads"
    ) == [(4,)]
    assert query_from_another_process(
        warehouse, "SELECT COUNT(*) FROM fct_pipeline_runs"
    ) == [(2,)]


# ── no shared memory: what Lambda lacks ───────────────────────────────────────


def test_builds_without_posix_semaphores(fresh_project, no_semaphores):
    """Lambda has no /dev/shm, so no multiprocessing lock can be created there.

    dbt's connection manager, its manifest and Python's ThreadPool each create one
    regardless of --single-threaded, so without a fallback the build dies before
    any node runs, with "[Errno 2] No such file or directory". See the
    ``no_semaphores`` fixture for why every kind of semaphore is refused, not only
    the two the fallback replaces.
    """
    summary = build(fresh_project)

    assert summary.success
    assert summary.failed == 0
    assert summary.passed == summary.total_nodes


def test_multiprocessing_is_left_alone_where_semaphores_work(built_once):
    """After a normal build on a laptop, the standard library is untouched."""
    assert mp_sync.Lock is ORIGINAL_MP_LOCK
    assert mp_sync.RLock is ORIGINAL_MP_RLOCK


# ── failure behaviour ─────────────────────────────────────────────────────────


def test_missing_project_raises_transform_error(tmp_path):
    with pytest.raises(TransformError, match="No dbt_project.yml"):
        run_transform(tmp_path / "nowhere")


def test_unknown_target_raises_transform_error(fresh_project):
    with pytest.raises(TransformError):
        run_transform(
            fresh_project["project_dir"],
            target="no_such_target",
            target_path=fresh_project["tmp"] / "target",
            log_path=fresh_project["tmp"] / "logs",
        )
