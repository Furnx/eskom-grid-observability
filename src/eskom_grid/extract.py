"""Pure extraction logic for the EskomSePush API v3.0.

This module knows nothing about Dagster, AWS, environment variables or where
files end up. Callers — the Dagster asset, the Lambda handler, the tests —
resolve configuration, build a sink, and call :func:`run_extraction`. That
separation is what lets one implementation run on a laptop, in Docker and in
Lambda without change.

Data contract (unchanged from the original worker):
  * If the API omits ``events`` (no events scheduled), an empty list is
    injected so downstream ``UNNEST`` yields zero rows instead of failing.
  * A ``_meta`` block (area_id, area_name, municipality, province) is injected
    so dimensional context does not depend on the API payload's shape.

Exceptions carry meaning: orchestrators can match on their class names
(Step Functions ``ErrorEquals``) to decide whether a retry is worthwhile.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Protocol

import requests

from .sinks import RawSink

API_BASE_URL = "https://developer.sepush.co.za/business/3.0"
DEFAULT_TIMEOUT_SECONDS = 30


# ── Exceptions ────────────────────────────────────────────────────────────────


class ExtractionError(Exception):
    """Base class for extraction failures."""


class RateLimitError(ExtractionError):
    """HTTP 429 — the daily API quota is exhausted. Retrying will not help today."""


class ApiError(ExtractionError):
    """The API answered, but not with a usable schedule payload."""


# ── Types ─────────────────────────────────────────────────────────────────────


class LogLike(Protocol):
    """Anything with ``info``/``warning``/``error`` — a stdlib Logger or Dagster's ``context.log``."""

    def info(self, msg: str, *args, **kwargs) -> None: ...
    def warning(self, msg: str, *args, **kwargs) -> None: ...
    def error(self, msg: str, *args, **kwargs) -> None: ...


@dataclass
class ExtractionSummary:
    """What a run did — the caller turns this into metadata, logs or a run record."""

    run_ts: str
    areas_processed: int = 0
    total_events: int = 0
    zero_event_areas: list[str] = field(default_factory=list)
    written: list[str] = field(default_factory=list)  # URIs, one per area


# ── Functions ─────────────────────────────────────────────────────────────────


def make_run_ts(now: dt.datetime | None = None) -> str:
    """UTC timestamp used as the file name for every area in one run."""
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.strftime("%Y%m%d_%H%M%S")


def fetch_area_schedule(
    area_id: str,
    api_key: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict:
    """Fetch the raw schedule payload for one area. One API request.

    Raises:
        RateLimitError: on HTTP 429.
        ApiError: on a non-JSON body or an ``error`` payload.
        requests.HTTPError: on any other non-2xx status.
    """
    response = requests.get(
        f"{API_BASE_URL}/area",
        params={"id": area_id},
        headers={"token": api_key},
        timeout=timeout,
    )

    if response.status_code == 429:
        raise RateLimitError(
            f"HTTP 429 fetching '{area_id}': daily API quota exceeded. "
            "Remaining areas were not fetched."
        )
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:  # requests' JSONDecodeError is a ValueError
        raise ApiError(
            f"Non-JSON response for '{area_id}': {response.text[:200]!r}"
        ) from exc

    if "error" in payload:
        raise ApiError(f"API error for '{area_id}': {payload['error']}")

    return payload


def normalize_payload(payload: dict, area: dict) -> tuple[dict, bool]:
    """Apply the data contract. Returns ``(normalised_payload, events_were_injected)``.

    The input is not mutated.
    """
    normalised = dict(payload)
    injected = "events" not in normalised
    if injected:
        normalised["events"] = []
    normalised["_meta"] = {
        "area_id": area["area_id"],
        "area_name": area["area_name"],
        "municipality": area["municipality"],
        "province": area["province"],
    }
    return normalised, injected


def run_extraction(
    areas: list[dict],
    api_key: str,
    sink: RawSink,
    *,
    log: LogLike | None = None,
    run_ts: str | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> ExtractionSummary:
    """Fetch, normalise and write every area's schedule. One request per area.

    Args:
        areas: the portfolio, e.g. from :func:`eskom_grid.config.load_areas`.
        api_key: EskomSePush token. The caller decides where it comes from.
        sink: where payloads go (see :mod:`eskom_grid.sinks`).
        log: optional logger-like object; defaults to a stdlib logger.
        run_ts: override the run timestamp (tests, replays). Every area in a
            run shares one timestamp so a run's files can be correlated.
        timeout: per-request HTTP timeout in seconds.

    A :class:`RateLimitError` stops the run immediately — spending further
    requests against an exhausted quota would only waste tomorrow's budget.
    """
    log = log or logging.getLogger(__name__)

    if not api_key:
        raise ValueError("api_key is empty. The caller must supply ESKOM_API_KEY.")
    if not areas:
        raise ValueError("No areas to extract. Check the area portfolio.")

    summary = ExtractionSummary(run_ts=run_ts or make_run_ts())
    log.info(f"Extracting {len(areas)} area(s) into {sink!r} (run_ts={summary.run_ts}).")

    for area in areas:
        area_id, area_name = area["area_id"], area["area_name"]
        log.info(f"Fetching schedule for '{area_name}' ({area_id}) ...")

        payload = fetch_area_schedule(area_id, api_key, timeout=timeout)
        payload, injected = normalize_payload(payload, area)
        if injected:
            log.info(
                f"No 'events' key in response for '{area_name}'. "
                "Injected an empty array to keep the schema stable."
            )

        event_count = len(payload["events"])
        summary.total_events += event_count
        if event_count == 0:
            summary.zero_event_areas.append(area_name)

        uri = sink.write(area_id, summary.run_ts, payload)
        summary.written.append(uri)
        summary.areas_processed += 1
        log.info(f"  → {event_count} event(s); saved to {uri}")

    log.info(
        f"Extraction complete: {summary.areas_processed} area(s), "
        f"{summary.total_events} event(s) in total."
    )
    if summary.zero_event_areas:
        log.info(f"Zero-event areas (grid was up): {summary.zero_event_areas}")

    return summary
