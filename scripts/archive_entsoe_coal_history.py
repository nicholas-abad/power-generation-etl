"""Archive one UTC year of Czech source XML, without database access.

Run using the extractor environment and its src on PYTHONPATH. The API key
comes from ENTSOE_API_KEY only. Completed daily files are hash-checked on resume;
no failed request is represented as an empty generation day.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time

import pandas as pd

from energy_extractors.entsoe.extractor import ThrottledSession, _utf8_raw_client
from energy_extractors.entsoe.periods import parse_coal_periods


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def archive(start, end, destination, reuse):
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("An increasing timezone-aware window is required")
    if start != start.normalize() or end != end.normalize():
        raise ValueError("Use complete UTC days")
    if end > pd.Timestamp.now(tz="UTC").normalize():
        raise ValueError("Do not request the current incomplete UTC day")
    destination.mkdir(parents=True, exist_ok=True)
    raw_dir = destination / "source_xml"
    raw_dir.mkdir(exist_ok=True)
    manifest_path = destination / "manifest.json"
    manifest = {
        "scope": "CZ coal source audit; no database access",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "requests": {},
    }
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if (manifest["start"], manifest["end"]) != (start.isoformat(), end.isoformat()):
            raise ValueError("Existing archive has different request bounds")
    reusable = {}
    if reuse:
        for entry in json.loads((reuse / "source-sampling.json").read_text()):
            if entry.get("status") == "success":
                reusable[entry["date"]] = entry
    # Never log request URLs: the source API embeds its token in query params.
    logging.getLogger("entsoe").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    worker_state = threading.local()
    sessions = []
    days = list(pd.date_range(start, end, freq="D", inclusive="left"))
    pending = []
    for day in days:
        date = day.strftime("%Y-%m-%d")
        entry = manifest["requests"].get(date)
        path = raw_dir / f"CZ_{day:%Y%m%d}.xml"
        if entry and entry.get("status") == "success":
            if hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
                raise ValueError(f"Archived XML hash mismatch: {date}")
        else:
            pending.append(day)

    def fetch(day):
        date = day.strftime("%Y-%m-%d")
        path = raw_dir / f"CZ_{day:%Y%m%d}.xml"
        entry = {
            "date": date,
            "start": day.isoformat(),
            "end": (day + pd.Timedelta(days=1)).isoformat(),
            "path": str(path.relative_to(destination)),
        }
        if date in reusable:
            xml = (reuse / "source_xml" / path.name).read_bytes()
            if hashlib.sha256(xml).hexdigest() != reusable[date]["sha256"]:
                raise ValueError(f"Reusable XML hash mismatch: {date}")
            entry["reused_archive"] = str(reuse.resolve())
            return path, entry, xml
        if not hasattr(worker_state, "client"):
            session = ThrottledSession(rate_limit_delay=1.0, jitter=0.1)
            sessions.append(session)
            worker_state.client = _utf8_raw_client(
                api_key=os.environ["ENTSOE_API_KEY"],
                session=session,
                retry_count=1,
                timeout=45,
            )
        for attempt in range(4):
            try:
                xml = worker_state.client.query_generation_per_plant(
                    "CZ", day, day + pd.Timedelta(days=1)
                ).encode("utf-8")
                entry["retrieved_at"] = datetime.now(timezone.utc).isoformat()
                entry.pop("error_type", None)
                return path, entry, xml
            except Exception as error:
                entry.update(status="request_failed", error_type=type(error).__name__)
                print(
                    f"{date}: request failed ({type(error).__name__}), attempt {attempt + 1}/4",
                    flush=True,
                )
                if attempt < 3:
                    time.sleep(min(30, 5 * 2**attempt))
        return path, entry, None

    completed = len(days) - len(pending)
    try:
        # Four independent sessions cap the aggregate at four requests/second.
        # Only one batch is in flight; errors never queue the rest of a year.
        with ThreadPoolExecutor(max_workers=4) as pool:
            for offset in range(0, len(pending), 4):
                futures = [
                    pool.submit(fetch, day) for day in pending[offset : offset + 4]
                ]
                for future in as_completed(futures):
                    path, entry, xml = future.result()
                    date = entry["date"]
                    if xml is None:
                        manifest["requests"][date] = entry
                        save(manifest_path, manifest)
                        raise RuntimeError(f"Source request failed for {date}")
                    # Preserve unexpected source shapes without certifying them.
                    path.write_bytes(xml)
                    entry["sha256"] = hashlib.sha256(xml).hexdigest()
                    try:
                        rows, stats = parse_coal_periods(
                            xml, "CZ", "10YCZ-CEPS-----N", entry["start"], entry["end"]
                        )
                    except Exception as error:
                        entry.update(
                            status="parse_failed", error_type=type(error).__name__
                        )
                        manifest["requests"][date] = entry
                        save(manifest_path, manifest)
                        raise
                    entry.update(status="success", rows=len(rows), parser_counts=stats)
                    manifest["requests"][date] = entry
                    save(manifest_path, manifest)
                    completed += 1
                    if completed % 10 == 0 or completed == len(days):
                        print(
                            f"Archived {completed}/{len(days)} days; latest {date}",
                            flush=True,
                        )
    finally:
        for session in sessions:
            session.close()
    if set(manifest["requests"]) != {day.strftime("%Y-%m-%d") for day in days}:
        raise ValueError("Archive is incomplete")
    print(f"Complete: {len(days)} hash-verified source days", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reuse-samples", type=Path)
    args = parser.parse_args()
    try:
        archive(
            pd.Timestamp(args.start, tz="UTC"),
            pd.Timestamp(args.end, tz="UTC"),
            args.output,
            args.reuse_samples,
        )
    except Exception as error:
        # Request failures may contain a secret-bearing URL, so emit no traceback.
        print(f"Archive stopped: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
