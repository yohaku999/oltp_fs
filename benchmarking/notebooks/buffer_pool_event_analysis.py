"""Memory-bounded buffer-pool event analysis for the benchmark notebook."""

from collections import Counter, deque
from pathlib import Path
import json
import re

import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import display


_CACHE_VERSION = 1
_CACHE_DIR_NAME = ".buffer_pool_event_cache"
_CACHE_TABLES = {
    "summary": [
        "type",
        "page_kind",
        "dirty",
        "hit",
        "events",
    ],
    "per_second": [
        "elapsed_second",
        "type",
        "hit",
        "events",
    ],
    "per_page": [
        "page_key",
        "page_kind",
        "type",
        "hit",
        "events",
    ],
    "per_frame": [
        "frame_id",
        "type",
        "events",
    ],
    "last_events": [
        "event_id",
        "logged_at",
        "execute_elapsed_seconds",
        "type",
        "page_id",
        "frame_id",
        "page_kind",
        "dirty",
        "hit",
        "file",
    ],
}

_DEDICATED_EVENT_PATTERN = re.compile(
    r"^logged_at_unix_ms=(?P<unix_ms>\d+)\s+"
    r"buffer_pool_event\s+type=(?P<type>\w+)\s+"
    r"event_id=(?P<event_id>\d+)\s+"
    r"file=(?P<file>\S+)\s+"
    r"page_id=(?P<page_id>\d+)\s+"
    r"frame_id=(?P<frame_id>\d+)\s+"
    r"page_kind=(?P<page_kind>\w+)\s+"
    r"dirty=(?P<dirty>[01])"
    r"(?:\s+hit=(?P<hit>[01]))?"
)
_GENERIC_EVENT_PATTERN = re.compile(
    r"buffer_pool_event\s+type=(?P<type>\w+)\s+"
    r"event_id=(?P<event_id>\d+)\s+"
    r"file=(?P<file>\S+)\s+"
    r"page_id=(?P<page_id>\d+)\s+"
    r"frame_id=(?P<frame_id>\d+)\s+"
    r"page_kind=(?P<page_kind>\w+)\s+"
    r"dirty=(?P<dirty>[01])"
    r"(?:\s+hit=(?P<hit>[01]))?"
)
_COMPOSE_TIMESTAMP_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)"
)
_BENCHBASE_TIMESTAMP_PATTERN = re.compile(
    r"\[INFO \]\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})"
)


def _parse_compose_timestamp(line):
    match = _COMPOSE_TIMESTAMP_PATTERN.search(line)
    if not match:
        return pd.NaT
    return pd.to_datetime(match.group(1), utc=True)


def _parse_benchbase_timestamp(line):
    match = _BENCHBASE_TIMESTAMP_PATTERN.search(line)
    if not match:
        return pd.NaT
    return pd.to_datetime(match.group(1).replace(",", "."), utc=True)


def _find_benchbase_execute_interval(result_dir):
    log_paths = [
        result_dir / "benchbase_execute.log",
        result_dir / "benchbase.log",
    ]
    execute_log_path = next((path for path in log_paths if path.exists()), None)
    if execute_log_path is None:
        return None, None

    start_at = None
    end_at = None
    with execute_log_path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            timestamp = _parse_benchbase_timestamp(line)
            if pd.isna(timestamp):
                continue
            if "MEASURE" in line and start_at is None:
                start_at = timestamp
            if "TERMINATE" in line:
                end_at = timestamp
                break
    return start_at, end_at


def _find_buffer_pool_log_paths(result_dir):
    candidates = [
        result_dir / "dbfs.compose.log",
        result_dir / "benchbase_execute.log",
        result_dir / "benchbase.log",
    ]
    candidates.extend(sorted(result_dir.glob("*.log")))
    seen = set()
    paths = []
    for path in candidates:
        if path in seen or not path.exists():
            continue
        seen.add(path)
        paths.append(path)
    return paths


def _find_event_log_paths(result_dir):
    dedicated_log_path = result_dir / "dbfs.buffer_pool_events.log"
    if dedicated_log_path.exists() and dedicated_log_path.stat().st_size > 0:
        return [dedicated_log_path]
    return _find_buffer_pool_log_paths(result_dir)


def _to_unix_ms(timestamp):
    if timestamp is None or pd.isna(timestamp):
        return None
    return int(timestamp.timestamp() * 1000)


def _parse_event(line, dedicated):
    match = (
        _DEDICATED_EVENT_PATTERN.match(line)
        if dedicated
        else _GENERIC_EVENT_PATTERN.search(line)
    )
    if not match:
        return None

    values = match.groupdict()
    unix_ms_text = values.pop("unix_ms", None)
    if unix_ms_text is not None:
        unix_ms = int(unix_ms_text)
    else:
        timestamp = _parse_compose_timestamp(line)
        if pd.isna(timestamp):
            timestamp = _parse_benchbase_timestamp(line)
        if pd.isna(timestamp):
            return None
        unix_ms = _to_unix_ms(timestamp)

    return {
        "unix_ms": unix_ms,
        "type": values["type"],
        "event_id": int(values["event_id"]),
        "file": values["file"],
        "page_id": int(values["page_id"]),
        "frame_id": int(values["frame_id"]),
        "page_kind": values["page_kind"],
        "dirty": int(values["dirty"]),
        "hit": int(values["hit"]) if values["hit"] is not None else None,
    }


def _cache_signature(log_paths, start_at, end_at):
    return {
        "version": _CACHE_VERSION,
        "sources": [
            {
                "path": str(path.resolve()),
                "size": path.stat().st_size,
                "mtime_ns": path.stat().st_mtime_ns,
            }
            for path in log_paths
        ],
        "execute_start_at": start_at.isoformat() if start_at is not None else None,
        "execute_end_at": end_at.isoformat() if end_at is not None else None,
    }


def _empty_table(name):
    return pd.DataFrame(columns=_CACHE_TABLES[name])


def _read_cache(cache_dir, signature):
    metadata_path = cache_dir / "metadata.json"
    if not metadata_path.exists():
        return None
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("signature") != signature:
            return None

        tables = {}
        for name, columns in _CACHE_TABLES.items():
            path = cache_dir / f"{name}.csv"
            tables[name] = (
                pd.read_csv(path) if path.exists() else pd.DataFrame(columns=columns)
            )
        if not tables["last_events"].empty:
            tables["last_events"]["logged_at"] = pd.to_datetime(
                tables["last_events"]["logged_at"], utc=True
            )
        tables["metadata"] = metadata.get("analysis", {})
        tables["metadata"]["cache_hit"] = True
        return tables
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _write_cache(cache_dir, signature, tables, analysis_metadata):
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        for name in _CACHE_TABLES:
            tables[name].to_csv(cache_dir / f"{name}.csv", index=False)
        metadata = {
            "signature": signature,
            "analysis": analysis_metadata,
        }
        (cache_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2), encoding="utf-8"
        )
    except OSError as error:
        print(f"Warning: could not write buffer-pool event cache: {error}")


def _counter_frame(counter, columns):
    if not counter:
        return pd.DataFrame(columns=[*columns, "events"])
    return pd.DataFrame(
        [(*key, events) for key, events in counter.items()],
        columns=[*columns, "events"],
    )


def load_buffer_pool_event_aggregates(
    result_dir,
    *,
    use_cache=True,
    progress_every_lines=5_000_000,
):
    """Stream event logs and retain only aggregates required by the plots."""
    result_dir = Path(result_dir)
    log_paths = _find_event_log_paths(result_dir)
    start_at, end_at = _find_benchbase_execute_interval(result_dir)
    signature = _cache_signature(log_paths, start_at, end_at)
    cache_dir = result_dir / _CACHE_DIR_NAME

    if use_cache:
        cached = _read_cache(cache_dir, signature)
        if cached is not None:
            print(f"Loading cached buffer pool event aggregates from: {cache_dir}")
            cached["log_paths"] = log_paths
            cached["execute_start_at"] = start_at
            cached["execute_end_at"] = end_at
            return cached

    print(
        "Streaming buffer pool events from: "
        + ", ".join(str(path) for path in log_paths)
    )
    total_source_bytes = sum(path.stat().st_size for path in log_paths)
    start_ms = _to_unix_ms(start_at)
    end_ms = _to_unix_ms(end_at)

    summary_counts = Counter()
    per_second_counts = Counter()
    per_page_counts = Counter()
    per_frame_counts = Counter()
    last_events = deque(maxlen=10)

    scanned_lines = 0
    matched_events = 0
    selected_events = 0
    scanned_bytes = 0
    first_selected_ms = None

    for log_path in log_paths:
        dedicated = log_path.name == "dbfs.buffer_pool_events.log"
        with log_path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                scanned_lines += 1
                scanned_bytes += len(line)
                if (
                    progress_every_lines > 0
                    and scanned_lines % progress_every_lines == 0
                ):
                    progress_pct = (
                        scanned_bytes / total_source_bytes * 100
                        if total_source_bytes
                        else 0
                    )
                    print(
                        f"Processed {scanned_lines:,} lines "
                        f"({progress_pct:.1f}% of input); "
                        f"selected {selected_events:,} events."
                    )
                event = _parse_event(line, dedicated)
                if event is None:
                    continue
                matched_events += 1

                unix_ms = event["unix_ms"]
                if start_ms is not None and unix_ms < start_ms:
                    continue
                if end_ms is not None and unix_ms > end_ms:
                    continue

                selected_events += 1
                if first_selected_ms is None:
                    first_selected_ms = unix_ms
                elapsed_second = (unix_ms - first_selected_ms) // 1000
                page_key = f"{event['file']}:{event['page_id']}"

                summary_counts[
                    (
                        event["type"],
                        event["page_kind"],
                        event["dirty"],
                        event["hit"],
                    )
                ] += 1
                per_second_counts[
                    (elapsed_second, event["type"], event["hit"])
                ] += 1
                per_page_counts[
                    (
                        page_key,
                        event["page_kind"],
                        event["type"],
                        event["hit"],
                    )
                ] += 1
                if event["type"] in {"read", "evict"}:
                    per_frame_counts[(event["frame_id"], event["type"])] += 1

                last_events.append(
                    {
                        "event_id": event["event_id"],
                        "logged_at": unix_ms,
                        "execute_elapsed_seconds": (unix_ms - first_selected_ms)
                        / 1000.0,
                        "type": event["type"],
                        "page_id": event["page_id"],
                        "frame_id": event["frame_id"],
                        "page_kind": event["page_kind"],
                        "dirty": event["dirty"],
                        "hit": event["hit"],
                        "file": event["file"],
                    }
                )

    tables = {
        "summary": _counter_frame(
            summary_counts, ["type", "page_kind", "dirty", "hit"]
        ),
        "per_second": _counter_frame(
            per_second_counts, ["elapsed_second", "type", "hit"]
        ),
        "per_page": _counter_frame(
            per_page_counts, ["page_key", "page_kind", "type", "hit"]
        ),
        "per_frame": _counter_frame(per_frame_counts, ["frame_id", "type"]),
        "last_events": pd.DataFrame(
            list(last_events), columns=_CACHE_TABLES["last_events"]
        ),
    }
    if not tables["last_events"].empty:
        tables["last_events"]["logged_at"] = pd.to_datetime(
            tables["last_events"]["logged_at"], unit="ms", utc=True
        )
    analysis_metadata = {
        "scanned_lines": scanned_lines,
        "matched_events": matched_events,
        "selected_events": selected_events,
        "source_bytes": total_source_bytes,
        "cache_hit": False,
    }
    tables["metadata"] = analysis_metadata
    tables["log_paths"] = log_paths
    tables["execute_start_at"] = start_at
    tables["execute_end_at"] = end_at

    if use_cache:
        _write_cache(cache_dir, signature, tables, analysis_metadata)
    return tables


def _plot_pin_events(tables):
    per_second_df = tables["per_second"]
    per_page_df = tables["per_page"]
    pin_per_second_df = per_second_df[
        (per_second_df["type"] == "pin") & per_second_df["hit"].notna()
    ].copy()
    if pin_per_second_df.empty:
        print("No pin events found.")
        return

    pin_pivot_df = pin_per_second_df.pivot_table(
        index="elapsed_second",
        columns="hit",
        values="events",
        aggfunc="sum",
        fill_value=0,
    ).rename(columns={0: "misses", 1: "hits"})
    for column in ["hits", "misses"]:
        if column not in pin_pivot_df.columns:
            pin_pivot_df[column] = 0
    pin_pivot_df["pin requests"] = pin_pivot_df["hits"] + pin_pivot_df["misses"]
    pin_pivot_df["hit rate (%)"] = (
        pin_pivot_df["hits"] / pin_pivot_df["pin requests"] * 100
    ).where(pin_pivot_df["pin requests"] > 0)

    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    ax_left = axes[0]
    ax_right = ax_left.twinx()
    ax_left.bar(
        pin_pivot_df.index,
        pin_pivot_df["pin requests"],
        color="lightgray",
        label="pin requests",
    )
    ax_left.plot(
        pin_pivot_df.index,
        pin_pivot_df["hits"],
        color="tab:blue",
        linewidth=1.8,
        label="hits",
    )
    ax_left.plot(
        pin_pivot_df.index,
        pin_pivot_df["misses"],
        color="tab:orange",
        linestyle="--",
        linewidth=1.8,
        label="misses",
    )
    ax_right.plot(
        pin_pivot_df.index,
        pin_pivot_df["hit rate (%)"],
        color="black",
        linewidth=1.4,
        label="hit rate (%)",
    )
    ax_left.set_title("Pin Requests and Hit/Miss per Second")
    ax_left.set_xlabel("Elapsed seconds")
    ax_left.set_ylabel("Calls")
    ax_right.set_ylabel("Hit rate (%)")
    ax_left.grid(True, alpha=0.25)
    lines1, labels1 = ax_left.get_legend_handles_labels()
    lines2, labels2 = ax_right.get_legend_handles_labels()
    ax_left.legend(lines1 + lines2, labels1 + labels2, loc="best")

    pin_page_df = per_page_df[
        (per_page_df["type"] == "pin") & per_page_df["hit"].notna()
    ].copy()
    pin_page_df["result"] = pin_page_df["hit"].map({0: "misses", 1: "hits"})
    top_pin_pages_df = pin_page_df.pivot_table(
        index=["page_key", "page_kind"],
        columns="result",
        values="events",
        aggfunc="sum",
        fill_value=0,
    )
    for column in ["hits", "misses"]:
        if column not in top_pin_pages_df.columns:
            top_pin_pages_df[column] = 0
    top_pin_pages_df["pin requests"] = (
        top_pin_pages_df["hits"] + top_pin_pages_df["misses"]
    )
    top_pin_pages_df["hit rate (%)"] = (
        top_pin_pages_df["hits"] / top_pin_pages_df["pin requests"] * 100
    ).where(top_pin_pages_df["pin requests"] > 0)
    top_pin_pages_df = top_pin_pages_df.nlargest(20, "pin requests")
    display(top_pin_pages_df.reset_index())
    top_pin_pages_df[["hits", "misses"]].plot(
        kind="barh",
        stacked=True,
        ax=axes[1],
        color=["tab:blue", "tab:orange"],
    )
    axes[1].set_title("Top Pages by Pin Requests")
    axes[1].set_xlabel("Pin requests")
    axes[1].set_ylabel("Page")
    axes[1].invert_yaxis()
    fig.tight_layout()
    plt.show()


def _plot_read_evict_events(tables):
    per_second_df = tables["per_second"]
    per_page_df = tables["per_page"]
    per_frame_df = tables["per_frame"]
    read_evict_second_df = per_second_df[
        per_second_df["type"].isin(["read", "evict"])
    ].copy()
    if read_evict_second_df.empty:
        return

    second_pivot_df = read_evict_second_df.pivot_table(
        index="elapsed_second",
        columns="type",
        values="events",
        aggfunc="sum",
        fill_value=0,
    )
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    event_style_map = {
        "read": {
            "color": "tab:blue",
            "linestyle": "--",
            "marker": "o",
            "markersize": 3,
        },
        "evict": {
            "color": "tab:orange",
            "linestyle": "-",
            "marker": "o",
            "markersize": 3,
        },
    }
    for column in second_pivot_df.columns:
        axes[0].plot(
            second_pivot_df.index,
            second_pivot_df[column],
            label=column,
            linewidth=1.8,
            **event_style_map.get(column, {}),
        )
    axes[0].set_title("Read/Evict Events per Second")
    axes[0].set_xlabel("Elapsed seconds")
    axes[0].set_ylabel("Events")
    axes[0].grid(True, alpha=0.25)
    axes[0].legend(loc="best", frameon=True)

    read_evict_page_df = per_page_df[
        per_page_df["type"].isin(["read", "evict"])
    ].copy()
    page_pivot_df = read_evict_page_df.pivot_table(
        index="page_key",
        columns="type",
        values="events",
        aggfunc="sum",
        fill_value=0,
    )
    page_pivot_df["total"] = page_pivot_df.sum(axis=1)
    top_page_pivot_df = page_pivot_df.nlargest(20, "total").drop(
        columns="total"
    )
    top_page_pivot_df.plot(kind="barh", ax=axes[1])
    axes[1].set_title("Top Pages by Read/Evict Events")
    axes[1].set_xlabel("Events")
    axes[1].set_ylabel("Page")
    axes[1].invert_yaxis()
    fig.tight_layout()
    plt.show()

    frame_pivot_df = per_frame_df.pivot_table(
        index="frame_id",
        columns="type",
        values="events",
        aggfunc="sum",
        fill_value=0,
    )
    for column in ["read", "evict"]:
        if column not in frame_pivot_df.columns:
            frame_pivot_df[column] = 0
    frame_pivot_df["total"] = frame_pivot_df["read"] + frame_pivot_df["evict"]
    display(frame_pivot_df.nlargest(20, "total").reset_index())

    fig, ax = plt.subplots(figsize=(12, 6))
    frame_pivot_df["total"].plot(
        kind="hist",
        bins=50,
        ax=ax,
        color="tab:purple",
        alpha=0.75,
    )
    ax.set_title("Frame Count by Page Replacement Frequency")
    ax.set_xlabel("Page replacements per frame")
    ax.set_ylabel("Frames")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    plt.show()


def analyze_buffer_pool_events(result_dir, *, use_cache=True):
    """Load aggregates, render the existing figures, and return the tables."""
    tables = load_buffer_pool_event_aggregates(result_dir, use_cache=use_cache)
    metadata = tables["metadata"]
    if metadata.get("cache_hit"):
        print(
            f"Using cached aggregates for "
            f"{metadata.get('selected_events', 0):,} execute-phase events."
        )
    else:
        print(
            f"Selected {metadata.get('selected_events', 0):,} of "
            f"{metadata.get('matched_events', 0):,} parsed events."
        )

    summary_df = tables["summary"]
    if summary_df.empty:
        print(
            "No buffer_pool_event rows found. Run dbfs with "
            "DBFS_BUFFER_POOL_EVENT_LOG=1 and DBFS_STORAGE_LOG_LEVEL=info."
        )
        return tables

    start_at = tables["execute_start_at"]
    end_at = tables["execute_end_at"]
    if start_at is not None and end_at is not None:
        print(f"Using BenchBase execute interval: {start_at} to {end_at}.")
    display(tables["last_events"])
    display(summary_df.sort_values("events", ascending=False))
    _plot_pin_events(tables)
    _plot_read_evict_events(tables)
    return tables
