#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import html
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DUCKDB = ROOT / "build" / "release" / "duckdb"
REPORT = ROOT / "docs" / "BENCHMARK_READ_REPORT.html"
REMOTE_REPORT = ROOT / "docs" / "BENCHMARK_READ_REMOTE_REPORT.html"
CACHE_ROOT = ROOT / ".cache" / "object-storage-benchmark"


def command(args, *, input_text=None, check=True, env=None):
    return subprocess.run(
        [str(arg) for arg in args],
        cwd=ROOT,
        env=env,
        input=input_text,
        text=True,
        capture_output=True,
        check=check,
    )


def build():
    env = os.environ.copy()
    env["CORE_EXTENSIONS"] = "tpch"
    print("Building DuckDB v1.5.5 with TPC-H...", flush=True)
    subprocess.run(["make"], cwd=ROOT, env=env, check=True)


def duckdb(sql, database=None, env=None):
    args = [DUCKDB, "-batch", "-bail"]
    if database:
        args.append(database)
    return command(args, input_text=sql, check=False, env=env)


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def parse_profiles(stderr):
    decoder = json.JSONDecoder()
    profiles = []
    offset = 0
    while True:
        start = stderr.find("{", offset)
        if start < 0:
            break
        try:
            value, offset = decoder.raw_decode(stderr, start)
        except json.JSONDecodeError:
            offset = start + 1
            continue
        if isinstance(value, dict) and "latency" in value and "query_name" in value:
            profiles.append(value)
    return profiles


def profile_summary(profile):
    return {
        "query": profile["query_name"],
        "latency_seconds": profile["latency"],
        "cpu_seconds": profile.get("cpu_time"),
        "peak_buffer_bytes": profile.get("system_peak_buffer_memory"),
        "bytes_read": profile.get("total_bytes_read"),
        "bytes_written": profile.get("total_bytes_written"),
    }


def save_process(run_dir, name, process):
    (run_dir / f"{name}.stdout.log").write_text(process.stdout, encoding="utf-8")
    (run_dir / f"{name}.stderr.log").write_text(process.stderr, encoding="utf-8")


def read_stats(path):
    integer_fields = {
        "hit_count",
        "miss_count",
        "entry_count",
        "size_bytes",
        "eviction_count",
        "evicted_bytes",
        "request_count",
        "bytes",
    }
    float_fields = {"hit_rate", "average_latency_ms", "stddev_latency_ms"}
    with path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    for row in rows:
        for field in integer_fields & row.keys():
            row[field] = int(row[field]) if row[field] not in {"", "NULL"} else None
        for field in float_fields & row.keys():
            row[field] = float(row[field]) if row[field] not in {"", "NULL"} else None
    return rows


def stats_sql(run_dir, name):
    cache_path = run_dir / f"{name}.cache.csv"
    io_path = run_dir / f"{name}.io.csv"
    return f"""
.mode csv
.headers on
.output {quote(cache_path)}
SELECT * FROM duckdb_objfs_cache_stats() ORDER BY cache;
.output {quote(io_path)}
SELECT * FROM duckdb_objfs_io_stats() ORDER BY operation;
.output stdout
""", cache_path, io_path


def profile_sql(queries, warmups, runs):
    statements = [".output /dev/null", "PRAGMA enable_profiling='json';"]
    for query in queries:
        statements.extend([f"PRAGMA tpch({query});"] * (warmups + runs))
    statements.append("PRAGMA disable_profiling;")
    return "\n".join(statements)


def objfs_sql(backend, root=None, read_only=False, bucket=None):
    statements = [
        "LOAD tpch;",
        "LOAD duckdb_object_storage;",
        f"SET duckdb_objfs_backend = {quote(backend)};",
    ]
    if root:
        statements.append(f"SET duckdb_objfs_root = {quote(root)};")
    if bucket:
        statements.append(f"SET duckdb_objfs_bucket = {quote(bucket)};")
    attach = "ATTACH 'duckdb_objfs://tpch.db' AS bench"
    if read_only:
        attach += " (READ_ONLY)"
    statements.extend([attach + ";", "USE bench;"])
    return "\n".join(statements)


def aws_environment(profile, region):
    process = command(["aws", "configure", "export-credentials", "--profile", profile, "--format", "process"])
    credentials = json.loads(process.stdout)
    env = os.environ.copy()
    env.update(
        {
            "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "AWS_SESSION_TOKEN": credentials["SessionToken"],
            "AWS_REGION": region,
            "AWS_DEFAULT_REGION": region,
        }
    )
    return env


def s3_secret_sql(bucket, root, region):
    scope = f"s3://{bucket}/{root}"
    return f"""
LOAD httpfs;
CREATE OR REPLACE SECRET benchmark_s3 (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID getenv('AWS_ACCESS_KEY_ID'),
    SECRET getenv('AWS_SECRET_ACCESS_KEY'),
    SESSION_TOKEN getenv('AWS_SESSION_TOKEN'),
    REGION {quote(region)},
    USE_SSL true,
    SCOPE {quote(scope)}
);
"""


def objfs_s3_sql(bucket, root, region, read_only=False):
    return f"""
{s3_secret_sql(bucket, root, region)}
{objfs_sql("s3", root, read_only, bucket)}
"""


def cardinality_sql(orders, lineitem):
    return f"""
SELECT CASE
    WHEN (SELECT count(*) FROM orders) = {orders}
     AND (SELECT count(*) FROM lineitem) = {lineitem}
    THEN true
    ELSE error('unexpected TPC-H cardinality')
END;
"""


def split_samples(profiles, queries, warmups, runs):
    expected = len(queries) * (warmups + runs)
    if len(profiles) != expected:
        raise RuntimeError(f"expected {expected} query profiles, found {len(profiles)}")
    samples = {}
    offset = 0
    for query in queries:
        group = profiles[offset : offset + warmups + runs]
        samples[str(query)] = {
            "warmup": [profile_summary(value) for value in group[:warmups]],
            "measured": [profile_summary(value) for value in group[warmups:]],
        }
        offset += warmups + runs
    return samples


def run_process(run_dir, name, sql, database=None, env=None):
    process = duckdb(sql, database, env)
    save_process(run_dir, name, process)
    if process.returncode != 0:
        message = process.stderr.strip().splitlines()
        raise RuntimeError(message[-1] if message else f"DuckDB exited with {process.returncode}")
    return parse_profiles(process.stderr)


def run_memory_case(case, run_dir, scale_factor, queries, warmups, runs, orders, lineitem):
    if case == "native_memory":
        setup = "LOAD tpch;"
        load = f"CALL dbgen(sf = {scale_factor});\nCHECKPOINT;"
    else:
        setup = objfs_sql("memory")
        load = f"CALL dbgen(sf = {scale_factor}, catalog = 'bench');\nCHECKPOINT bench;"
    sql = f"""
{setup}
PRAGMA enable_profiling='json';
{load}
PRAGMA disable_profiling;
{cardinality_sql(orders, lineitem)}
{profile_sql(queries, warmups, runs)}
"""
    profiles = run_process(run_dir, case, sql)
    query_count = len(queries) * (warmups + runs)
    return {
        "status": "complete",
        "setup": [profile_summary(value) for value in profiles[:-query_count]],
        "samples": split_samples(profiles[-query_count:], queries, warmups, runs),
        "storage_bytes": None,
    }


def directory_size(path):
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def run_local_case(case, run_dir, scale_factor, queries, warmups, runs, orders, lineitem):
    if case == "native_local":
        database = run_dir / "native.duckdb"
        setup_sql = f"""
LOAD tpch;
PRAGMA enable_profiling='json';
CALL dbgen(sf = {scale_factor});
CHECKPOINT;
PRAGMA disable_profiling;
"""
        query_setup = "LOAD tpch;"
        query_database = database
    else:
        database = None
        objfs_root = run_dir / "objfs-local"
        objfs_root.mkdir()
        setup_sql = f"""
{objfs_sql("local", objfs_root)}
PRAGMA enable_profiling='json';
CALL dbgen(sf = {scale_factor}, catalog = 'bench');
CHECKPOINT bench;
PRAGMA disable_profiling;
"""
        query_setup = objfs_sql("local", objfs_root, read_only=True)
        query_database = None

    setup_profiles = run_process(run_dir, f"{case}-setup", setup_sql, database)
    query_sql = f"""
{query_setup}
{cardinality_sql(orders, lineitem)}
{profile_sql(queries, warmups, runs)}
"""
    query_profiles = run_process(run_dir, f"{case}-queries", query_sql, query_database)
    storage_bytes = database.stat().st_size if database else directory_size(objfs_root)
    return {
        "status": "complete",
        "setup": [profile_summary(value) for value in setup_profiles],
        "samples": split_samples(query_profiles, queries, warmups, runs),
        "storage_bytes": storage_bytes,
    }


def run_remote_queries(case, run_dir, query_setup, queries, runs, profile, region, persistent_cache_root=None):
    samples = {}
    for query in queries:
        measured = []
        for run in range(1, runs + 1):
            name = f"{case}-q{query:02d}-run{run}"
            stats, cache_stats_path, io_path = stats_sql(run_dir, name) if case == "objfs_s3" else ("", None, None)
            sample_setup = query_setup
            persistent_path = None
            if persistent_cache_root and case == "objfs_s3":
                persistent_path = persistent_cache_root / name
                persistent_path.mkdir()
                sample_setup = f"""
LOAD duckdb_object_storage;
SET duckdb_objfs_persistent_cache_path = {quote(persistent_path)};
{query_setup}
"""
            sql = f"""
{sample_setup}
{profile_sql([query], 0, 1)}
{stats}
"""
            profiles = run_process(
                run_dir,
                name,
                sql,
                env=aws_environment(profile, region),
            )
            sample = profile_summary(profiles[0])
            if cache_stats_path:
                sample["cache_stats"] = read_stats(cache_stats_path)
                sample["io_stats"] = read_stats(io_path)
            if persistent_path:
                shutil.rmtree(persistent_path)
            measured.append(sample)
            print(f"{case} Q{query:02d} run {run}/{runs} complete", flush=True)
        samples[str(query)] = {"warmup": [], "measured": measured}
    return samples


def s3_prefix_size(bucket, prefix, profile, region):
    process = command(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            prefix,
            "--profile",
            profile,
            "--region",
            region,
            "--query",
            "Contents[].Size",
            "--output",
            "json",
        ]
    )
    return sum(json.loads(process.stdout) or [])


def run_remote_case(
    case,
    run_dir,
    scale_factor,
    queries,
    runs,
    orders,
    lineitem,
    bucket,
    prefix,
    profile,
    region,
    seed_run=None,
    reuse_remote_data=False,
    persistent_cache_root=None,
):
    if case == "native_s3":
        database = seed_run / "native.duckdb" if seed_run else run_dir / "native-s3.duckdb"
        if seed_run or reuse_remote_data:
            setup_profiles = []
        else:
            setup_sql = f"""
LOAD tpch;
PRAGMA enable_profiling='json';
CALL dbgen(sf = {scale_factor});
CHECKPOINT;
PRAGMA disable_profiling;
{cardinality_sql(orders, lineitem)}
"""
            setup_profiles = run_process(run_dir, f"{case}-setup", setup_sql, database)
        remote_uri = f"s3://{bucket}/{prefix}/native/tpch.duckdb"
        if not reuse_remote_data:
            upload = command(
                [
                    "aws",
                    "s3",
                    "cp",
                    database,
                    remote_uri,
                    "--only-show-errors",
                    "--profile",
                    profile,
                    "--region",
                    region,
                ],
                check=False,
            )
            save_process(run_dir, f"{case}-upload", upload)
            if upload.returncode != 0:
                raise RuntimeError(upload.stderr.strip() or "native S3 upload failed")
        query_setup = f"""
{s3_secret_sql(bucket, prefix, region)}
SET enable_external_file_cache = true;
LOAD tpch;
ATTACH {quote(remote_uri)} AS bench (READ_ONLY);
USE bench;
"""
        storage_bytes = (
            s3_prefix_size(bucket, f"{prefix}/native", profile, region)
            if reuse_remote_data
            else database.stat().st_size
        )
    else:
        root = f"{prefix}/objfs"
        if reuse_remote_data:
            setup_profiles = []
        elif seed_run:
            upload = command(
                [
                    "aws",
                    "s3",
                    "cp",
                    seed_run / "objfs-local",
                    f"s3://{bucket}/{root}",
                    "--recursive",
                    "--only-show-errors",
                    "--profile",
                    profile,
                    "--region",
                    region,
                ],
                check=False,
            )
            save_process(run_dir, f"{case}-upload", upload)
            if upload.returncode != 0:
                raise RuntimeError(upload.stderr.strip() or "ObjFS S3 upload failed")
            setup_profiles = []
        else:
            setup_sql = f"""
{objfs_s3_sql(bucket, root, region)}
PRAGMA enable_profiling='json';
CALL dbgen(sf = {scale_factor}, catalog = 'bench');
CHECKPOINT bench;
PRAGMA disable_profiling;
{cardinality_sql(orders, lineitem)}
"""
            setup_profiles = run_process(
                run_dir,
                f"{case}-setup",
                setup_sql,
                env=aws_environment(profile, region),
            )
        query_setup = objfs_s3_sql(bucket, root, region, read_only=True)
        storage_bytes = s3_prefix_size(bucket, root, profile, region)

    samples = run_remote_queries(
        case,
        run_dir,
        query_setup,
        queries,
        runs,
        profile,
        region,
        persistent_cache_root,
    )
    return {
        "status": "complete",
        "setup": [profile_summary(value) for value in setup_profiles],
        "samples": samples,
        "storage_bytes": storage_bytes,
    }


def latency_stats(case, query, metric="latency_seconds"):
    measured = case["samples"][str(query)]["measured"]
    values = [sample[metric] for sample in measured]
    deviation = statistics.stdev(values) if len(values) > 1 else 0
    return statistics.median(values), deviation


def format_seconds(value):
    if value is None:
        return "—"
    if value < 1:
        return f"{value * 1000:.1f} ms"
    return f"{value:.3f} s"


def format_timing(median, deviation):
    if median < 1:
        return f"{median * 1000:.1f} ± {deviation * 1000:.1f} ms"
    return f"{median:.3f} ± {deviation:.3f} s"


def format_bytes(value):
    if value is None:
        return "—"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return "—"


def axis_timing(milliseconds):
    if milliseconds >= 1000:
        return f"{milliseconds / 1000:g} s"
    return f"{milliseconds:g} ms"


def latency_chart(timings, title, metric_label="Query latency"):
    width, height = 1000, 430
    left, right, top, bottom = 80, 30, 45, 65
    plot_width = width - left - right
    plot_height = height - top - bottom
    bounds = []
    for _, native_median, native_deviation, objfs_median, objfs_deviation, _ in timings:
        for median, deviation in ((native_median, native_deviation), (objfs_median, objfs_deviation)):
            bounds.extend((max((median - deviation) * 1000, 0.001), (median + deviation) * 1000))
    log_min, log_max = math.log10(min(bounds)), math.log10(max(bounds))
    padding = max((log_max - log_min) * 0.08, 0.08)
    log_min -= padding
    log_max += padding

    group_width = plot_width / len(timings)

    def x_position(index):
        return left + (index + 0.5) * group_width

    def y_position(seconds):
        value = math.log10(max(seconds * 1000, 0.001))
        return top + (log_max - value) * plot_height / (log_max - log_min)

    ticks = [10**power for power in range(math.floor(log_min), math.ceil(log_max) + 1)]
    grid = []
    for power in range(math.floor(log_min), math.ceil(log_max)):
        for multiple in range(2, 10):
            tick = multiple * 10**power
            y = top + (log_max - math.log10(tick)) * plot_height / (log_max - log_min)
            if top <= y <= top + plot_height:
                grid.append(
                    f"<line class='grid-line minor' x1='{left}' y1='{y:.1f}' x2='{left + plot_width}' y2='{y:.1f}'/>"
                )
    for tick in ticks:
        y = top + (log_max - math.log10(tick)) * plot_height / (log_max - log_min)
        if top <= y <= top + plot_height:
            grid.append(
                f"<line class='grid-line' x1='{left}' y1='{y:.1f}' x2='{left + plot_width}' y2='{y:.1f}'/>"
                f"<text class='axis-text' x='{left - 12}' y='{y + 4:.1f}' text-anchor='end'>{axis_timing(tick)}</text>"
            )

    bar_width = min(15, group_width * 0.34)
    baseline = top + plot_height
    bars = []
    ratios = []
    for index, (query, native_median, native_deviation, objfs_median, objfs_deviation, ratio) in enumerate(timings):
        center = x_position(index)
        highest = baseline
        for name, median, deviation, css_class, x in (
            ("Native", native_median, native_deviation, "series-native", center - bar_width - 1.5),
            ("ObjFS", objfs_median, objfs_deviation, "series-objfs", center + 1.5),
        ):
            y = y_position(median)
            low = y_position(max(median - deviation, 0.000001))
            high = y_position(median + deviation)
            midpoint = x + bar_width / 2
            highest = min(highest, high)
            value = format_timing(median, deviation)
            error_bar = (
                f"<line class='error-bar' x1='{midpoint:.1f}' y1='{high:.1f}' x2='{midpoint:.1f}' y2='{low:.1f}'/>"
                f"<line class='error-bar' x1='{midpoint - 3:.1f}' y1='{high:.1f}' x2='{midpoint + 3:.1f}' y2='{high:.1f}'/>"
                f"<line class='error-bar' x1='{midpoint - 3:.1f}' y1='{low:.1f}' x2='{midpoint + 3:.1f}' y2='{low:.1f}'/>"
            )
            bars.append(
                f"<g class='{css_class}'><title>Q{query:02d} {name}: {value}</title>"
                f"<rect class='data-bar' x='{x:.1f}' y='{y:.1f}' width='{bar_width:.1f}' height='{baseline - y:.1f}'/>"
                f"{error_bar}"
                "</g>"
            )
        ratios.append(
            f"<text class='ratio-label' x='{center:.1f}' y='{max(top + 12, highest - 7):.1f}' text-anchor='middle'>{ratio:.2f}×</text>"
        )

    x_labels = "".join(
        f"<text class='axis-text' x='{x_position(index):.1f}' y='{top + plot_height + 24}' text-anchor='middle'>Q{query:02d}</text>"
        for index, (query, *_rest) in enumerate(timings)
    )
    chart_id = title.lower().replace(" ", "-")
    return f"""
<div class="chart-wrap">
<svg class="latency-chart" viewBox="0 0 {width} {height}" role="img" aria-labelledby="{chart_id}-title {chart_id}-desc">
  <title id="{chart_id}-title">{html.escape(title)}</title>
  <desc id="{chart_id}-desc">Median {html.escape(metric_label.lower())} with sample standard deviation error bars for Native DuckDB and ObjFS.</desc>
  <rect class="plot-frame" x="{left}" y="{top}" width="{plot_width}" height="{plot_height}"/>
  {''.join(grid)}
  {''.join(bars)}
  {''.join(ratios)}
  {x_labels}
  <text class="axis-title" x="{left + plot_width / 2}" y="{height - 12}" text-anchor="middle">TPC-H query</text>
  <text class="axis-title" x="18" y="{top + plot_height / 2}" text-anchor="middle" transform="rotate(-90 18 {top + plot_height / 2})">{html.escape(metric_label)} (ms, log scale)</text>
  <g class="legend" transform="translate({width - 230} 18)">
    <rect class="series-native" x="0" y="-7" width="18" height="12"/><text class="axis-text" x="26" y="4">Native</text>
    <rect class="series-objfs" x="102" y="-7" width="18" height="12"/><text class="axis-text" x="128" y="4">ObjFS</text>
  </g>
</svg>
</div>
"""


def comparison(
    data,
    title,
    native_name,
    objfs_name,
    queries,
    metric="latency_seconds",
    metric_label="Query latency",
    caption=None,
):
    native = data["cases"].get(native_name, {})
    objfs = data["cases"].get(objfs_name, {})
    if native.get("status") != "complete" or objfs.get("status") != "complete":
        return f"<section><h2>{html.escape(title)}</h2><p>Comparison unavailable because one or both cases failed.</p></section>"

    rows = []
    timings = []
    for query in queries:
        native_median, native_deviation = latency_stats(native, query, metric)
        objfs_median, objfs_deviation = latency_stats(objfs, query, metric)
        ratio = objfs_median / native_median
        timings.append((query, native_median, native_deviation, objfs_median, objfs_deviation, ratio))
    for query, native_median, native_deviation, objfs_median, objfs_deviation, ratio in timings:
        native_value = format_timing(native_median, native_deviation)
        objfs_value = format_timing(objfs_median, objfs_deviation)
        rows.append(
            "<tr>"
            f"<td>Q{query:02d}</td>"
            f"<td>{native_value}</td>"
            f"<td>{objfs_value}</td>"
            f"<td><strong>{ratio:.2f}×</strong></td>"
            "</tr>"
        )
    return f"""
<section>
  <h2>{html.escape(title)}</h2>
  <p class="caption">{html.escape(caption or 'Bars show medians; whiskers show ± one sample standard deviation. Labels show the ObjFS/native median ratio.')}</p>
  {latency_chart(timings, title, metric_label)}
  <h3>Detailed results</h3>
  <table>
    <thead><tr><th>Query</th><th>Native median ± SD</th><th>ObjFS median ± SD</th><th>Ratio of medians</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</section>
"""


def cache_summary(data, queries):
    case = data["cases"].get("objfs_s3", {})
    if case.get("status") != "complete":
        return ""

    caches = {}
    operations = {}
    query_rows = []
    sample_count = 0
    for query in queries:
        query_caches = {}
        query_operations = {}
        samples = case["samples"][str(query)]["measured"]
        for sample in samples:
            if "cache_stats" not in sample or "io_stats" not in sample:
                return ""
            sample_count += 1
            for row in sample["cache_stats"]:
                for target in (caches, query_caches):
                    totals = target.setdefault(row["cache"], [0, 0])
                    totals[0] += row["hit_count"]
                    totals[1] += row["miss_count"]
            for row in sample["io_stats"]:
                count = row["request_count"]
                for target in (operations, query_operations):
                    totals = target.setdefault(row["operation"], [0, 0.0, 0])
                    totals[0] += count
                    totals[1] += count * (row["average_latency_ms"] or 0)
                    if totals[2] is not None:
                        payload_bytes = row.get("bytes")
                        totals[2] = totals[2] + payload_bytes if payload_bytes is not None else None

        def cache_cell(name):
            hits, misses = query_caches.get(name, (0, 0))
            accesses = hits + misses
            rate = f"{hits / accesses:.1%}" if accesses else "—"
            return f"<strong class='metric'>{rate}</strong><small>{hits:,} hits · {misses:,} misses</small>"

        def requests_per_run(name):
            return f"{query_operations.get(name, (0, 0))[0] / len(samples):.1f}"

        read_count, read_latency, read_bytes = query_operations.get("read", (0, 0.0, None))
        average_read_latency = f"{read_latency / read_count:.1f} ms" if read_count else "—"
        read_bytes_per_run = format_bytes(read_bytes / len(samples)) if read_bytes is not None else "—"
        other_operations = (
            f"{requests_per_run('stat')} stat · {requests_per_run('list')} list · "
            f"{requests_per_run('write')} write · {requests_per_run('delete')} delete"
        )
        query_rows.append(
            f"<tr><td>Q{query:02d}</td><td>{cache_cell('memory_data')}</td>"
            f"<td>{cache_cell('memory_metadata')}</td><td>{cache_cell('persistent')}</td>"
            f"<td><strong class='metric'>{requests_per_run('read')} reads</strong>"
            f"<small>{other_operations}</small></td>"
            f"<td>{average_read_latency}</td><td>{read_bytes_per_run}</td></tr>"
        )

    cache_rows = []
    for name, label in (
        ("memory_data", "Memory data"),
        ("memory_metadata", "Memory metadata"),
        ("persistent", "Persistent"),
    ):
        hits, misses = caches.get(name, (0, 0))
        accesses = hits + misses
        rate = f"{hits / accesses:.1%}" if accesses else "—"
        cache_rows.append(
            f"<tr><td>{label}</td><td>{hits:,}</td><td>{misses:,}</td><td>{accesses:,}</td><td>{rate}</td></tr>"
        )

    operation_rows = []
    for name in ("read", "stat", "list", "write", "delete"):
        count, total_latency, total_bytes = operations.get(name, (0, 0.0, None))
        average = f"{total_latency / count:.1f} ms" if count else "—"
        size = format_bytes(total_bytes) if name in ("read", "write") else "—"
        operation_rows.append(f"<tr><td>{name}</td><td>{count:,}</td><td>{size}</td><td>{average}</td></tr>")

    persistent_enabled = data["config"].get("persistent_cache", False)
    persistent_note = (
        "Persistent cache is enabled with a new empty local-disk directory for every process; the directory is removed after statistics are collected."
        if persistent_enabled
        else "Persistent cache is disabled for this benchmark."
    )
    return f"""
<section>
  <h2>ObjFS cache and I/O statistics</h2>
  <p class="caption">Aggregated across {sample_count} independent ObjFS query processes. Counters include database attachment and query execution; reported benchmark latency is query-only. Each process starts with empty memory caches.</p>
  <h3>Cache</h3>
  <p class="caption">Memory caches last for one process. Persistent is an optional local-disk SST cache. {persistent_note}</p>
  <table>
    <thead><tr><th>Cache</th><th>Hits</th><th>Misses</th><th>Accesses</th><th>Hit rate</th></tr></thead>
    <tbody>{''.join(cache_rows)}</tbody>
  </table>
  <h3>OpenDAL operations</h3>
  <p class="caption">Counts and bytes are totals; bytes are OpenDAL payload, not S3 wire traffic. Average latency is weighted by request count.</p>
  <table>
    <thead><tr><th>Operation</th><th>Requests</th><th>Bytes</th><th>Average latency</th></tr></thead>
    <tbody>{''.join(operation_rows)}</tbody>
  </table>
  <h3>Per-query statistics</h3>
  <p class="caption">Hit rate is hits / (hits + misses), using totals from {data['config']['runs']} runs. Data, metadata, and persistent caches count different lookup types, so their totals are not expected to match. OpenDAL operations are averages per process.</p>
  <div class="table-wrap"><table>
    <thead><tr><th>Query</th><th>Data-block cache</th><th>Metadata cache</th><th>Persistent SST cache</th><th>OpenDAL requests/run</th><th>Avg read latency</th><th>Read bytes/run</th></tr></thead>
    <tbody>{''.join(query_rows)}</tbody>
  </table></div>
</section>
"""


def comparison_cases(data):
    if data["config"].get("remote"):
        return [
            (
                "remote_s3",
                "Remote S3 comparison",
                "native_s3",
                "objfs_s3",
                "latency_seconds",
            ),
        ]
    return [
        ("memory", "In-memory comparison", "native_memory", "objfs_memory", "latency_seconds"),
        ("local", "Local filesystem comparison", "native_local", "objfs_local", "latency_seconds"),
    ]


def write_summary_csv(data, path):
    fields = [
        "comparison",
        "query",
        "metric",
        "native_median_seconds",
        "native_sample_stddev_seconds",
        "objfs_median_seconds",
        "objfs_sample_stddev_seconds",
        "objfs_native_ratio",
    ]
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for name, _title, native_name, objfs_name, metric in comparison_cases(data):
            native = data["cases"].get(native_name, {})
            objfs = data["cases"].get(objfs_name, {})
            if native.get("status") != "complete" or objfs.get("status") != "complete":
                continue
            for query in data["config"]["queries"]:
                native_median, native_deviation = latency_stats(native, query, metric)
                objfs_median, objfs_deviation = latency_stats(objfs, query, metric)
                writer.writerow(
                    {
                        "comparison": name,
                        "query": query,
                        "metric": metric,
                        "native_median_seconds": native_median,
                        "native_sample_stddev_seconds": native_deviation,
                        "objfs_median_seconds": objfs_median,
                        "objfs_sample_stddev_seconds": objfs_deviation,
                        "objfs_native_ratio": objfs_median / native_median,
                    }
                )


def render_report(data):
    config = data["config"]
    cases = data["cases"]
    case_rows = []
    case_names = (
        ("native_s3", "objfs_s3")
        if config.get("remote")
        else ("native_memory", "objfs_memory", "native_local", "objfs_local")
    )
    for name in case_names:
        case = cases.get(name, {"status": "not run"})
        setup = sum(item["latency_seconds"] for item in case.get("setup", []))
        error = html.escape(case.get("error", ""))
        setup_cell = (
            "" if config.get("remote") else f"<td>{format_seconds(setup) if case.get('setup') else '—'}</td>"
        )
        case_rows.append(
            f"<tr><td>{name.replace('_', ' ')}</td><td>{case['status']}</td>"
            f"{setup_cell}<td>{format_bytes(case.get('storage_bytes'))}</td><td>{error}</td></tr>"
        )

    case_heading = "Case status" if config.get("remote") else "Case status and setup"
    setup_header = "" if config.get("remote") else "<th>dbgen + checkpoint</th>"

    metadata_rows = "".join(
        f"<tr><th>{html.escape(str(key).replace('_', ' '))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in data["metadata"].items()
    )
    query_list = config["queries"]
    query_description = f"Q{query_list[0]}" if config.get("smoke") else "Q1–Q22"
    comparisons = ""
    for name, title, native_name, objfs_name, metric in comparison_cases(data):
        comparisons += comparison(
            data,
            title,
            native_name,
            objfs_name,
            query_list,
            metric,
            "Query latency",
        )
    if config.get("remote"):
        comparisons += cache_summary(data, query_list)
    generated = html.escape(data["metadata"]["generated_at"])
    seed_note = ""
    if config.get("seed_run"):
        seed_note = (
            "<p>The remote databases were seeded from the completed local run at "
            f"<code>{html.escape(config['seed_run'])}</code>. Upload and local data generation are excluded "
            "from query latency.</p>"
        )
    if config.get("remote"):
        cache_note = (
            "ObjFS enables memory and persistent caches, but every measured process receives a new empty persistent-cache directory."
            if config.get("persistent_cache")
            else "ObjFS enables process-local memory caches; persistent cache is disabled."
        )
        execution_note = (
            "Native DuckDB reads one database file from S3 through HTTPFS; ObjFS reads SlateDB objects from S3 "
            f"through OpenDAL. Each query runs {config['runs']} times in a new DuckDB process with no warm-up or "
            f"shared cache. {cache_note}"
        )
        measurement_note = (
            "Charts and tables report DuckDB query-only latency as "
            "<strong>median ± sample standard deviation</strong>."
        )
        single_value_note = "Storage-size values are single observations and therefore have no deviation."
    else:
        execution_note = (
            f"Each query receives {config['warmups']} unmeasured warm-up and "
            f"{config['runs']} measured executions."
        )
        measurement_note = (
            f"Measured query latency is reported as <strong>median ± sample standard deviation</strong> over the "
            f"{config['runs']} measured executions."
        )
        single_value_note = (
            "One-time setup and storage-size values are single observations and therefore have no deviation."
        )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DuckDB Object Storage Benchmark</title>
<style>
:root {{
  color-scheme: light dark;
  font-family: ui-sans-serif, system-ui, sans-serif;
  --native: #64748b;
  --objfs: #0f766e;
  --grid: #94a3b8;
}}
body {{ max-width: 1100px; margin: 0 auto; padding: 2rem; line-height: 1.45; }}
h1 {{ margin-bottom: .2rem; }} h2 {{ margin-top: 2.5rem; }}
.muted, small, .caption {{ color: #666; }}
table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; }}
th, td {{ text-align: left; border-bottom: 1px solid #9995; padding: .55rem; vertical-align: top; }}
.metric {{ display: block; white-space: nowrap; }}
td small {{ display: block; margin-top: .15rem; white-space: nowrap; }}
.table-wrap {{ overflow-x: auto; }}
.table-wrap table {{ min-width: 900px; }}
.chart-wrap {{ width: 100%; overflow-x: auto; }}
.latency-chart {{ display: block; width: 100%; min-width: 760px; color: inherit; }}
.plot-frame {{ fill: none; stroke: var(--grid); stroke-width: 1; }}
.grid-line {{ stroke: var(--grid); stroke-width: 1; opacity: .35; }}
.grid-line.minor {{ opacity: .16; }}
.axis-text {{ fill: currentColor; font-size: 12px; }}
.axis-title {{ fill: currentColor; font-size: 13px; font-weight: 500; }}
.series-native {{ fill: var(--native); stroke: var(--native); }}
.series-objfs {{ fill: var(--objfs); stroke: var(--objfs); }}
.data-bar {{ stroke: none; }}
.error-bar {{ fill: none; stroke: currentColor; stroke-width: 1.2; }}
.ratio-label {{ fill: var(--objfs); font-size: 11px; font-weight: 500; }}
.note {{ border-left: .25rem solid #d79b00; padding: .5rem 1rem; background: #d79b0015; }}
code {{ font-size: .9em; }}
@media (max-width: 700px) {{
  body {{ padding: 1rem; }}
  table {{ display: block; overflow-x: auto; }}
}}
</style>
</head>
<body>
<header>
  <h1>DuckDB Object Storage Read Benchmark</h1>
  <p class="muted">Generated {generated}</p>
</header>
<section>
  <h2>Method</h2>
  <p>TPC-H SF{config['scale_factor']} using {query_description}. {execution_note} DuckDB's default thread count is used.</p>
  <p>{measurement_note} {single_value_note}</p>
  <p>The ratio is ObjFS/native: below 1.00× favors ObjFS; above 1.00× favors native DuckDB.</p>
  {seed_note}
  <table>{metadata_rows}</table>
</section>
<section>
  <h2>{case_heading}</h2>
  <table><thead><tr><th>Case</th><th>Status</th>{setup_header}<th>Stored bytes</th><th>Error</th></tr></thead>
  <tbody>{''.join(case_rows)}</tbody></table>
</section>
{comparisons}
</body>
</html>
"""


def write_outputs(data, run_dir, report):
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(render_report(data), encoding="utf-8")
    write_summary_csv(data, run_dir / "summary.csv")


def save(data, run_dir, report=REPORT):
    (run_dir / "results.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
    write_outputs(data, run_dir, report)


def metadata():
    version = command(
        [DUCKDB, "-csv", "-noheader", "-c", "SELECT version(), current_setting('threads'), current_setting('memory_limit');"]
    ).stdout.strip().split(",")
    if not version or version[0] != "v1.5.5":
        raise RuntimeError(f"expected DuckDB v1.5.5, found {version[0] if version else 'unknown'}")
    # macOS only: system_profiler supplies the host hardware metadata.
    hardware = command(["system_profiler", "SPHardwareDataType"], check=False).stdout
    details = {}
    for line in hardware.splitlines():
        key, separator, value = line.strip().partition(":")
        if separator and key in {"Chip", "Memory", "Total Number of Cores"}:
            details[key.lower().replace(" ", "_")] = value.strip()
    return {
        "generated_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "repository_commit": command(["git", "rev-parse", "HEAD"]).stdout.strip(),
        "duckdb_commit": command(["git", "-C", "duckdb", "rev-parse", "HEAD"]).stdout.strip(),
        "duckdb_version": version[0],
        "default_threads": version[1],
        "default_memory_limit": version[2],
        "platform": platform.platform(),
        "python": platform.python_version(),
        "free_disk": format_bytes(shutil.disk_usage(ROOT).free),
        **details,
    }


def main():
    parser = argparse.ArgumentParser(description="Run native and ObjFS TPC-H benchmarks")
    parser.add_argument("--render-results", type=Path, help="render an existing results.json without benchmarking")
    parser.add_argument("--report", type=Path, help="HTML report path")
    parser.add_argument("--smoke", action="store_true", help="run SF0.01 Q6 instead of the full suite")
    parser.add_argument("--runs", type=int, help="measured runs per query (default: 3; smoke: 1)")
    parser.add_argument("--scale-factor", type=int, choices=(1, 10), help="TPC-H scale factor (default: 10)")
    parser.add_argument("--remote", action="store_true", help="run S3 HTTPFS versus ObjFS instead of local cases")
    parser.add_argument("--s3-bucket", help="S3 bucket used by --remote")
    parser.add_argument("--s3-region", default="ap-east-2", help="S3 region (default: ap-east-2)")
    parser.add_argument("--aws-profile", default="duckdb-bench", help="AWS CLI profile (default: duckdb-bench)")
    parser.add_argument("--seed-run", type=Path, help="completed local run whose native and ObjFS data seed S3")
    parser.add_argument("--reuse-s3-prefix", help="existing S3 prefix containing native and ObjFS SF data")
    parser.add_argument(
        "--persistent-cache",
        action="store_true",
        help="enable an empty per-query ObjFS persistent cache (remote only)",
    )
    args = parser.parse_args()
    if args.render_results:
        results = args.render_results.resolve()
        if not results.is_file():
            parser.error(f"results file not found: {results}")
        data = json.loads(results.read_text(encoding="utf-8"))
        report = args.report.resolve() if args.report else (REMOTE_REPORT if data["config"].get("remote") else REPORT)
        write_outputs(data, results.parent, report)
        print(f"Report: {report}")
        print(f"Summary: {results.parent / 'summary.csv'}")
        return 0
    if args.remote and not args.s3_bucket:
        parser.error("--s3-bucket is required with --remote")
    if args.seed_run and not args.remote:
        parser.error("--seed-run requires --remote")
    if args.reuse_s3_prefix and not args.remote:
        parser.error("--reuse-s3-prefix requires --remote")
    if args.persistent_cache and not args.remote:
        parser.error("--persistent-cache requires --remote")
    if args.seed_run and args.reuse_s3_prefix:
        parser.error("--seed-run and --reuse-s3-prefix are mutually exclusive")
    if args.smoke and args.scale_factor:
        parser.error("--scale-factor cannot be used with --smoke")
    if args.runs is not None and args.runs < 1:
        parser.error("--runs must be at least 1")
    seed_run = args.seed_run.resolve() if args.seed_run else None
    if seed_run and not ((seed_run / "native.duckdb").is_file() and (seed_run / "objfs-local").is_dir()):
        parser.error("--seed-run must contain native.duckdb and objfs-local")

    build()
    if args.remote:
        install = duckdb("INSTALL httpfs;")
        if install.returncode != 0:
            parser.error(install.stderr.strip() or "failed to install HTTPFS")
    runs = args.runs if args.runs is not None else (1 if args.smoke else 3)
    if args.smoke:
        scale_factor, queries, warmups = 0.01, [6], 0 if args.remote else 1
        orders, lineitem = 15000, 60175
    else:
        scale_factor = args.scale_factor or 10
        queries = list(range(1, 23))
        warmups = 0 if args.remote else 1
        orders, lineitem = {
            1: (1500000, 6001215),
            10: (15000000, 59986052),
        }[scale_factor]
    if seed_run:
        seed_results = json.loads((seed_run / "results.json").read_text(encoding="utf-8"))
        seed_cases = seed_results.get("cases", {})
        if seed_results.get("config", {}).get("scale_factor") != scale_factor or any(
            seed_cases.get(name, {}).get("status") != "complete" for name in ("native_local", "objfs_local")
        ):
            parser.error("--seed-run scale factor and completed local cases must match this run")

    run_type = "remote" if args.remote else "local"
    scale_name = "smoke" if args.smoke else f"sf{scale_factor}"
    if args.persistent_cache:
        scale_name += "-persistent-cache"
    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S") + f"-{run_type}-{scale_name}"
    run_dir = CACHE_ROOT / run_id
    run_dir.mkdir(parents=True)
    persistent_cache_root = run_dir / "objfs-persistent-cache" if args.persistent_cache else None
    if persistent_cache_root:
        persistent_cache_root.mkdir()
    s3_prefix = args.reuse_s3_prefix or (f"benchmarks/{run_id}" if args.remote else None)
    report = args.report.resolve() if args.report else (REMOTE_REPORT if args.remote else REPORT)
    data = {
        "metadata": metadata(),
        "config": {
            "scale_factor": scale_factor,
            "queries": queries,
            "warmups": warmups,
            "runs": runs,
            "smoke": args.smoke,
            "remote": args.remote,
            "s3_bucket": args.s3_bucket,
            "s3_region": args.s3_region if args.remote else None,
            "s3_prefix": s3_prefix,
            "aws_profile": args.aws_profile if args.remote else None,
            "seed_run": str(seed_run.relative_to(ROOT)) if seed_run else None,
            "reused_s3_prefix": bool(args.reuse_s3_prefix),
            "persistent_cache": args.persistent_cache,
        },
        "cases": {},
        "artifacts": str(run_dir.relative_to(ROOT)),
    }
    save(data, run_dir, report)

    cases = ("native_s3", "objfs_s3") if args.remote else (
        "native_memory",
        "objfs_memory",
        "native_local",
        "objfs_local",
    )
    for case in cases:
        print(f"Running {case}...", flush=True)
        try:
            if args.remote:
                result = run_remote_case(
                    case,
                    run_dir,
                    scale_factor,
                    queries,
                    runs,
                    orders,
                    lineitem,
                    args.s3_bucket,
                    s3_prefix,
                    args.aws_profile,
                    args.s3_region,
                    seed_run,
                    bool(args.reuse_s3_prefix),
                    persistent_cache_root,
                )
            elif case.endswith("memory"):
                result = run_memory_case(case, run_dir, scale_factor, queries, warmups, runs, orders, lineitem)
            else:
                result = run_local_case(case, run_dir, scale_factor, queries, warmups, runs, orders, lineitem)
        except Exception as error:
            result = {"status": "failed", "error": str(error), "setup": [], "samples": {}, "storage_bytes": None}
            print(f"{case} failed: {error}", file=sys.stderr, flush=True)
        else:
            print(f"{case} complete", flush=True)
        data["cases"][case] = result
        save(data, run_dir, report)

    print(f"Report: {report}", flush=True)
    print(f"Raw results: {run_dir / 'results.json'}", flush=True)
    print(f"Summary: {run_dir / 'summary.csv'}", flush=True)
    return 1 if any(case["status"] != "complete" for case in data["cases"].values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
