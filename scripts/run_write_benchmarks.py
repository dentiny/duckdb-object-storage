#!/usr/bin/env python3
"""Measure DuckDB TPC-H lineitem ingestion into native and ObjFS databases."""

import argparse
import csv
import datetime as dt
import html
import json
import shutil
import statistics
import time
from pathlib import Path

from run_read_benchmarks import (
    CACHE_ROOT,
    ROOT,
    aws_environment,
    build,
    command,
    directory_size,
    duckdb,
    format_bytes,
    objfs_s3_sql,
    objfs_sql,
    parse_profiles,
    quote,
    read_stats,
    s3_secret_sql,
    save_process,
    stats_sql,
)


LOCAL_REPORT = ROOT / "docs" / "BENCHMARK_WRITE_REPORT.html"
REMOTE_REPORT = ROOT / "docs" / "BENCHMARK_WRITE_REMOTE_REPORT.html"
CASES = ("native_local", "objfs_local", "native_s3", "objfs_s3")
LABELS = {
    "native_local": "Native · local",
    "objfs_local": "ObjFS · local",
    "native_s3": "Native · local + S3 upload",
    "objfs_s3": "ObjFS · direct S3",
}
EXPECTED_ROWS = {0.01: 60175, 1: 6001215, 10: 59986052}


def source_sql(scale_factor):
    return f"""
.mode csv
.headers off
.output /dev/null
LOAD tpch;
CALL dbgen(sf = {scale_factor});
.output stdout
SELECT 'SOURCE', count(*), sum(l_orderkey), sum(l_quantity) FROM lineitem;
.output /dev/null
"""


def attach_sql(case, run_dir, bucket, prefix, region):
    if case.startswith("native"):
        return f"ATTACH {quote(run_dir / 'native.duckdb')} AS bench;"
    if case == "objfs_local":
        root = run_dir / "objfs-local"
        root.mkdir()
        return objfs_sql("local", root)
    return objfs_s3_sql(bucket, f"{prefix}/objfs", region)


def check_process(process, run_dir, name):
    save_process(run_dir, name, process)
    if process.returncode:
        lines = process.stderr.strip().splitlines()
        message = next((line for line in lines if "Error:" in line), None)
        raise RuntimeError(message or f"{name} exited with {process.returncode}; see {run_dir}")


def stats_from_output(output):
    for row in csv.reader(output.splitlines()):
        if row and row[0] == "SOURCE":
            return row[1:]
    raise RuntimeError("source statistics not found")


def validation_sql(case, run_dir, bucket, prefix, region):
    if case == "native_local":
        return "", run_dir / "native.duckdb", False, "lineitem"
    if case == "objfs_local":
        return objfs_sql("local", run_dir / "objfs-local", read_only=True), None, False, "bench.lineitem"
    if case == "native_s3":
        uri = f"s3://{bucket}/{prefix}/native/lineitem.duckdb"
        sql = f"{s3_secret_sql(bucket, prefix, region)}\nATTACH {quote(uri)} AS bench (READ_ONLY);"
        return sql, None, True, "bench.lineitem"
    return objfs_s3_sql(bucket, f"{prefix}/objfs", region, read_only=True), None, True, "bench.lineitem"


def run_case(case, number, scale_factor, root, bucket, region, profile):
    name = f"{case}-run{number}"
    run_dir = root / name
    run_dir.mkdir()
    prefix = f"benchmarks/write/{root.name}/{name}"
    env = aws_environment(profile, region) if case.endswith("s3") else None
    stats, cache_path, io_path = stats_sql(run_dir, name) if case.startswith("objfs") else ("", None, None)
    sql = f"""
{source_sql(scale_factor)}
{attach_sql(case, run_dir, bucket, prefix, region)}
PRAGMA enable_profiling='json';
CREATE TABLE bench.lineitem AS SELECT * FROM memory.lineitem;
CHECKPOINT bench;
PRAGMA disable_profiling;
{stats}
"""
    process = duckdb(sql, env=env)
    check_process(process, run_dir, name)
    expected = stats_from_output(process.stdout)
    if int(expected[0]) != EXPECTED_ROWS[scale_factor]:
        raise RuntimeError(f"unexpected SF{scale_factor} lineitem row count: {expected[0]}")
    profiles = parse_profiles(process.stderr)
    if (
        len(profiles) != 2
        or not profiles[0]["query_name"].startswith("CREATE TABLE bench.lineitem")
        or not profiles[1]["query_name"].startswith("CHECKPOINT bench")
    ):
        raise RuntimeError(f"expected CREATE TABLE and CHECKPOINT profiles, found {len(profiles)}")
    table_write_seconds, checkpoint_seconds = (item["latency"] for item in profiles)
    write_seconds = table_write_seconds + checkpoint_seconds
    upload_seconds = 0.0
    storage_bytes = None
    if case == "native_s3":
        database = run_dir / "native.duckdb"
        uri = f"s3://{bucket}/{prefix}/native/lineitem.duckdb"
        start = time.perf_counter()
        upload = command(
            ["aws", "s3", "cp", database, uri, "--only-show-errors", "--profile", profile, "--region", region],
            check=False,
        )
        upload_seconds = time.perf_counter() - start
        check_process(upload, run_dir, f"{name}-upload")
        storage_bytes = database.stat().st_size
    elif case == "native_local":
        storage_bytes = (run_dir / "native.duckdb").stat().st_size
    elif case == "objfs_local":
        storage_bytes = directory_size(run_dir / "objfs-local")

    setup, database, needs_aws, table = validation_sql(case, run_dir, bucket, prefix, region)
    verify_sql = f"""
.mode csv
.headers off
.output /dev/null
{setup}
.output stdout
SELECT 'SOURCE', count(*), sum(l_orderkey), sum(l_quantity) FROM {table};
"""
    verify_env = aws_environment(profile, region) if needs_aws else None
    verify = duckdb(verify_sql, database=database, env=verify_env)
    check_process(verify, run_dir, f"{name}-verify")
    if stats_from_output(verify.stdout) != expected:
        raise RuntimeError(f"{name}: reopened database does not match source")
    rows = int(expected[0])
    delivery_seconds = write_seconds + upload_seconds
    result = {
        "case": case,
        "run": number,
        "rows": rows,
        "table_write_seconds": table_write_seconds,
        "checkpoint_seconds": checkpoint_seconds,
        "write_seconds": write_seconds,
        "upload_seconds": upload_seconds,
        "delivery_seconds": delivery_seconds,
        "write_rows_per_second": rows / write_seconds,
        "delivery_rows_per_second": rows / delivery_seconds,
        "storage_bytes": storage_bytes,
        "s3_prefix": prefix if case.endswith("s3") else None,
    }
    if io_path:
        result["cache_stats"] = read_stats(cache_path)
        result["io_stats"] = read_stats(io_path)
    if case == "native_local":
        (run_dir / "native.duckdb").unlink()
    elif case == "objfs_local":
        shutil.rmtree(run_dir / "objfs-local")
    return result


def summary(samples, case, metric):
    values = [sample[metric] for sample in samples if sample["case"] == case]
    return statistics.median(values), statistics.stdev(values) if len(values) > 1 else 0.0


def chart(samples, title, cases, metric, show_sd):
    values = {case: summary(samples, case, metric) for case in cases}
    width, height = 1000, 430
    left, top, plot_width, plot_height = 105, 55, 850, 285
    baseline = top + plot_height
    maximum = max(median + deviation for median, deviation in values.values()) * 1.2 or 1
    description = "Rows per second with sample standard deviation error bars." if show_sd else "Rows per second."
    lines = [
        f'<div class="chart-wrap"><svg class="throughput-chart" viewBox="0 0 {width} {height}" '
        f'role="img" aria-labelledby="chart-title chart-desc">',
        f'<title id="chart-title">{html.escape(title)}</title>',
        f'<desc id="chart-desc">{description}</desc>',
        f'<rect class="plot-frame" x="{left}" y="{top}" width="{plot_width}" height="{plot_height}"/>',
    ]
    for tick in range(5):
        value = maximum * tick / 4
        y = baseline - plot_height * tick / 4
        lines.append(
            f'<line class="grid-line" x1="{left}" y1="{y:.1f}" '
            f'x2="{left + plot_width}" y2="{y:.1f}"/>'
            f'<text class="axis-text" x="{left - 12}" y="{y + 4:.1f}" '
            f'text-anchor="end">{value / 1_000_000:.2f}M</text>'
        )
    for index, case in enumerate(cases):
        median, deviation = values[case]
        center = left + plot_width * (index + 0.5) / len(cases)
        y = baseline - median / maximum * plot_height
        high = baseline - (median + deviation) / maximum * plot_height
        low = baseline - max(median - deviation, 0) / maximum * plot_height
        css_class = "series-native" if case.startswith("native") else "series-objfs"
        label = f"{median:,.0f} ± {deviation:,.0f}" if show_sd else f"{median:,.0f}"
        whiskers = (
            f'<line class="error-bar" x1="{center:.1f}" y1="{high:.1f}" '
            f'x2="{center:.1f}" y2="{low:.1f}"/>'
            f'<line class="error-bar" x1="{center - 7:.1f}" y1="{high:.1f}" '
            f'x2="{center + 7:.1f}" y2="{high:.1f}"/>'
            f'<line class="error-bar" x1="{center - 7:.1f}" y1="{low:.1f}" '
            f'x2="{center + 7:.1f}" y2="{low:.1f}"/>'
        ) if show_sd else ""
        lines.append(
            f'<g class="{css_class}"><title>{html.escape(LABELS[case])}: '
            f'{label} rows/s</title>'
            f'<rect class="data-bar" x="{center - 55:.1f}" y="{y:.1f}" '
            f'width="110" height="{baseline - y:.1f}"/>'
            f'{whiskers}</g>'
            f'<text class="value-label" x="{center:.1f}" y="{max(top + 17, high - 10):.1f}" '
            f'text-anchor="middle">{label}</text>'
            f'<text class="axis-text" x="{center:.1f}" y="{baseline + 25}" '
            f'text-anchor="middle">{html.escape(LABELS[case])}</text>'
        )
    lines.append(
        f'<text class="axis-title" x="{left + plot_width / 2}" y="{height - 16}" '
        'text-anchor="middle">Storage path</text>'
        f'<text class="axis-title" x="25" y="{top + plot_height / 2}" text-anchor="middle" '
        f'transform="rotate(-90 25 {top + plot_height / 2})">Throughput (million rows/s)</text>'
        '</svg></div>'
    )
    return "\n".join(lines)


def objfs_stats_html(samples, case):
    records = [sample for sample in samples if sample["case"] == case]
    has_cache_activity = any(
        (row["hit_count"] or 0) + (row["miss_count"] or 0) + (row.get("entry_count") or 0)
        for sample in records for row in sample["cache_stats"]
    )
    cache_rows = []
    for cache in ("memory_data", "memory_metadata", "persistent"):
        entries = [row for sample in records for row in sample["cache_stats"] if row["cache"] == cache]
        hits = sum(row["hit_count"] or 0 for row in entries)
        misses = sum(row["miss_count"] or 0 for row in entries)
        rate = f"{hits / (hits + misses):.1%}" if hits + misses else "—"
        cache_rows.append(f"<tr><td>{cache}</td><td>{hits:,}</td><td>{misses:,}</td><td>{rate}</td></tr>")
    io_rows = []
    for operation in ("read", "write", "stat", "list", "delete"):
        entries = [row for sample in records for row in sample["io_stats"] if row["operation"] == operation]
        count = sum(row["request_count"] or 0 for row in entries)
        weighted_ms = sum((row["request_count"] or 0) * (row["average_latency_ms"] or 0) for row in entries)
        average = f"{weighted_ms / count:.1f}" if count else "—"
        size = "—"
        if operation in ("read", "write") and entries and all(row.get("bytes") is not None for row in entries):
            size = format_bytes(sum(row["bytes"] for row in entries))
        io_rows.append(f"<tr><td>{operation}</td><td>{count:,}</td><td>{size}</td><td>{average}</td></tr>")
    cache_html = f"""<h3>Cache</h3>
<table><thead><tr><th>Cache</th><th>Hits</th><th>Misses</th><th>Hit rate</th></tr></thead>
<tbody>{''.join(cache_rows)}</tbody></table>""" if has_cache_activity else ""
    return f"""
<h2>ObjFS {'cache and ' if has_cache_activity else ''}I/O statistics</h2>
<p>Totals across {len(records)} ObjFS runs; verification excluded. Bytes are OpenDAL payload, not S3 wire traffic. I/O latency is weighted by request count.</p>
{cache_html}
<h3>OpenDAL operations</h3>
<table><thead><tr><th>Operation</th><th>Requests</th><th>Bytes</th><th>Average latency (ms)</th></tr></thead>
<tbody>{''.join(io_rows)}</tbody></table>
"""


def write_outputs(data, run_dir, report):
    samples = data["samples"]
    remote = data["config"]["remote"]
    runs = data["config"]["runs"]
    cases = CASES[2:] if remote else CASES[:2]
    with (run_dir / "summary.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "case", "runs", "write_rows_per_second_median", "write_rows_per_second_stddev",
            "delivery_rows_per_second_median", "delivery_rows_per_second_stddev",
        ])
        for case in cases:
            write_median, write_deviation = summary(samples, case, "write_rows_per_second")
            delivery_median, delivery_deviation = summary(samples, case, "delivery_rows_per_second")
            writer.writerow([
                case, data["config"]["runs"], write_median, write_deviation,
                delivery_median, delivery_deviation,
            ])
    title = "S3 delivery throughput" if remote else "Local database write throughput"
    metric = "delivery_rows_per_second" if remote else "write_rows_per_second"
    chart_html = chart(samples, title, cases, metric, runs > 1)
    objfs_case = "objfs_s3" if remote else "objfs_local"
    objfs_samples = [sample for sample in samples if sample["case"] == objfs_case]
    stats = objfs_stats_html(objfs_samples, objfs_case) if objfs_samples and all(
        "cache_stats" in sample and "io_stats" in sample for sample in objfs_samples
    ) else ""
    native_median, native_deviation = summary(samples, cases[0], metric)
    objfs_median, objfs_deviation = summary(samples, cases[1], metric)
    ratio = objfs_median / native_median
    summary_rows = []
    for case, median, deviation in (
        (cases[0], native_median, native_deviation), (cases[1], objfs_median, objfs_deviation)
    ):
        value = f"{median:,.0f} ± {deviation:,.0f}" if runs > 1 else f"{median:,.0f}"
        summary_rows.append(f"<tr><td>{html.escape(LABELS[case])}</td><td>{value}</td></tr>")
    stage_header = "<th>Upload (s)</th><th>Delivery (s)</th><th>Delivery (rows/s)</th>" if remote else (
        "<th>Write (s)</th><th>Write (rows/s)</th>"
    )
    rows = []
    matching_samples = (sample for sample in samples if sample["case"] in cases)
    for sample in sorted(matching_samples, key=lambda item: (item["run"], cases.index(item["case"]))):
        stage_cells = (
            f"<td>{sample['upload_seconds']:.3f}</td><td>{sample['delivery_seconds']:.3f}</td>"
            f"<td>{sample['delivery_rows_per_second']:,.0f}</td>"
            if remote else
            f"<td>{sample['write_seconds']:.3f}</td><td>{sample['write_rows_per_second']:,.0f}</td>"
        )
        rows.append(
            f"<tr><td>{html.escape(LABELS[sample['case']])}</td><td>{sample['run']}</td>"
            f"<td>{sample['table_write_seconds']:.3f}</td><td>{sample['checkpoint_seconds']:.3f}</td>"
            f"{stage_cells}</tr>"
        )
    method = (
        "Native: local write + checkpoint + S3 upload. ObjFS: direct S3 write + checkpoint. "
        "Source generation and verification are excluded."
        if remote else
        "Write time is CREATE TABLE AS SELECT plus CHECKPOINT; source generation and verification are excluded."
    )
    run_note = "" if remote else "<p>Write = Table write + Checkpoint; rows/s = row count / Write.</p>"
    run_label = f"{runs} {'run' if runs == 1 else 'runs'}"
    chart_note = (
        "Bars: median; whiskers: ± sample standard deviation (rows/s). Higher is better."
        if runs > 1 else "Rows/s; higher is better."
    )
    summary_heading = "Median ± SD (rows/s)" if runs > 1 else "Rows/s"
    ratio_heading = "ObjFS/native median throughput" if runs > 1 else "ObjFS/native throughput"
    page = f"""<!doctype html>
<html lang="en"><meta charset="utf-8"><title>DuckDB {'S3' if remote else 'local'} write benchmark</title>
<style>
body{{font:16px system-ui,sans-serif;color:#243041;max-width:1050px;margin:40px auto;padding:0 24px}}
h1{{font-size:32px}} h2{{margin-top:40px}} p{{line-height:1.5;color:#536171}}
.chart-wrap{{width:100%;overflow-x:auto}}
.throughput-chart{{display:block;width:100%;min-width:760px;color:inherit}}
.plot-frame,.grid-line{{fill:none;stroke:#dce3eb;stroke-width:1}}
.grid-line{{opacity:.7}} .axis-text{{fill:#536171;font-size:14px}}
.axis-title{{fill:#243041;font-size:15px;font-weight:500}}
.series-native{{fill:#4d6684;stroke:#4d6684}} .series-objfs{{fill:#287d78;stroke:#287d78}}
.data-bar{{stroke:none}} .error-bar{{fill:none;stroke:#243041;stroke-width:2}}
.value-label{{fill:#243041;font-size:16px;font-weight:600}}
table{{border-collapse:collapse;width:100%;margin-top:16px}}
th,td{{padding:10px;border-bottom:1px solid #dce3eb;text-align:right}}
th:first-child,td:first-child{{text-align:left}}
</style>
<h1>DuckDB {'S3 delivery' if remote else 'local'} write benchmark</h1>
<p>TPC-H lineitem SF{data['config']['scale_factor']:g}; {run_label} per backend.
{method}</p>
<h2>{title}</h2>
<p>{chart_note}</p>
{chart_html}
<table><thead><tr><th>Case</th><th>{summary_heading}</th></tr></thead>
<tbody>{''.join(summary_rows)}</tbody></table>
<p>{ratio_heading}: {ratio:.2f}×.</p>
<h2>Individual runs</h2>
{run_note}
<table><thead><tr><th>Case</th><th>Run</th><th>Table write (s)</th><th>Checkpoint (s)</th>
{stage_header}</tr></thead><tbody>{''.join(rows)}</tbody></table>
{stats}
</html>"""
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(page, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Benchmark native and ObjFS database write throughput")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--local", action="store_true", help="compare native and ObjFS local database writes")
    modes.add_argument("--remote", action="store_true", help="compare native upload with direct ObjFS S3 writes")
    parser.add_argument("--smoke", action="store_true", help="run SF0.01 once")
    parser.add_argument("--runs", type=int, help="fresh database runs per case (default: 3; smoke: 1)")
    parser.add_argument("--scale-factor", type=int, choices=(1, 10), help="TPC-H scale factor (default: 1)")
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-region", default="ap-east-2")
    parser.add_argument("--aws-profile", default="duckdb-bench")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--render-results", type=Path, help="render an existing results.json without benchmarking")
    parser.add_argument("--no-build", action="store_true", help="use the existing release binary")
    args = parser.parse_args()
    if args.render_results:
        results = args.render_results.resolve()
        data = json.loads(results.read_text(encoding="utf-8"))
        report = args.report or (REMOTE_REPORT if data["config"]["remote"] else LOCAL_REPORT)
        write_outputs(data, results.parent, report.resolve())
        print(f"Report: {report.resolve()}\nSummary: {results.parent / 'summary.csv'}")
        return 0
    if not (args.local or args.remote):
        parser.error("choose --local or --remote")
    if args.remote and not args.s3_bucket:
        parser.error("--s3-bucket is required with --remote")
    if args.smoke and args.scale_factor:
        parser.error("--scale-factor cannot be used with --smoke")
    if args.runs is not None and args.runs < 1:
        parser.error("--runs must be at least 1")
    if not args.no_build:
        build()
    version = duckdb("SELECT version();")
    if version.returncode or "v1.5.5" not in version.stdout:
        parser.error("benchmark requires the DuckDB v1.5.5 release binary")
    if args.remote:
        install = duckdb("INSTALL httpfs;")
        if install.returncode:
            parser.error(install.stderr.strip() or "failed to install HTTPFS")
    scale_factor = 0.01 if args.smoke else (args.scale_factor or 1)
    runs = args.runs if args.runs is not None else (1 if args.smoke else 3)
    mode = "remote" if args.remote else "local"
    size = "smoke" if args.smoke else f"sf{scale_factor}"
    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f") + f"-write-{mode}-{size}"
    run_dir = CACHE_ROOT / run_id
    run_dir.mkdir(parents=True)
    data = {"config": {"scale_factor": scale_factor, "runs": runs, "remote": args.remote}, "samples": []}
    cases = CASES[2:] if args.remote else CASES[:2]
    for number in range(1, runs + 1):
        for case in cases:
            sample = run_case(case, number, scale_factor, run_dir, args.s3_bucket, args.s3_region, args.aws_profile)
            data["samples"].append(sample)
            (run_dir / "results.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
            print(f"{case} run {number}/{runs}: {sample['delivery_rows_per_second']:,.0f} rows/s", flush=True)
    report = args.report or (REMOTE_REPORT if args.remote else LOCAL_REPORT)
    write_outputs(data, run_dir, report.resolve())
    print(
        f"Report: {report.resolve()}\n"
        f"Raw results: {run_dir / 'results.json'}\n"
        f"Summary: {run_dir / 'summary.csv'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
