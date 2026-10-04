#!/usr/bin/env python3
"""Run DuckDB SQLLogicTests against the SlateDB test configs and maintain skip lists.

Every test runs in its own unittest process so that crashes and hangs are attributed
to a single test instead of aborting the whole suite.

Subcommands:
  survey   Run tests with the skip lists disabled and report failures that no skip
           list covers.
  skipped  Run the tests listed in skip lists (with the skip lists disabled) and
           report entries that pass now and can be removed.
  lint     Validate the skip lists: format, duplicates, and unknown test paths.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
DUCKDB_DIR = PROJECT_DIR / "duckdb"
SKIP_LIST_DIR = PROJECT_DIR / "test" / "configs" / "skip"
SKIP_LISTS = {
    "extension_bugs": SKIP_LIST_DIR / "extension_bugs.json",
    "unsupported_features": SKIP_LIST_DIR / "unsupported_features.json",
    "by_design": SKIP_LIST_DIR / "by_design.json",
}

FAILURE_HEADER = re.compile(
    r"^(Query unexpectedly (failed|succeeded)|Statement unexpectedly (failed|succeeded)|"
    r"Wrong (result|column count|row count|error message)[^\n]*|"
    r"Mismatch on row[^\n]*|Failed to [^\n]*)",
    re.MULTILINE,
)
ERROR_LINE = re.compile(r"^[A-Z][A-Za-z ]*(Error|Exception): .*$", re.MULTILINE)
OUTPUT_TAIL_BYTES = 6000


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_skip_list(path):
    """Returns {test_path: reason} for one skip list."""
    entries = {}
    for group in load_json(path).get("skip_tests", []):
        for test in group["paths"]:
            entries[test] = group["reason"]
    return entries


def selected_skip_lists(names):
    if not names:
        return dict(SKIP_LISTS)
    unknown = [name for name in names if name not in SKIP_LISTS]
    if unknown:
        sys.exit(f"Unknown skip list(s): {', '.join(unknown)}; expected {', '.join(SKIP_LISTS)}")
    return {name: SKIP_LISTS[name] for name in names}


def all_skipped_tests():
    """Returns {test_path: (skip_list_name, reason)} across every skip list."""
    result = {}
    for name, path in SKIP_LISTS.items():
        for test, reason in load_skip_list(path).items():
            result[test] = (name, reason)
    return result


def write_config_without_skip_lists(config_path, directory):
    """Copies a test config, dropping skip-list `extends` entries so skipped tests run."""
    config_path = Path(config_path).resolve()
    config = load_json(config_path)
    skip_paths = {path.resolve() for path in SKIP_LISTS.values()}
    extends = []
    for entry in config.get("extends", []):
        resolved = (config_path.parent / entry).resolve()
        if resolved not in skip_paths:
            extends.append(str(resolved))
    if extends:
        config["extends"] = extends
    else:
        config.pop("extends", None)
    output = Path(directory) / config_path.name
    with open(output, "w") as f:
        json.dump(config, f, indent=2)
    return output


def list_tests(unittest, test_filter):
    command = [str(unittest), "--test-dir", str(DUCKDB_DIR), "--list-test-names-only"]
    if test_filter:
        command.append(test_filter)
    # Catch exits with a non-zero status when listing, so rely on the output instead.
    output = subprocess.run(command, cwd=PROJECT_DIR, capture_output=True, text=True).stdout
    tests = [line.strip() for line in output.splitlines() if line.strip()]
    if not tests:
        sys.exit(f"No tests listed by {' '.join(command)}")
    return tests


def normalize_test_name(test):
    """Maps the extension's absolute test paths to the key DuckDB uses for skip lookups."""
    if test.startswith("/"):
        position = test.find("test/sql")
        if position != -1:
            return test[position:]
    return test


def summarize_failure(output):
    header = FAILURE_HEADER.search(output)
    error = ERROR_LINE.search(output)
    parts = []
    if header:
        parts.append(header.group(0).strip())
    if error:
        parts.append(error.group(0).strip()[:300])
    if not parts:
        lines = [line for line in output.splitlines() if line.strip()]
        parts.append(lines[-1][:300] if lines else "no output")
    return " | ".join(parts)


def run_test(unittest, config, test, timeout):
    command = [str(unittest), "--test-config", str(config), "--test-dir", str(DUCKDB_DIR), test]
    start = time.monotonic()
    process = subprocess.Popen(
        command, cwd=PROJECT_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True
    )
    try:
        output, _ = process.communicate(timeout=timeout)
        status = "pass" if process.returncode == 0 else ("crash" if process.returncode < 0 else "fail")
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, 9)
        output, _ = process.communicate()
        status = "timeout"
    finally:
        temp_dir = DUCKDB_DIR / "duckdb_unittest_tempdir"
        shutil.rmtree(temp_dir / str(process.pid), ignore_errors=True)
        shutil.rmtree(temp_dir / f"{process.pid}_slatedb", ignore_errors=True)
    output = output.decode(errors="replace")
    if "No test cases matched" in output:
        status = "missing"
    result = {
        "test": test,
        "status": status,
        "returncode": process.returncode,
        "seconds": round(time.monotonic() - start, 2),
    }
    if status != "pass":
        result["summary"] = "timed out" if status == "timeout" else summarize_failure(output)
        result["output_tail"] = output[-OUTPUT_TAIL_BYTES:]
    return result


def run_tests(unittest, config, tests, jobs, timeout, keep_skip_lists):
    results = []
    lock = threading.Lock()
    with tempfile.TemporaryDirectory(prefix="duckdb_objfs_test_config_") as directory:
        effective_config = Path(config).resolve() if keep_skip_lists else write_config_without_skip_lists(config, directory)
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = [executor.submit(run_test, unittest, effective_config, test, timeout) for test in tests]
            for future in as_completed(futures):
                result = future.result()
                with lock:
                    results.append(result)
                    done = len(results)
                    if result["status"] != "pass":
                        print(f"[{done}/{len(tests)}] {result['status'].upper()} {result['test']}: {result['summary']}")
                    elif done % 200 == 0:
                        print(f"[{done}/{len(tests)}] ...")
                    sys.stdout.flush()
    results.sort(key=lambda result: result["test"])
    return results


def write_results(path, config, results):
    if not path:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"config": str(config), "results": results}, f, indent=2)
    print(f"Wrote results to {path}")


def command_survey(args):
    tests = args.tests or list_tests(args.unittest, args.filter)
    results = run_tests(args.unittest, args.config, tests, args.jobs, args.timeout, keep_skip_lists=False)
    write_results(args.output, args.config, results)

    skipped = all_skipped_tests()
    failures = [result for result in results if result["status"] != "pass"]
    uncovered = [result for result in failures if normalize_test_name(result["test"]) not in skipped]
    stale = [
        result
        for result in results
        if result["status"] == "pass" and normalize_test_name(result["test"]) in skipped
    ]
    print(f"\n{len(results)} tests: {len(results) - len(failures)} passed, {len(failures)} failed")
    if stale:
        print(f"\n{len(stale)} skip-listed test(s) pass now and can be removed:")
        for result in stale:
            print(f"  [{skipped[normalize_test_name(result['test'])][0]}] {result['test']}")
    if uncovered:
        print(f"\n{len(uncovered)} failure(s) not covered by any skip list:")
        for result in uncovered:
            print(f"  {result['status'].upper()} {result['test']}: {result['summary']}")
        return 1
    return 0


def command_skipped(args):
    """Skip lists are shared by all configs, so an entry is stale only if it passes under every config."""
    lists = selected_skip_lists(args.lists)
    tests = {}
    for name, path in lists.items():
        for test in load_skip_list(path):
            tests[test] = name
    if not tests:
        print("No skipped tests to run")
        return 0

    registered = {normalize_test_name(test): test for test in list_tests(args.unittest, None)}
    names = {registered.get(test, test): test for test in tests}
    statuses = {test: [] for test in tests}
    all_results = {}
    for config in args.config:
        print(f"Running {len(tests)} skipped tests with {config}")
        results = run_tests(args.unittest, config, sorted(names), args.jobs, args.timeout, keep_skip_lists=False)
        all_results[str(config)] = results
        for result in results:
            statuses[names[result["test"]]].append(result["status"])
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(all_results, f, indent=2)

    passing = sorted(test for test, status in statuses.items() if all(s == "pass" for s in status))
    missing = sorted(test for test, status in statuses.items() if "missing" in status)
    print(f"\n{len(tests)} skipped tests: {len(tests) - len(passing)} still fail, {len(passing)} pass under every config")
    for test in missing:
        print(f"  MISSING [{tests[test]}] {test}: not found in the DuckDB test suite")
    for test in passing:
        print(f"  PASS [{tests[test]}] {test}: remove it from the skip list")
    return 1 if passing or missing else 0


def command_lint(args):
    known_tests = {normalize_test_name(test) for test in list_tests(args.unittest, None)} if args.unittest else None
    seen = {}
    problems = []
    for name, path in SKIP_LISTS.items():
        config = load_json(path)
        if set(config) - {"description", "skip_tests"}:
            problems.append(f"{path}: only 'description' and 'skip_tests' are allowed")
        if config.get("skip_tests") == []:
            problems.append(f"{path}: DuckDB cannot parse an empty 'skip_tests' list; omit the key instead")
        for group in config.get("skip_tests", []):
            if not group.get("reason"):
                problems.append(f"{path}: every skip group needs a reason")
            paths = group.get("paths", [])
            if paths != sorted(paths):
                problems.append(f"{path}: paths for reason '{group.get('reason')}' are not sorted")
            for test in paths:
                if test in seen:
                    problems.append(f"{test}: listed in both {seen[test]} and {name}")
                seen[test] = name
                if known_tests is not None and test not in known_tests:
                    problems.append(f"{test}: [{name}] not found in the DuckDB test suite")
    if args.unittest:
        for config in sorted((PROJECT_DIR / "test" / "configs").glob("*.json")):
            command = [str(args.unittest), "--test-config", str(config), "--list-test-names-only", "[none]"]
            output = subprocess.run(command, cwd=PROJECT_DIR, capture_output=True, text=True)
            if "Failed to parse config file" in output.stdout + output.stderr:
                problems.append(f"{config}: unittest cannot load it: {(output.stdout + output.stderr).strip()[-300:]}")
    for problem in problems:
        print(problem)
    print(f"{len(seen)} skipped tests across {len(SKIP_LISTS)} skip lists, {len(problems)} problem(s)")
    return 1 if problems else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_run_arguments(subparser):
        subparser.add_argument("--unittest", required=True, type=Path, help="path to the unittest binary")
        subparser.add_argument("--jobs", type=int, default=os.cpu_count(), help="parallel test processes")
        # Each WAL fsync is a durable SlateDB write, so commit-heavy tests are slow under full parallelism.
        subparser.add_argument("--timeout", type=float, default=900, help="per-test timeout in seconds")
        subparser.add_argument("--output", help="write per-test results to this JSON file")

    survey = subparsers.add_parser("survey", help="run tests with skip lists disabled")
    add_run_arguments(survey)
    survey.add_argument("--config", required=True, type=Path, help="DuckDB test config to run with")
    survey.add_argument("--filter", help="Catch test filter, e.g. 'test/sql/attach/*'")
    survey.add_argument("tests", nargs="*", help="explicit test paths (default: the whole suite)")
    survey.set_defaults(handler=command_survey)

    skipped = subparsers.add_parser("skipped", help="run skip-listed tests and report ones that pass")
    add_run_arguments(skipped)
    skipped.add_argument(
        "--config", required=True, type=Path, action="append", help="DuckDB test config; repeat to check several"
    )
    skipped.add_argument("lists", nargs="*", help=f"skip lists to run (default: all of {', '.join(SKIP_LISTS)})")
    skipped.set_defaults(handler=command_skipped)

    lint = subparsers.add_parser("lint", help="validate skip list files")
    lint.add_argument("--unittest", type=Path, help="also verify that every path exists in the test suite")
    lint.set_defaults(handler=command_lint)

    args = parser.parse_args()
    sys.exit(args.handler(args))


if __name__ == "__main__":
    main()
