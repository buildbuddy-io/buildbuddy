#!/usr/bin/env python3
"""Small deterministic component-contract harness; only Python stdlib + Go + git."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
MODULE = "github.com/buildbuddy-io/buildbuddy"
PACKAGES = {
    "rangemap": "server/util/rangemap",
    "retry": "server/util/retry",
    "peerset": "server/util/peerset",
    "seq": "server/util/lib/seq",
}
CORPUS = json.loads((HERE / "corpus.json").read_text())


def command(args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, **kwargs)


def git(*args):
    return command(["git", *args]).decode().strip()


def blob(revision, path):
    return command(["git", "show", f"{revision}:{path}"])


def fingerprint():
    h = hashlib.sha256()
    for path in [HERE / "run.py", HERE / "corpus.json", *sorted((HERE / "drivers").glob("*.txt"))]:
        h.update(path.name.encode() + b"\0" + path.read_bytes())
    return h.hexdigest()


def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def copy_package(dest, revision, package, working=False):
    """Copy production .go files verbatim; never copy historical tests."""
    if working:
        paths = sorted((ROOT / package).glob("*.go"))
        sources = [(str(p.relative_to(ROOT)), p.read_bytes()) for p in paths if not p.name.endswith("_test.go")]
    else:
        paths = git("ls-tree", "--name-only", revision, package + "/").splitlines()
        sources = [(p, blob(revision, p)) for p in paths if p.endswith(".go") and not p.endswith("_test.go") and str(Path(p).parent) == package]
    if not sources:
        raise RuntimeError(f"no production Go source: {revision}:{package}")
    for path, content in sources:
        write(dest / path, content)
    return b"\n".join(content for _, content in sources).decode()


def stage(dest, revision, package, support):
    # Supporting code and third-party versions are controlled independently of
    # the historical component. No production source rewriting or dependency stubs.
    for name in ("go.mod", "go.sum"):
        write(dest / name, blob(support, name))
    source = copy_package(dest, revision, PACKAGES[package], working=revision == "WORKTREE")
    pending = re.findall(r'"' + re.escape(MODULE) + r'/([^"\s]+)"', source)
    seen = {PACKAGES[package]}
    while pending:
        dep = pending.pop()
        if dep in seen:
            continue
        seen.add(dep)
        dep_source = copy_package(dest, support, dep)
        pending.extend(re.findall(r'"' + re.escape(MODULE) + r'/([^"\s]+)"', dep_source))
    driver = (HERE / "drivers" / f"{package}.go.txt").read_text()
    # Compatibility is confined to test call sites. Detect API shape, never a
    # revision ID, a fix, or an expected behavior.
    if package == "rangemap":
        generic = "func New[" in source
        driver = driver.replace("@NEW@", "New[int]()" if generic else "New()")
        driver = driver.replace("@VALUE@", "r.Val" if generic else "r.Val.(int)")
        lookup = "got,_:=m.Lookup([]byte{byte(key)})" if generic else "got:=0;if v:=m.Lookup([]byte{byte(key)});v!=nil {got=v.(int)}"
        driver = driver.replace("@LOOKUP@", lookup)
    elif package == "seq":
        driver = driver.replace("@TAKE@", "Take" if "func Take[" in source else "Truncate")
    elif package == "peerset":
        limit = "maxFailedFallbackPeers" if "const maxFailedFallbackPeers" in source else "0"
        driver += f"\nconst harnessFallbackLimit = {limit}\n"
    write(dest / PACKAGES[package] / "contract_harness_test.go", driver.encode())
    return hashlib.sha256(source.encode()).hexdigest()


def test_pattern(name):
    # Go interprets each slash-delimited component as a separate regexp.
    return "/".join("^" + re.escape(part) + "$" for part in name.split("/"))


def interpret(raw, code, test=None):
    statuses, output = {}, {}
    started = set()
    for line in raw.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or "Action" not in event:
            continue
        name = event.get("Test")
        if name:
            if event["Action"] == "run":
                started.add(name)
            if event["Action"] in ("pass", "fail", "skip"):
                statuses[name] = event["Action"]
            if "Output" in event:
                output.setdefault(name, []).append(event["Output"])
    # Parent failures summarize child failures; only score executable leaves.
    leaves = {name: status for name, status in statuses.items() if not any(other.startswith(name + "/") for other in statuses)}
    failures = {name: "".join(output.get(name, [])) for name, status in leaves.items() if status == "fail"}
    infrastructure = code not in (0, 1) or (code != 0 and not failures) or not any(s != "skip" for s in leaves.values())
    # A Go test timeout/crash can exit 1 after earlier assertions failed. Those
    # partial results must not make an interrupted suite look like a clean run.
    if started - statuses.keys():
        infrastructure = True
    if test and (test not in leaves or leaves[test] == "skip"):
        infrastructure = True
    return dict(status="infrastructure_error" if infrastructure else ("fail" if failures else "pass"),
                exit_code=code, tests=leaves, failures=failures)


def distinguish(before, after):
    natural_order = lambda name: [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", name)]
    witnesses = sorted((name for name in before["failures"] if after["tests"].get(name) == "pass"), key=natural_order)
    infra = any(r["status"] == "infrastructure_error" for r in (before, after))
    return ("infrastructure_error" if infra else ("detected" if witnesses else "missed")), witnesses


def execute(revision, package, profile, support, out, test=None):
    label = f"{package}-{revision[:12]}-{profile}"
    if test:
        label += "-" + hashlib.sha256(test.encode()).hexdigest()[:10]
    result = dict(revision=revision, package=package, profile=profile, support_revision=support)
    with tempfile.TemporaryDirectory(prefix="bb-correctness-") as temporary:
        dest = Path(temporary)
        result["source_sha256"] = stage(dest, revision, package, support)
        toolchain = command(["go", "env", "GOVERSION"]).decode().strip()
        env = {**os.environ, "HARNESS_PROFILE": profile, "GOWORK": "off", "GOTOOLCHAIN": toolchain, "GOFLAGS": "", "TZ": "UTC"}
        args = ["go", "test", "-mod=readonly", "-json", "-count=1", "-timeout=30s"]
        if test:
            args += ["-run", test_pattern(test)]
        args += ["./" + PACKAGES[package]]
        try:
            process = subprocess.run(args, cwd=dest, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=240)
            raw = process.stdout.decode(errors="replace")
            code = process.returncode
        except subprocess.TimeoutExpired as exc:
            raw = (exc.stdout or b"").decode(errors="replace") + "\nHARNESS: process timeout\n"
            code = 124
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{label}.jsonl").write_text(raw)
        result.update(interpret(raw, code, test), log=f"{label}.jsonl")
        if result["status"] == "infrastructure_error":
            result["error"] = raw[-6000:]
    return result


def metadata():
    return dict(schema=1, harness_sha256=fingerprint(), go_version=command(["go", "version"]).decode().strip(),
                platform=command(["go", "env", "GOOS", "GOARCH"]).decode().splitlines(), support_revision=CORPUS["support_revision"])


def calibrate(args):
    report = metadata()
    report["cases"] = []
    profiles = ["baseline", "improved"] if args.profile == "all" else [args.profile]
    cases = [c for c in CORPUS["cases"] if not args.case or c["id"] == args.case]
    if not cases:
        raise ValueError(f"unknown case: {args.case}")
    cache = {}
    for case in cases:
        # Guard against a mistyped/stale manifest silently testing the wrong tree.
        if git("rev-parse", case["fix"] + "^1") != case["before"]:
            raise ValueError(f"not first parent: {case['id']}")
        row = dict(case)
        row["measurements"] = {}
        for profile in profiles:
            pair = []
            for rev in (case["before"], case["fix"]):
                key = rev, case["package"], profile
                if key not in cache:
                    print(f"{case['id']} {profile} {rev[:12]}", flush=True)
                    cache[key] = execute(*key, CORPUS["support_revision"], args.out)
                pair.append(cache[key])
            before, after = pair
            outcome, witnesses = distinguish(before, after)
            row["measurements"][profile] = dict(outcome=outcome, witnesses=witnesses, before=before, after=after)
            print(f"  {outcome}: {len(witnesses)} distinguishing scenarios", flush=True)
        report["cases"].append(row)
    report["totals"] = {profile: {outcome: sum(row["measurements"][profile]["outcome"] == outcome for row in report["cases"])
                                  for outcome in ("detected", "missed", "infrastructure_error")} for profile in profiles}
    (args.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (args.out / "evidence.json").write_text(json.dumps(compact(report), indent=2) + "\n")
    write_markdown(report, args.out / "report.md")
    print(json.dumps(report["totals"], indent=2))
    # Corpus misses are legitimate measured outcomes, but not a passing gate.
    return int(any(v["missed"] or v["infrastructure_error"] for p, v in report["totals"].items() if p == profiles[-1]))


def compact(report):
    """Keep one replayable witness per case; full measurements remain in report.json."""
    evidence = json.loads(json.dumps(report))
    for row in evidence["cases"]:
        for measurement in row["measurements"].values():
            measurement["witness_count"] = len(measurement["witnesses"])
            measurement["witnesses"] = measurement["witnesses"][:1]
            for side in ("before", "after"):
                run = measurement[side]
                run["test_counts"] = {status: list(run["tests"].values()).count(status) for status in ("pass", "fail", "skip")}
                del run["tests"]
                run["failures"] = {name: text for name, text in run["failures"].items() if name in measurement["witnesses"]}
    return evidence


def write_markdown(report, path):
    lines = ["# Initial correctness corpus", "", f"Go: `{report['go_version']}`", "",
             f"Supporting code/dependencies: `{report['support_revision']}`", "",
             f"Harness SHA-256: `{report['harness_sha256']}`", "",
             "Detection requires the same generated scenario to fail on the first parent and pass on the fix. Other failures on the fix are retained in JSON, not counted as detections.", "",
             "| Case | Fix | Baseline | Improved | Missing capabilities |", "|---|---|---|---|---|"]
    for row in report["cases"]:
        outcome = lambda profile: row["measurements"].get(profile, {}).get("outcome", "not run")
        lines.append(f"| {row['id']} | `{row['fix'][:10]}` | {outcome('baseline')} | {outcome('improved')} | {', '.join(row['missing_capabilities'])} |")
    lines += ["", "## Improvements and reproducible evidence", ""]
    for row in report["cases"]:
        lines += [f"### {row['id']}", "", row["improvement"], ""]
        measurement = row["measurements"].get("improved", {})
        if measurement.get("witnesses"):
            witness = measurement["witnesses"][0]
            lines += [f"Generated witness: `{witness}`", "", "```text", measurement["before"]["failures"][witness].strip(), "```", "",
                      f"Replay both revisions: `python3 tools/correctness/run.py replay --report {path.parent.relative_to(ROOT) if path.parent.is_relative_to(ROOT) else path.parent}/evidence.json --case {row['id']}`", ""]
    path.write_text("\n".join(lines) + "\n")


def replay(args):
    report = json.loads(args.report.read_text())
    if report["harness_sha256"] != fingerprint():
        raise ValueError("harness changed since this evidence was recorded; recalibrate or restore its version")
    if report["go_version"] != metadata()["go_version"] or report["platform"] != metadata()["platform"]:
        raise ValueError("Go version/platform differs from recorded evidence")
    row = next(c for c in report["cases"] if c["id"] == args.case)
    measurement = row["measurements"][args.profile]
    if not measurement["witnesses"]:
        raise ValueError("no validated witness for this case/profile")
    witness = args.test or measurement["witnesses"][0]
    if witness not in measurement["witnesses"]:
        raise ValueError("test is not a validated witness")
    results = [execute(rev, row["package"], args.profile, report["support_revision"], args.out, witness) for rev in (row["before"], row["fix"])]
    for result in results:
        print(result["revision"], result["status"], result["failures"])
    return int(results[0]["status"] != "fail" or results[1]["status"] != "pass")


def check(args):
    revision = git("rev-parse", args.revision) if args.revision else "WORKTREE"
    results = [execute(revision, package, args.profile, CORPUS["support_revision"], args.out, args.test)
               for package in ([args.package] if args.package else PACKAGES)]
    report = metadata()
    report["results"] = results
    (args.out / "check.json").write_text(json.dumps(report, indent=2) + "\n")
    for result in results:
        print(result["package"], result["status"], f"{len(result['failures'])} failing scenarios")
        if result["status"] == "infrastructure_error":
            print(result["error"])
        for name, detail in list(result["failures"].items())[:3]:
            print(name, detail)
    return int(any(result["status"] != "pass" for result in results))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("calibrate", "check", "replay"):
        p = sub.add_parser(name)
        p.add_argument("--out", type=Path, default=HERE / ".runs" / name)
        p.add_argument("--profile", choices=("baseline", "improved", "all") if name == "calibrate" else ("baseline", "improved"), default="all" if name == "calibrate" else "improved")
        if name in ("calibrate", "replay"):
            p.add_argument("--case", required=name == "replay")
        if name == "replay":
            p.add_argument("--report", type=Path, required=True)
        if name in ("check", "replay"):
            p.add_argument("--test", help="exact Go subtest name")
        if name == "check":
            p.add_argument("--revision", help="default: production source in working tree")
            p.add_argument("--package", choices=PACKAGES)
    args = parser.parse_args()
    return {"calibrate": calibrate, "check": check, "replay": replay}[args.command](args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"harness error: {exc}", file=sys.stderr)
        sys.exit(2)
