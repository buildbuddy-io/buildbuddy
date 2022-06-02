#!/usr/bin/env python3

import os
import re
import shlex
import subprocess
import sys

INPUT_METRICS_PATH = "server/metrics/metrics.go"
OUTPUT_PATH = "docs/prometheus-metrics.md"
PRETTIER_TOOL_PATH = "tools/prettier/prettier.sh"

FILE_HEADER = """---
id: prometheus-metrics
title: Prometheus Metrics
sidebar_label: Prometheus Metrics
---

<!--

============================
GENERATED FILE - DO NOT EDIT
============================

Run `python3 server/metrics/generate_docs.py` to re-generate.

-->

BuildBuddy exposes [Prometheus](https://prometheus.io) metrics that allow monitoring the
[four golden signals](https://landing.google.com/sre/sre-book/chapters/monitoring-distributed-systems/):
latency, traffic, errors, and saturation.

To view these metrics in a live-updating dashboard, we recommend using a tool
like [Grafana](https://grafana.com).
"""


def fatal(msg: str, line_index=None):
    line_marker = "" if line_index is None else f" (Line {line_index+1})"
    sys.stderr.write(f"ERROR{line_marker}: {msg}\n")
    sys.stderr.flush()
    sys.exit(1)


class DocsGenerator(object):
    def __init__(self, metrics_go_path):
        self.metrics_go_path = metrics_go_path

        self.state = None
        self.output_lines = [FILE_HEADER]

        # Parsed LABEL_CONSTANTS section.
        # Maps label constant token name to {"comments": [...], "value": ...}
        self.label_constants = {}

        # Fields for METRIC and METRIC.LABELS state

        self.metric = None

        # Fields for LABEL_CONSTANTS state

        self.label_constant_comments = []

        # Fields for METRIC.LABELS state

        self.label_comments = []
        self.label_value = ""

        # Fields for PROMQL state (promql code block in markdown)

        self.promql_lines = []

    def parse(self):
        with open(self.metrics_go_path, "r") as f:
            lines = f.readlines()
            for (i, line) in enumerate(lines):
                self.process_line(i, line)

        self.output_lines.append("")
        return self.output_lines

    def process_line(self, line_index, line):
        line = line.strip()

        if line == "const (":
            self.state = "LABEL_CONSTANTS"
            return
        if self.state == "LABEL_CONSTANTS":
            if line == ")":
                self.state = None
                return
            if line.startswith("///"):
                m = re.match(r"///\s(.*)", line)
                if not m:
                    fatal(
                        f'Label constant comments should start with "/// "',
                        line_index=line_index,
                    )
                self.label_constant_comments.append(m.group(1))
            m = re.match(r'(\w+) = "(.*)"', line)
            if m:
                self.label_constants[m.group(1)] = {
                    "value": m.group(2),
                    "comments": self.label_constant_comments,
                }
                self.label_constant_comments = []

            return

        if self.state == "METRIC":
            # We're inside a metric definition.
            if line == "})":
                # End metric declaration.
                self.state = None
                self.flush_metric()
                return

            if "[]string{" in line:
                # Begin declaration of metric labels.
                self.state = "METRIC.LABELS"
                return

            # Parse string attributes (Name:, Help:, etc.)
            m = re.match(r'^(Namespace|Subsystem|Name|Help):\s+"(.*)"', line)
            if m:
                self.set_metric_attr(m.group(1).lower(), m.group(2))
                return

        # Inside a metric label list ([]string{ ... })
        if self.state == "METRIC.LABELS":
            # We're inside the labels list of a metric vector definition.
            if line == "})":
                # End metric and metric label declaration.
                self.state = None
                self.flush_metric()
                return

            if line.startswith("///"):
                m = re.match(r"///\s(.*)", line)
                if not m:
                    fatal(
                        f'Label comments should start with "/// "',
                        line_index=line_index,
                    )
                self.label_comments.append(m.group(1))
                return

            if line.startswith('"'):
                m = re.match(r'"(.*)"', line)
                if not m:
                    fatal(
                        f"Expected label constant after comment line.",
                        line_index=line_index,
                    )
                self.add_metric_label(line_index, m.group(1))
                return

            m = re.match(r"\w+", line)
            if m:
                label_constant_token = m.group()
                const = self.label_constants[label_constant_token]
                self.label_comments = const["comments"]
                self.add_metric_label(line_index, const["value"])

        # Check for a new metric definition.
        m = re.match(r"^\w+ = promauto.New(\w+)", line)
        if m:
            metric_type = m.group(1)
            if metric_type.endswith("Vec"):
                metric_type = metric_type[: -len("Vec")]
            self.set_metric_attr("type", metric_type)
            self.state = "METRIC"
            return

        # Insert markdown if applicable.
        if line.startswith("///"):
            if line == "///":
                self.output_lines.append("")
                return
            m = re.match("^///\s(.*)", line)
            if not m:
                fatal(
                    f'Doc comments (///) should start with "/// ".',
                    line_index=line_index,
                )

            self.output_lines.append(m.group(1))
            return

    def flush_metric(self):
        metric = self.metric
        (help_main, *help_details) = metric["help"].split(". ")
        metric_name = "_".join(
            [
                part
                for part in [
                    metric.get("namespace", "buildbuddy"),
                    metric.get("subsystem"),
                    metric.get("name"),
                ]
                if part is not None
            ]
        )
        self.output_lines.extend(
            [
                "",
                f'### **`{metric_name}`** ({metric["type"]})',
                "",
                f'{help_main}{"." if len(help_details) else ""}',
            ]
        )
        if len(help_details):
            self.output_lines.extend(["", f'{". ".join(help_details)}'])
        if "labels" in metric:
            self.output_lines.extend(["", "#### Labels", ""])
            for label in metric["labels"]:
                self.output_lines.append(
                    f'- **{label["name"]}**: {" ".join(label["comments"])}'
                )
            self.output_lines.append("")
        self.metric = {}

    def add_metric_label(self, line_index, label):
        metric = self.metric
        if metric is None:
            fatal(
                "Bad state: found metric label documentation outside a metric label.",
                line_index=line_index,
            )
        if "labels" not in metric:
            metric["labels"] = []
        metric["labels"].append({"name": label, "comments": self.label_comments})
        self.label_comments = []

    def set_metric_attr(self, name, value):
        if self.metric is None:
            self.metric = {}
        self.metric[name] = value


def main():
    if not os.path.exists("WORKSPACE"):
        fatal("Must be run from workspace root.")
    if len(sys.argv) >= 2 and sys.argv[1] in ["-w", "--watch"]:
        subprocess.run(
            f'''nodemon --watch '{sys.path[0]}/metrics.go' --watch '{sys.argv[0]}' --exec "python3 '{sys.argv[0]}'"''',
            shell=True,
        )
        sys.exit(0)
    lines = DocsGenerator(INPUT_METRICS_PATH).parse()
    with open(OUTPUT_PATH, "w") as f:
        f.write("\n".join(lines))
    subprocess.run([PRETTIER_TOOL_PATH, '--write'], check=True)


if __name__ == "__main__":
    main()
