load("//tools/metrics/grafana:dashboards.bzl", "DASHBOARD_NAMES")

# Generate VictoriaMetrics dashboards in this package for each
# dashboard under ../dashboards.

package(default_visibility = ["//visibility:public"])

[
    genrule(
        name = "%s.json__gen" % name,
        srcs = ["//tools/metrics/grafana:dashboards/%s.json" % name],
        outs = ["%s.json" % name],
        cmd_bash = """
            python3 $(location //tools/metrics:process_dashboard.py) --data_source_uid=vm <$(SRCS) >$@
        """,
        tools = ["//tools/metrics:process_dashboard.py"],
    )
    for name in DASHBOARD_NAMES
]

filegroup(
    name = "dashboard_files",
    srcs = [":%s.json" % name for name in DASHBOARD_NAMES],
)
