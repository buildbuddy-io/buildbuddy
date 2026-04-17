package(default_visibility = ["//visibility:public"])

# Aggregated filegroup for every Grafana dashboard generated from Go source
# under this tree. Downstream consumers (e.g. the grafana dashboards container
# layer in the internal repo) reference this single label instead of listing
# each generator individually.
#
# Register a new generator by adding its genrule output below.
filegroup(
    name = "all_dashboards",
    srcs = [
        "//tools/metrics/grafana/generated/baremetal_networking:baremetal-networking.json",
        "//tools/metrics/grafana/generated/disk_performance:disk-performance.json",
    ],
)
