load(":defs.bzl", "cache_benchmark")

package(default_visibility = ["//enterprise:__subpackages__"])

cache_benchmark(
    name = "cache_set_test",
    benchmark = "BenchmarkSet",
)

cache_benchmark(
    name = "cache_read_test",
    benchmark = "BenchmarkRead",
)

cache_benchmark(
    name = "cache_get_single_test",
    benchmark = "BenchmarkGetSingle",
)

cache_benchmark(
    name = "cache_get_multi_test",
    benchmark = "BenchmarkGetMulti",
)

cache_benchmark(
    name = "cache_find_missing_test",
    benchmark = "BenchmarkFindMissing",
)

cache_benchmark(
    name = "cache_parallel_test",
    benchmark = "BenchmarkParallel",
)

test_suite(
    name = "cache_test",
    tests = [
        ":cache_find_missing_test",
        ":cache_get_multi_test",
        ":cache_get_single_test",
        ":cache_parallel_test",
        ":cache_read_test",
        ":cache_set_test",
    ],
)
