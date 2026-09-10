ANALYZERS = [
    "atomictypes",
    "reflecttypeassert",
    "errorsastype",
    "embedlit",
    "slicesclip",
    "slicesbackward",
    "any",
    # "bloop", # DO NOT ENABLE, see golang/go#74967
    # "fmtappendf",
    "forvar",
    "mapsloop",
    "minmax",
    "newexpr",
    # "plusbuild",
    # "omitzero",
    "rangeint",
    "reflecttypefor",
    "slicescontains",
    "slicessort",
    "stditerators",
    "stringscut",
    "stringscutprefix",
    "stringsseq",
    "stringsbuilder",
    # "testingcontext",
    # "waitgroup",
]

MODERNIZE_ANALYZERS = ["//rules/go/analyzer:" + analyzer for analyzer in ANALYZERS]

# Modernize repository-owned source, leaving generator and upstream output alone.
# Keep these exclusions local to modernize so other nogo checks still run.
MODERNIZE_CONFIG = {
    analyzer: {
        "exclude_files": {
            ".*\\.pb\\.go$": "generated protobuf sources",
            ".*/gazelle\\+/cmd/gazelle/.*": "third-party gazelle sources compiled by //cli/fix/langs:gazelle",
        },
    }
    for analyzer in ANALYZERS
}
