ANALYZERS = [
    # "any",
    # "bloop", # DO NOT ENABLE, see golang/go#74967
    # "fmtappendf",
    # "forvar",
    # "mapsloop",
    # "minmax",
    # "newexpr",
    # "plusbuild",
    # "omitzero",
    # "rangeint",
    # "reflecttypefor",
    # "slicescontains",
    # "slicessort",
    # "stditerators",
    "stringscut",
    "stringscutprefix",
    "stringsseq",
    "stringsbuilder",
    # "testingcontext",
    # "waitgroup",
]

MODERNIZE_ANALYZERS = ["//rules/go/analyzer:" + analyzer for analyzer in ANALYZERS]
