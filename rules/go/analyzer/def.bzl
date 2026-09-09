# Keep this list aligned with the supported modernize.Suite.
# Upstream excludes appendclipped and slicesdelete (nilness changes),
# bloop (golang/go#74967), and fmtappendf (golang/go#77581).
ANALYZERS = [
    "any",
    "atomictypes",
    "embedlit",
    "errorsastype",
    "forvar",
    "importcomment",
    "mapsloop",
    "minmax",
    "newexpr",
    "omitzero",
    "plusbuild",
    "rangeint",
    "reflecttypeassert",
    "reflecttypefor",
    "slicesbackward",
    "slicesclip",
    "slicescontains",
    "slicessort",
    "stditerators",
    "stringsbuilder",
    "stringscut",
    "stringscutprefix",
    "stringsseq",
    "testingcontext",
    "unsafefuncs",
    "waitgroupgo",
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
