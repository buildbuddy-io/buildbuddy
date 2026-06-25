# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 20 | 20 | 65% | 90% | 0.76 | 100% | 100% | 1.00 | 0% | 40% | 0.18 | 0.44 | 90% |
| keyword | 12 | 12 | 8% | 67% | 0.30 | 17% | 58% | 0.38 | 33% | 58% | 0.47 | 0.42 | 42% |
| phrase | 8 | 8 | 0% | 12% | 0.06 | 50% | 75% | 0.66 | 50% | 88% | 0.68 | 0.12 | 12% |
| regex | 8 | 2 | 0% | 50% | 0.24 | 50% | 50% | 0.50 | 0% | 0% | 0.02 | 0.15 | 43% |
| filter | 6 | 2 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 0% | 50% | 0.25 | 0.42 | 67% |
| ranking | 8 | 0 | - | - | - | - | - | - | - | - | - | 0.20 | 25% |
| **total** | 62 | 44 | 36% | 68% | 0.50 | 66% | 82% | 0.75 | 18% | 52% | 0.35 | 0.33 | 54% |

1 local errors, 1 google errors, 1 zoekt errors (see results.jsonl).

## Queries where we miss the target but Google finds it

- `class NestedSet` (bz-sym-nestedset): local rank 10, google rank 1, target src/main/java/com/google/devtools/build/lib/collect/nestedset/NestedSet.java
- `class Label` (bz-sym-label): local rank 17, google rank 1, target src/main/java/com/google/devtools/build/lib/cmdline/Label.java
- `starlark eval` (bz-kw-starlark-evaluation): local rank 26, google rank 2, target src/main/java/net/starlark/java/eval/Eval.java
- `"no such package"` (bz-phr-no-such-package): local rank 26, google rank 1, target src/main/java/com/google/devtools/build/lib/packages/NoSuchPackageException.java
- `"no such target"` (bz-phr-no-such-target): local rank miss, google rank 1, target src/main/java/com/google/devtools/build/lib/packages/NoSuchTargetException.java
- `"missing input file"` (bz-phr-missing-input): local rank miss, google rank 7, target src/main/java/com/google/devtools/build/lib/skyframe/ArtifactFunction.java
- `"error loading package"` (bz-phr-error-loading-package): local rank 13, google rank 1, target src/main/java/com/google/devtools/build/lib/packages/BuildFileContainsErrorsException.java
- `"start the build"` (bz-phr-couldnt-start-build): local rank miss, google rank 1, target src/main/java/com/google/devtools/build/lib/runtime/commands/TestCommand.java
- `"Invalid label"` (bz-phr-invalid-label): local rank 17, google rank 2, target src/main/java/com/google/devtools/build/lib/rules/config/ConfigSetting.java
- `"is not defined"` (bz-phr-is-not-defined): local rank miss, google rank 9, target src/main/java/net/starlark/java/eval/Module.java
