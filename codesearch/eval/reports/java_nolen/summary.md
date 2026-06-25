# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 20 | 20 | 90% | 100% | 0.94 | 100% | 100% | 1.00 | 0.40 | 100% |
| keyword | 12 | 12 | 8% | 58% | 0.27 | 17% | 58% | 0.38 | 0.36 | 75% |
| phrase | 8 | 8 | 38% | 50% | 0.44 | 50% | 75% | 0.66 | 0.32 | 57% |
| regex | 8 | 2 | 0% | 50% | 0.17 | 50% | 50% | 0.50 | 0.13 | 57% |
| filter | 6 | 2 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 0.54 | 67% |
| ranking | 8 | 0 | - | - | - | - | - | - | 0.29 | 75% |
| **total** | 62 | 44 | 55% | 77% | 0.63 | 66% | 82% | 0.75 | 0.35 | 78% |

1 local errors, 1 google errors, 0 zoekt errors (see results.jsonl).

## Queries where we miss the target but Google finds it

- `build event protocol` (bz-kw-build-event-protocol): local rank miss, google rank 20, target src/main/java/com/google/devtools/build/lib/buildeventservice/BuildEventServiceModule.java
- `"missing input file"` (bz-phr-missing-input): local rank miss, google rank 7, target src/main/java/com/google/devtools/build/lib/skyframe/ArtifactFunction.java
- `"start the build"` (bz-phr-couldnt-start-build): local rank miss, google rank 1, target src/main/java/com/google/devtools/build/lib/runtime/commands/TestCommand.java
- `"Invalid label"` (bz-phr-invalid-label): local rank 32, google rank 2, target src/main/java/com/google/devtools/build/lib/rules/config/ConfigSetting.java
- `"is not defined"` (bz-phr-is-not-defined): local rank miss, google rank 9, target src/main/java/net/starlark/java/eval/Module.java
