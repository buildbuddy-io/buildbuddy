# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 20 | 20 | 70% | 85% | 0.79 | 100% | 100% | 1.00 | 0% | 60% | 0.24 | 0.39 | 85% |
| keyword | 12 | 12 | 17% | 58% | 0.31 | 17% | 58% | 0.38 | 25% | 75% | 0.39 | 0.36 | 75% |
| phrase | 8 | 8 | 38% | 50% | 0.44 | 50% | 75% | 0.66 | 50% | 88% | 0.68 | 0.32 | 57% |
| regex | 8 | 2 | 0% | 50% | 0.17 | 50% | 50% | 0.50 | 0% | 0% | 0.04 | 0.13 | 57% |
| filter | 6 | 2 | 50% | 50% | 0.56 | 100% | 100% | 1.00 | 0% | 50% | 0.25 | 0.55 | 67% |
| ranking | 8 | 0 | - | - | - | - | - | - | - | - | - | 0.30 | 62% |
| **total** | 62 | 44 | 45% | 68% | 0.56 | 66% | 82% | 0.75 | 16% | 66% | 0.35 | 0.35 | 72% |

1 local errors, 1 google errors, 1 zoekt errors (see results.jsonl).

## Queries where we miss the target but Google finds it

- `RuleContext` (bz-sym-rulecontext): local rank 9, google rank 1, target src/main/java/com/google/devtools/build/lib/analysis/RuleContext.java
- `SkyframeExecutor` (bz-sym-skyframeexecutor): local rank 17, google rank 1, target src/main/java/com/google/devtools/build/lib/skyframe/SkyframeExecutor.java
- `class Label` (bz-sym-label): local rank 6, google rank 1, target src/main/java/com/google/devtools/build/lib/cmdline/Label.java
- `build event protocol` (bz-kw-build-event-protocol): local rank miss, google rank 20, target src/main/java/com/google/devtools/build/lib/buildeventservice/BuildEventServiceModule.java
- `"missing input file"` (bz-phr-missing-input): local rank miss, google rank 7, target src/main/java/com/google/devtools/build/lib/skyframe/ArtifactFunction.java
- `"start the build"` (bz-phr-couldnt-start-build): local rank miss, google rank 1, target src/main/java/com/google/devtools/build/lib/runtime/commands/TestCommand.java
- `"Invalid label"` (bz-phr-invalid-label): local rank 32, google rank 2, target src/main/java/com/google/devtools/build/lib/rules/config/ConfigSetting.java
- `"is not defined"` (bz-phr-is-not-defined): local rank miss, google rank 9, target src/main/java/net/starlark/java/eval/Module.java
- `lang:java file:analysis RuleContext` (bz-flt-rulecontext-analysis): local rank 9, google rank 1, target src/main/java/com/google/devtools/build/lib/analysis/RuleContext.java
