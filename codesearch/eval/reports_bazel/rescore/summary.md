# Search quality: local codesearch vs cs.opensource.google

Target metrics are over queries with labeled target files (n_t). `g@1∈L5` = how often Google's top result appears in our top 5; `J@10` = mean Jaccard overlap of top-10 file sets.

| category | n | n_t | local R@1 | local R@5 | local MRR | google R@1 | google R@5 | google MRR | zoekt R@1 | zoekt R@5 | zoekt MRR | J@10 | g@1∈L5 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| symbol | 20 | 20 | 70% | 90% | 0.79 | 100% | 100% | 1.00 | 95% | 100% | 0.97 | 0.39 | 90% |
| keyword | 12 | 12 | 17% | 58% | 0.31 | 17% | 58% | 0.38 | 17% | 50% | 0.32 | 0.36 | 75% |
| phrase | 8 | 8 | 38% | 62% | 0.52 | 50% | 75% | 0.66 | 25% | 62% | 0.43 | 0.37 | 71% |
| regex | 8 | 2 | 0% | 50% | 0.17 | 50% | 50% | 0.50 | 100% | 100% | 1.00 | 0.15 | 57% |
| filter | 6 | 2 | 50% | 50% | 0.56 | 100% | 100% | 1.00 | 100% | 100% | 1.00 | 0.56 | 67% |
| ranking | 8 | 0 | - | - | - | - | - | - | - | - | - | 0.30 | 62% |
| **total** | 62 | 44 | 45% | 73% | 0.57 | 66% | 82% | 0.75 | 61% | 80% | 0.70 | 0.36 | 75% |

1 local errors, 1 google errors, 1 zoekt errors (see results.jsonl).

## Queries where we miss the target but Google finds it

- `RuleContext` (bz-sym-rulecontext): local rank 9, google rank 1, target src/main/java/com/google/devtools/build/lib/analysis/RuleContext.java
- `SkyframeExecutor` (bz-sym-skyframeexecutor): local rank 17, google rank 1, target src/main/java/com/google/devtools/build/lib/skyframe/SkyframeExecutor.java
- `build event protocol` (bz-kw-build-event-protocol): local rank miss, google rank 20, target src/main/java/com/google/devtools/build/lib/buildeventservice/BuildEventServiceModule.java
- `"start the build"` (bz-phr-couldnt-start-build): local rank miss, google rank 1, target src/main/java/com/google/devtools/build/lib/runtime/commands/TestCommand.java
- `"Invalid label"` (bz-phr-invalid-label): local rank 32, google rank 2, target src/main/java/com/google/devtools/build/lib/rules/config/ConfigSetting.java
- `lang:java file:analysis RuleContext` (bz-flt-rulecontext-analysis): local rank 9, google rank 1, target src/main/java/com/google/devtools/build/lib/analysis/RuleContext.java
