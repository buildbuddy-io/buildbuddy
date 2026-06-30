import { getExecConfiguredPathLookupCandidates } from "./compact_exec_log_edges";

describe("getExecConfiguredPathLookupCandidates", () => {
  it("keeps the exact ST path and adds a canonical fallback for opt exec outputs", () => {
    expect(
      getExecConfiguredPathLookupCandidates("bazel-out/darwin_arm64-opt-exec-ST-d57f47055a04/bin/pkg/foo")
    ).toEqual([
      "bazel-out/darwin_arm64-opt-exec-ST-d57f47055a04/bin/pkg/foo",
      "bazel-out/darwin_arm64-opt-exec/bin/pkg/foo",
    ]);
  });

  it("keeps the exact ST path and adds a canonical fallback for non-default exec outputs", () => {
    expect(getExecConfiguredPathLookupCandidates("bazel-out/k8-dbg-exec-ST-abcd1234/bin/pkg/foo")).toEqual([
      "bazel-out/k8-dbg-exec-ST-abcd1234/bin/pkg/foo",
      "bazel-out/k8-dbg-exec/bin/pkg/foo",
    ]);
  });

  it("uses the canonical path as the only lookup candidate when there is no ST hash", () => {
    expect(getExecConfiguredPathLookupCandidates("bazel-out/my-platform-fastbuild-exec/bin/pkg/foo")).toEqual([
      "bazel-out/my-platform-fastbuild-exec/bin/pkg/foo",
    ]);
  });
});
