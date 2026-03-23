import { normalizeExecConfiguredPath } from "./compact_exec_log_edges";

describe("normalizeExecConfiguredPath", () => {
  it("strips the ST hash from opt exec outputs", () => {
    expect(normalizeExecConfiguredPath("bazel-out/darwin_arm64-opt-exec-ST-d57f47055a04/bin/pkg/foo")).toEqual(
      "bazel-out/darwin_arm64-opt-exec/bin/pkg/foo"
    );
  });

  it("strips the ST hash from non-default exec outputs", () => {
    expect(normalizeExecConfiguredPath("bazel-out/k8-dbg-exec-ST-abcd1234/bin/pkg/foo")).toEqual(
      "bazel-out/k8-dbg-exec/bin/pkg/foo"
    );
  });

  it("leaves canonical exec outputs unchanged", () => {
    expect(normalizeExecConfiguredPath("bazel-out/my-platform-fastbuild-exec/bin/pkg/foo")).toEqual(
      "bazel-out/my-platform-fastbuild-exec/bin/pkg/foo"
    );
  });
});
