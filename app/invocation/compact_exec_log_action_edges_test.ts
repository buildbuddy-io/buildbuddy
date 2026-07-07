import Long from "long";
import { tools } from "../../proto/spawn_ts_proto";
import {
  buildCompactExecLogActionEdgesIndex,
  compareTargetLabelsByRelevance,
  findCompactExecLogAction,
  getCompactExecLogActionEdgesIndex,
  getExecConfiguredPathLookupCandidates,
} from "./compact_exec_log_action_edges";

describe("compact execution log action edges", () => {
  it("links producer outputs to consumer inputs", () => {
    const index = buildCompactExecLogActionEdgesIndex([
      file(1, "src/input.txt"),
      inputSet(2, [1]),
      file(3, "bazel-out/bin/pkg/lib.a"),
      spawn(4, {
        digest: digest("compile", 1),
        inputSetId: 2,
        mnemonic: "CppCompile",
        outputs: [output(3)],
        targetLabel: "//pkg:lib",
      }),
      inputSet(5, [3]),
      file(6, "bazel-out/bin/pkg/app"),
      spawn(7, {
        digest: digest("link", 1),
        inputSetId: 5,
        mnemonic: "CppLink",
        outputs: [output(6)],
        targetLabel: "//pkg:app",
      }),
    ]);

    const compile = requireDefined(findCompactExecLogAction(index, digest("compile", 1)), "compile action");
    const link = requireDefined(findCompactExecLogAction(index, digest("link", 1)), "link action");

    expect(compile.childIds).toEqual([link.id]);
    expect(link.parentIds).toEqual([compile.id]);
    expect(compile.childEdges).toEqual([
      {
        actionId: link.id,
        artifactId: 3,
        artifactPath: "bazel-out/bin/pkg/lib.a",
      },
    ]);
    expect(link.parentEdges).toEqual([
      {
        actionId: compile.id,
        artifactId: 3,
        artifactPath: "bazel-out/bin/pkg/lib.a",
      },
    ]);
  });

  it("keeps the consumed output path when one action has several outputs", () => {
    const index = buildCompactExecLogActionEdgesIndex([
      file(1, "bazel-out/bin/pkg/lib.a"),
      file(2, "bazel-out/bin/pkg/lib.h"),
      spawn(3, {
        digest: digest("generate", 1),
        mnemonic: "Genrule",
        outputs: [output(1), output(2)],
        targetLabel: "//pkg:generate",
      }),
      inputSet(4, [1]),
      spawn(5, {
        digest: digest("archive", 1),
        mnemonic: "Archive",
        targetLabel: "//pkg:archive",
        inputSetId: 4,
      }),
      inputSet(6, [2]),
      spawn(7, {
        digest: digest("compile", 1),
        mnemonic: "CppCompile",
        targetLabel: "//pkg:compile",
        inputSetId: 6,
      }),
    ]);

    const generate = requireDefined(findCompactExecLogAction(index, digest("generate", 1)), "generate action");
    const archive = requireDefined(findCompactExecLogAction(index, digest("archive", 1)), "archive action");
    const compile = requireDefined(findCompactExecLogAction(index, digest("compile", 1)), "compile action");

    expect(generate.childIds).toEqual([archive.id, compile.id]);
    expect(generate.childEdges).toEqual([
      {
        actionId: archive.id,
        artifactId: 1,
        artifactPath: "bazel-out/bin/pkg/lib.a",
      },
      {
        actionId: compile.id,
        artifactId: 2,
        artifactPath: "bazel-out/bin/pkg/lib.h",
      },
    ]);
  });

  it("disambiguates repeated mnemonics within the same target", () => {
    const index = buildCompactExecLogActionEdgesIndex([
      file(1, "bazel-out/k8-fastbuild/bin/pkg/lib.a"),
      spawn(2, {
        digest: digest("compile-fastbuild", 1),
        mnemonic: "GoCompilePkg",
        outputs: [output(1)],
        targetLabel: "//pkg:lib",
      }),
      file(3, "bazel-out/k8-fastbuild/bin/pkg/lib.x"),
      spawn(4, {
        digest: digest("compile-x", 1),
        mnemonic: "GoCompilePkg",
        outputs: [output(3)],
        targetLabel: "//pkg:lib",
      }),
      file(5, "bazel-out/k8-opt/bin/pkg/lib.a"),
      spawn(6, {
        digest: digest("compile-opt", 1),
        mnemonic: "GoCompilePkg",
        outputs: [output(5)],
        targetLabel: "//pkg:lib",
      }),
    ]);

    expect(findCompactExecLogAction(index, digest("compile-fastbuild", 1))?.label).toEqual(
      "GoCompilePkg (k8-fastbuild)"
    );
    expect(findCompactExecLogAction(index, digest("compile-x", 1))?.label).toEqual("GoCompilePkg (lib.x)");
    expect(findCompactExecLogAction(index, digest("compile-opt", 1))?.label).toEqual("GoCompilePkg (k8-opt)");
  });

  it("links tool inputs and transitive input sets", () => {
    const index = buildCompactExecLogActionEdgesIndex([
      file(1, "bazel-out/bin/tools/compiler"),
      spawn(2, {
        digest: digest("tool", 1),
        mnemonic: "GoLink",
        outputs: [output(1)],
        targetLabel: "//tools:compiler",
      }),
      inputSet(3, [1]),
      inputSet(4, [], [3]),
      file(5, "bazel-out/bin/pkg/out"),
      spawn(6, {
        digest: digest("compile", 1),
        mnemonic: "GoCompilePkg",
        outputs: [output(5)],
        targetLabel: "//pkg:pkg",
        toolSetId: 4,
      }),
    ]);

    const tool = requireDefined(findCompactExecLogAction(index, digest("tool", 1)), "tool action");
    const compile = requireDefined(findCompactExecLogAction(index, digest("compile", 1)), "compile action");

    expect(tool.childIds).toEqual([compile.id]);
    expect(compile.parentIds).toEqual([tool.id]);
  });

  it("can find actions by digest while preferring target and mnemonic matches", () => {
    const sharedDigest = digest("same", 1);
    const index = buildCompactExecLogActionEdgesIndex([
      spawn(1, {
        digest: sharedDigest,
        mnemonic: "Javac",
        targetLabel: "//pkg:a",
      }),
      spawn(2, {
        digest: sharedDigest,
        mnemonic: "Javac",
        targetLabel: "//pkg:b",
      }),
    ]);

    expect(findCompactExecLogAction(index, sharedDigest, "//pkg:b")?.targetLabel).toEqual("//pkg:b");
  });

  it("adds canonical exec path fallback candidates for ST-hashed paths", () => {
    expect(
      getExecConfiguredPathLookupCandidates("bazel-out/darwin_arm64-opt-exec-ST-d57f47055a04/bin/pkg/foo")
    ).toEqual([
      "bazel-out/darwin_arm64-opt-exec-ST-d57f47055a04/bin/pkg/foo",
      "bazel-out/darwin_arm64-opt-exec/bin/pkg/foo",
    ]);
  });

  it("sorts target labels by relevance to the current action target", () => {
    const targetLabels = [
      "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder",
      "//server/other:tool",
      "unlabeled action",
      "//server/util/shlex:shlex_test",
      "//server/util/shlex:helper",
      "//server/util:library",
      "@repo//server/util/shlex:external_tool",
    ];

    expect(targetLabels.sort((a, b) => compareTargetLabelsByRelevance(a, b, "//server/util/shlex:shlex_test"))).toEqual(
      [
        "//server/util/shlex:shlex_test",
        "//server/util/shlex:helper",
        "//server/other:tool",
        "//server/util:library",
        "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder",
        "@repo//server/util/shlex:external_tool",
        "unlabeled action",
      ]
    );
  });

  it("sorts external labels in the same external workspace before unrelated labels", () => {
    const targetLabels = [
      "//server/util/shlex:local_tool",
      "@@rules_go++go_sdk+main___download_0_linux_amd64//other:tool",
      "@@other_repo//:tool",
      "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder_reset",
      "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder",
    ];

    expect(
      targetLabels.sort((a, b) =>
        compareTargetLabelsByRelevance(a, b, "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder_reset")
      )
    ).toEqual([
      "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder_reset",
      "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder",
      "@@rules_go++go_sdk+main___download_0_linux_amd64//other:tool",
      "//server/util/shlex:local_tool",
      "@@other_repo//:tool",
    ]);
  });

  it("links executable symlinks back to their source action", () => {
    const builderSourcePath =
      "bazel-out/k8-opt-exec-ST-d57f47055a04/bin/external/" +
      "rules_go++go_sdk+main___download_0_linux_amd64/builder_reset/builder";
    const builderCanonicalPath =
      "bazel-out/k8-opt-exec/bin/external/" + "rules_go++go_sdk+main___download_0_linux_amd64/builder_reset/builder";
    const builderSymlinkPath =
      "bazel-out/k8-opt-exec/bin/external/" +
      "rules_go++go_sdk+main___download_0_linux_amd64/builder_reset/builder_link";

    const index = buildCompactExecLogActionEdgesIndex([
      file(1, builderSourcePath),
      spawn(2, {
        digest: digest("builder", 1),
        mnemonic: "GoLink",
        outputs: [output(1)],
        targetLabel: "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder",
      }),
      unresolvedSymlink(3, builderSymlinkPath, builderCanonicalPath),
      symlinkAction(4, {
        inputPath: builderCanonicalPath,
        outputPath: builderSymlinkPath,
        mnemonic: "ExecutableSymlink",
        targetLabel: "@@rules_go++go_sdk+main___download_0_linux_amd64//:builder_reset",
      }),
      inputSet(5, [3]),
      spawn(6, {
        digest: digest("compile", 1),
        inputSetId: 5,
        mnemonic: "GoCompilePkg",
        targetLabel: "//pkg:compile",
      }),
    ]);

    const builder = requireDefined(findCompactExecLogAction(index, digest("builder", 1)), "builder action");
    const compile = requireDefined(findCompactExecLogAction(index, digest("compile", 1)), "compile action");
    const symlink = requireDefined(
      index.actions.find((action) => action.kind === "symlink"),
      "symlink action"
    );

    expect(symlink.parentIds).toEqual([builder.id]);
    expect(builder.childIds).toEqual([symlink.id]);
    expect(symlink.parentEdges).toEqual([
      {
        actionId: builder.id,
        artifactId: 1,
        artifactPath: builderSourcePath,
      },
    ]);
    expect(symlink.childIds).toEqual([compile.id]);
    expect(compile.parentIds).toEqual([symlink.id]);
    expect(compile.parentEdges).toEqual([
      {
        actionId: symlink.id,
        artifactId: 3,
        artifactPath: builderSymlinkPath,
      },
    ]);
  });

  it("caches the built index by compact execution log URI", async () => {
    let loadCount = 0;
    const entries = [
      spawn(1, {
        digest: digest("compile", 1),
        mnemonic: "CppCompile",
        targetLabel: "//pkg:lib",
      }),
    ];

    const first = await getCompactExecLogActionEdgesIndex("test://cache-hit", async () => {
      loadCount++;
      return entries;
    });
    const second = await getCompactExecLogActionEdgesIndex("test://cache-hit", async () => {
      loadCount++;
      return [];
    });

    expect(loadCount).toEqual(1);
    expect(second).toBe(first);
    expect(second.actions.length).toEqual(1);
  });
});

function file(id: number, path: string) {
  return new tools.protos.ExecLogEntry({
    id,
    file: new tools.protos.ExecLogEntry.File({ path }),
  });
}

function unresolvedSymlink(id: number, path: string, targetPath: string) {
  return new tools.protos.ExecLogEntry({
    id,
    unresolvedSymlink: new tools.protos.ExecLogEntry.UnresolvedSymlink({ path, targetPath }),
  });
}

function inputSet(id: number, inputIds: number[], transitiveSetIds: number[] = []) {
  return new tools.protos.ExecLogEntry({
    id,
    inputSet: new tools.protos.ExecLogEntry.InputSet({ inputIds, transitiveSetIds }),
  });
}

function output(outputId: number) {
  return new tools.protos.ExecLogEntry.Output({ outputId });
}

function spawn(id: number, properties: tools.protos.ExecLogEntry.ISpawn) {
  return new tools.protos.ExecLogEntry({
    id,
    spawn: new tools.protos.ExecLogEntry.Spawn(properties),
  });
}

function symlinkAction(id: number, properties: tools.protos.ExecLogEntry.ISymlinkAction) {
  return new tools.protos.ExecLogEntry({
    id,
    symlinkAction: new tools.protos.ExecLogEntry.SymlinkAction(properties),
  });
}

function digest(hash: string, sizeBytes: number) {
  return new tools.protos.Digest({ hash, sizeBytes: Long.fromNumber(sizeBytes) });
}

function requireDefined<T>(value: T | null | undefined, description: string): T {
  if (value == null) {
    throw new Error(`Expected ${description} to be defined`);
  }
  return value;
}
