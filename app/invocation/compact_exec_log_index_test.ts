import Long from "long";
import { tools } from "../../proto/spawn_ts_proto";
import {
  buildCompactExecLogIndex,
  findSpawnEntryByDigest,
  getCompactExecLogDirectoryHash,
  getCompactExecLogDirectorySize,
} from "./compact_exec_log_index";

describe("compact execution log index", () => {
  it("groups entries and builds reusable lookups and spawn facets", () => {
    const entries = [
      file(1, "src/input.txt"),
      directory(2, "tree", [new tools.protos.ExecLogEntry.File({ path: "nested.txt" })]),
      unresolvedSymlink(3, "link", "target"),
      inputSet(4, [1]),
      spawn(5, {
        digest: digest("compile", 10),
        mnemonic: "CppCompile",
        runner: "remote",
        platform: { properties: [{ name: "cpu", value: "x86_64" }] },
      }),
    ];

    const index = buildCompactExecLogIndex(entries);

    expect(index.entries).toBe(entries);
    expect(index.fileEntries).toEqual([entries[0]]);
    expect(index.directoryEntries.map((directory) => directory.entry)).toEqual([entries[1]]);
    expect(index.unresolvedSymlinkEntries).toEqual([entries[2]]);
    expect(index.spawnEntries).toEqual([entries[4]]);
    expect(index.filesById.get(1)?.path).toEqual("src/input.txt");
    expect(index.directoriesById.get(2)?.path).toEqual("tree");
    expect(index.unresolvedSymlinksById.get(3)?.targetPath).toEqual("target");
    expect(index.inputSetsById.get(4)?.inputIds).toEqual([1]);
    expect(index.spawnsById.get(5)?.mnemonic).toEqual("CppCompile");
    expect(index.spawnMnemonics).toEqual(new Set(["CppCompile"]));
    expect(index.spawnRunners).toEqual(new Set(["remote"]));
    expect(index.spawnPlatformPropertyValues.get("cpu")).toEqual(new Set(["x86_64"]));
  });

  it("precomputes directory sizes without losing Long precision and lazily computes hashes", () => {
    const index = buildCompactExecLogIndex([
      directory(1, "large", [
        new tools.protos.ExecLogEntry.File({ digest: digest("a", "9007199254740992") }),
        new tools.protos.ExecLogEntry.File({ digest: digest("b", 5) }),
      ]),
    ]);

    const directoryEntry = index.directoryEntries[0];
    expect(directoryEntry.sizeBytes).toBeUndefined();
    expect(getCompactExecLogDirectorySize(directoryEntry).toString()).toEqual("9007199254740997");
    expect(directoryEntry.sizeBytes?.toString()).toEqual("9007199254740997");
    expect(directoryEntry.hash).toBeUndefined();
    expect(getCompactExecLogDirectoryHash(directoryEntry)).toBeTruthy();
    expect(directoryEntry.hash).toBeDefined();
  });

  it("finds spawn entries by digest hash and optionally matches the size", () => {
    const small = spawn(1, { digest: digest("shared", 1), mnemonic: "Small" });
    const large = spawn(2, { digest: digest("shared", 2), mnemonic: "Large" });
    const index = buildCompactExecLogIndex([small, large]);

    expect(findSpawnEntryByDigest(index, digest("shared", 2))).toBe(large);
    expect(findSpawnEntryByDigest(index, digest("shared", 999))).toBeUndefined();
    expect(findSpawnEntryByDigest(index, digest("shared", 999), false)).toBe(small);
  });
});

function file(id: number, path: string) {
  return new tools.protos.ExecLogEntry({ id, file: new tools.protos.ExecLogEntry.File({ path }) });
}

function directory(id: number, path: string, files: tools.protos.ExecLogEntry.File[]) {
  return new tools.protos.ExecLogEntry({
    id,
    directory: new tools.protos.ExecLogEntry.Directory({ path, files }),
  });
}

function unresolvedSymlink(id: number, path: string, targetPath: string) {
  return new tools.protos.ExecLogEntry({
    id,
    unresolvedSymlink: new tools.protos.ExecLogEntry.UnresolvedSymlink({ path, targetPath }),
  });
}

function inputSet(id: number, inputIds: number[]) {
  return new tools.protos.ExecLogEntry({
    id,
    inputSet: new tools.protos.ExecLogEntry.InputSet({ inputIds }),
  });
}

function spawn(id: number, properties: tools.protos.ExecLogEntry.ISpawn) {
  return new tools.protos.ExecLogEntry({
    id,
    spawn: new tools.protos.ExecLogEntry.Spawn(properties),
  });
}

function digest(hash: string, sizeBytes: number | string) {
  return new tools.protos.Digest({ hash, sizeBytes: Long.fromValue(sizeBytes) });
}
