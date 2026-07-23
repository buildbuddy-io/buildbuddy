import Long from "long";
import { tools } from "../../proto/spawn_ts_proto";

export interface CompactExecLogDirectoryEntry {
  entry: tools.protos.ExecLogEntry;
  sizeBytes?: Long;
  hash?: string;
}

export interface CompactExecLogIndex {
  entries: tools.protos.ExecLogEntry[];
  fileEntries: tools.protos.ExecLogEntry[];
  directoryEntries: CompactExecLogDirectoryEntry[];
  unresolvedSymlinkEntries: tools.protos.ExecLogEntry[];
  spawnEntries: tools.protos.ExecLogEntry[];
  filesById: Map<number, tools.protos.ExecLogEntry.File>;
  directoriesById: Map<number, tools.protos.ExecLogEntry.Directory>;
  unresolvedSymlinksById: Map<number, tools.protos.ExecLogEntry.UnresolvedSymlink>;
  runfilesTreesById: Map<number, tools.protos.ExecLogEntry.RunfilesTree>;
  inputSetsById: Map<number, tools.protos.ExecLogEntry.InputSet>;
  symlinkEntrySetsById: Map<number, tools.protos.ExecLogEntry.SymlinkEntrySet>;
  spawnsById: Map<number, tools.protos.ExecLogEntry.Spawn>;
  symlinkActionsById: Map<number, tools.protos.ExecLogEntry.SymlinkAction>;
  spawnEntriesByDigestHash: Map<string, tools.protos.ExecLogEntry[]>;
  spawnMnemonics: Set<string>;
  spawnRunners: Set<string>;
  spawnPlatformPropertyValues: Map<string, Set<string>>;
}

/** Builds the shared, read-optimized representation of a decoded compact execution log. */
export function buildCompactExecLogIndex(entries: tools.protos.ExecLogEntry[]): CompactExecLogIndex {
  const index: CompactExecLogIndex = {
    entries,
    fileEntries: [],
    directoryEntries: [],
    unresolvedSymlinkEntries: [],
    spawnEntries: [],
    filesById: new Map(),
    directoriesById: new Map(),
    unresolvedSymlinksById: new Map(),
    runfilesTreesById: new Map(),
    inputSetsById: new Map(),
    symlinkEntrySetsById: new Map(),
    spawnsById: new Map(),
    symlinkActionsById: new Map(),
    spawnEntriesByDigestHash: new Map(),
    spawnMnemonics: new Set(),
    spawnRunners: new Set(),
    spawnPlatformPropertyValues: new Map(),
  };

  for (const entry of entries) {
    const id = Number(entry.id || 0);
    if (entry.file) {
      index.fileEntries.push(entry);
      if (id) index.filesById.set(id, entry.file);
      continue;
    }
    if (entry.directory) {
      index.directoryEntries.push({ entry });
      if (id) index.directoriesById.set(id, entry.directory);
      continue;
    }
    if (entry.unresolvedSymlink) {
      index.unresolvedSymlinkEntries.push(entry);
      if (id) index.unresolvedSymlinksById.set(id, entry.unresolvedSymlink);
      continue;
    }
    if (entry.runfilesTree) {
      if (id) index.runfilesTreesById.set(id, entry.runfilesTree);
      continue;
    }
    if (entry.inputSet) {
      if (id) index.inputSetsById.set(id, entry.inputSet);
      continue;
    }
    if (entry.symlinkEntrySet) {
      if (id) index.symlinkEntrySetsById.set(id, entry.symlinkEntrySet);
      continue;
    }
    if (entry.spawn) {
      index.spawnEntries.push(entry);
      if (id) index.spawnsById.set(id, entry.spawn);
      if (entry.spawn.digest?.hash) {
        let entriesForDigest = index.spawnEntriesByDigestHash.get(entry.spawn.digest.hash);
        if (!entriesForDigest) {
          entriesForDigest = [];
          index.spawnEntriesByDigestHash.set(entry.spawn.digest.hash, entriesForDigest);
        }
        entriesForDigest.push(entry);
      }
      if (entry.spawn.mnemonic) index.spawnMnemonics.add(entry.spawn.mnemonic);
      if (entry.spawn.runner) index.spawnRunners.add(entry.spawn.runner);
      for (const property of entry.spawn.platform?.properties || []) {
        if (!property.name || !property.value) continue;
        let values = index.spawnPlatformPropertyValues.get(property.name);
        if (!values) {
          values = new Set();
          index.spawnPlatformPropertyValues.set(property.name, values);
        }
        values.add(property.value);
      }
      continue;
    }
    if (entry.symlinkAction && id) {
      index.symlinkActionsById.set(id, entry.symlinkAction);
    }
  }

  return index;
}

export function findSpawnEntryByDigest(
  index: CompactExecLogIndex,
  digest: { hash?: string | null; sizeBytes?: unknown } | null | undefined,
  matchSize: boolean = true
): tools.protos.ExecLogEntry | undefined {
  if (!digest?.hash) return undefined;
  const candidates = index.spawnEntriesByDigestHash.get(digest.hash) || [];
  if (!matchSize) return candidates[0];
  const sizeBytes = String(digest.sizeBytes || 0);
  return candidates.find((entry) => String(entry.spawn?.digest?.sizeBytes || 0) === sizeBytes);
}

export function getCompactExecLogDirectoryHash(directory: CompactExecLogDirectoryEntry): string {
  directory.hash ??= hashFileHashes(directory.entry.directory?.files);
  return directory.hash;
}

export function getCompactExecLogDirectorySize(directory: CompactExecLogDirectoryEntry): Long {
  directory.sizeBytes ??= sumFileSizes(directory.entry.directory?.files || []);
  return directory.sizeBytes;
}

function sumFileSizes(files: tools.protos.ExecLogEntry.File[]): Long {
  return files.reduce((sum, file) => sum.add(file.digest?.sizeBytes || 0), Long.ZERO);
}

function hashFileHashes(files: tools.protos.ExecLogEntry.File[] | undefined): string {
  // Keep this lightweight synthetic directory digest consistent with the Files tab's existing behavior.
  return (files || [])
    .map((file) => file.digest?.hash || "")
    .reduce((a, b) => {
      let hash = "";
      for (let i = 0; i < Math.max(a.length, b.length); i++) {
        const char = (a.charCodeAt(i) + b.charCodeAt(i)) % 36;
        hash += String.fromCharCode(char < 26 ? char + 97 : char + 22);
      }
      return hash;
    }, "");
}
