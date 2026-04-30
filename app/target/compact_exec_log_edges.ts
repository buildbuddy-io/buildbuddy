import { tools } from "../../proto/spawn_ts_proto";

const CACHE_VERSION = 8;
const DB_NAME = "buildbuddy-compact-exec-log-cache";
const DB_VERSION = 2;
const STORE_NAME = "target-edges";

const memoryCache = new Map<string, CompactExecLogEdgesIndex>();
const pendingLoads = new Map<string, Promise<CompactExecLogEdgesIndex>>();

export interface RelatedTargetSummary {
  targetLabel: string;
  actionIds: string[];
}

export interface CompactExecLogActionSummary {
  id: string;
  kind: "spawn" | "symlink";
  targetLabel: string;
  label: string;
  mnemonic: string;
  cached: boolean;
  result: "succeeded" | "failed";
  primaryOutputPath?: string;
  digestHash?: string;
  digestSizeBytes?: string;
  parents: RelatedTargetSummary[];
  children: RelatedTargetSummary[];
}

export interface CompactExecLogTargetSummary {
  label: string;
  actionIds: string[];
  parents: RelatedTargetSummary[];
  children: RelatedTargetSummary[];
}

export interface CompactExecLogEdgesIndex {
  version: number;
  cacheKey: string;
  createdAtMs: number;
  actions: Record<string, CompactExecLogActionSummary>;
  targets: Record<string, CompactExecLogTargetSummary>;
}

type ActionKind = CompactExecLogActionSummary["kind"];

interface IndexBuildState {
  files: Map<number, tools.protos.ExecLogEntry.File>;
  directories: Map<number, tools.protos.ExecLogEntry.Directory>;
  symlinks: Map<number, tools.protos.ExecLogEntry.UnresolvedSymlink>;
  runfilesTrees: Map<number, tools.protos.ExecLogEntry.RunfilesTree>;
  inputSets: Map<number, tools.protos.ExecLogEntry.InputSet>;
  symlinkEntrySets: Map<number, tools.protos.ExecLogEntry.SymlinkEntrySet>;
  spawns: Map<number, tools.protos.ExecLogEntry.Spawn>;
  symlinkActions: Map<number, tools.protos.ExecLogEntry.SymlinkAction>;
  actions: Map<string, MutableActionSummary>;
  targets: Map<string, MutableTargetSummary>;
  pathToArtifactId: Map<string, number>;
  artifactProducers: Map<number, string>;
  expandedInputSets: Map<number, Set<number>>;
  expandedSymlinkEntrySets: Map<number, Map<string, number>>;
}

interface MutableActionSummary {
  id: string;
  kind: ActionKind;
  rawOrder: number;
  targetLabel: string;
  label: string;
  mnemonic: string;
  cached: boolean;
  result: "succeeded" | "failed";
  primaryOutputPath?: string;
  digestHash?: string;
  digestSizeBytes?: string;
  parentMap: Map<string, Set<string>>;
  childMap: Map<string, Set<string>>;
}

interface MutableTargetSummary {
  label: string;
  actionIds: Set<string>;
  parentMap: Map<string, Set<string>>;
  childMap: Map<string, Set<string>>;
}

interface ActionLabelMeta {
  actionId: string;
  mnemonic: string;
  filename: string;
  platform: string;
}

let dbPromise: Promise<IDBDatabase | null> | undefined;

export async function getCompactExecLogEdgesIndex(
  executionLogUri: string,
  loadEntries: () => Promise<tools.protos.ExecLogEntry[]>
): Promise<CompactExecLogEdgesIndex> {
  const cacheKey = `v${CACHE_VERSION}:${executionLogUri}`;

  const memoryHit = memoryCache.get(cacheKey);
  if (memoryHit) {
    return memoryHit;
  }

  const pending = pendingLoads.get(cacheKey);
  if (pending) {
    return pending;
  }

  const loadPromise = (async () => {
    const cached = await readCachedIndex(cacheKey);
    if (cached) {
      memoryCache.set(cacheKey, cached);
      return cached;
    }

    const entries = await loadEntries();
    const index = buildCompactExecLogEdgesIndex(entries, cacheKey);
    memoryCache.set(cacheKey, index);
    await writeCachedIndex(index);
    return index;
  })().finally(() => {
    pendingLoads.delete(cacheKey);
  });

  pendingLoads.set(cacheKey, loadPromise);
  return loadPromise;
}

function buildCompactExecLogEdgesIndex(
  entries: tools.protos.ExecLogEntry[],
  cacheKey: string
): CompactExecLogEdgesIndex {
  const state: IndexBuildState = {
    files: new Map(),
    directories: new Map(),
    symlinks: new Map(),
    runfilesTrees: new Map(),
    inputSets: new Map(),
    symlinkEntrySets: new Map(),
    spawns: new Map(),
    symlinkActions: new Map(),
    actions: new Map(),
    targets: new Map(),
    pathToArtifactId: new Map(),
    artifactProducers: new Map(),
    expandedInputSets: new Map(),
    expandedSymlinkEntrySets: new Map(),
  };

  let nextSpawnId = 1;
  let nextSymlinkActionId = 1;

  entries.forEach((entry, rawOrder) => {
    const id = Number(entry.id || 0);

    if (entry.file) {
      if (id) {
        state.files.set(id, entry.file);
      }
      if (id && entry.file.path) {
        state.pathToArtifactId.set(entry.file.path, id);
      }
      return;
    }

    if (entry.directory) {
      if (id) {
        state.directories.set(id, entry.directory);
      }
      if (id && entry.directory.path) {
        state.pathToArtifactId.set(entry.directory.path, id);
      }
      return;
    }

    if (entry.unresolvedSymlink) {
      if (id) {
        state.symlinks.set(id, entry.unresolvedSymlink);
      }
      if (id && entry.unresolvedSymlink.path) {
        state.pathToArtifactId.set(entry.unresolvedSymlink.path, id);
      }
      return;
    }

    if (entry.runfilesTree) {
      if (id) {
        state.runfilesTrees.set(id, entry.runfilesTree);
      }
      if (id && entry.runfilesTree.path) {
        state.pathToArtifactId.set(entry.runfilesTree.path, id);
      }
      return;
    }

    if (entry.inputSet) {
      if (id) {
        state.inputSets.set(id, entry.inputSet);
      }
      return;
    }

    if (entry.symlinkEntrySet) {
      if (id) {
        state.symlinkEntrySets.set(id, entry.symlinkEntrySet);
      }
      return;
    }

    if (entry.spawn) {
      const spawnId = id || nextSpawnId++;
      const actionId = makeActionId("spawn", spawnId);
      state.spawns.set(spawnId, entry.spawn);
      const action = createSpawnSummary(actionId, entry.spawn, rawOrder);
      state.actions.set(actionId, action);
      registerTargetAction(action.targetLabel, actionId, state);

      for (const output of entry.spawn.outputs || []) {
        const outputId =
          Number(output.outputId || 0) ||
          Number(output.fileId || 0) ||
          Number(output.directoryId || 0) ||
          Number(output.unresolvedSymlinkId || 0);
        if (outputId) {
          state.artifactProducers.set(outputId, actionId);
        }
      }
      return;
    }

    if (entry.symlinkAction) {
      const symlinkActionId = id || nextSymlinkActionId++;
      const actionId = makeActionId("symlink", symlinkActionId);
      state.symlinkActions.set(symlinkActionId, entry.symlinkAction);
      const action = createSymlinkSummary(actionId, entry.symlinkAction, rawOrder);
      state.actions.set(actionId, action);
      registerTargetAction(action.targetLabel, actionId, state);
    }
  });

  for (const [actionId, action] of state.actions.entries()) {
    populatePrimaryOutputPath(actionId, action, state);
  }

  for (const [actionId, action] of state.actions.entries()) {
    if (action.kind !== "symlink") {
      continue;
    }
    const symlinkArtifactId = resolveSymlinkOutputArtifactId(actionId, state);
    if (symlinkArtifactId) {
      state.artifactProducers.set(symlinkArtifactId, actionId);
    }
  }

  for (const [actionId, action] of state.actions.entries()) {
    if (!action.targetLabel) {
      continue;
    }

    const consumedArtifactIds =
      action.kind === "spawn"
        ? getConsumedArtifactsForSpawn(actionId, state)
        : getConsumedArtifactsForSymlinkAction(actionId, state);

    for (const artifactId of consumedArtifactIds) {
      const producerActionId = state.artifactProducers.get(artifactId);
      if (!producerActionId || producerActionId === actionId) {
        continue;
      }
      const producer = state.actions.get(producerActionId);
      if (!producer?.targetLabel) {
        continue;
      }

      addRelation(action.parentMap, producer.targetLabel, producerActionId);
      addRelation(producer.childMap, action.targetLabel, actionId);

      const consumerTarget = getOrCreateTarget(action.targetLabel, state);
      addRelation(consumerTarget.parentMap, producer.targetLabel, actionId);

      const producerTarget = getOrCreateTarget(producer.targetLabel, state);
      addRelation(producerTarget.childMap, action.targetLabel, producerActionId);
    }
  }

  applyDisambiguatedActionLabels(state);

  const serializedActions: Record<string, CompactExecLogActionSummary> = {};
  const serializedTargets: Record<string, CompactExecLogTargetSummary> = {};

  Array.from(state.actions.values())
    .sort(compareActions)
    .forEach((action) => {
      serializedActions[action.id] = {
        id: action.id,
        kind: action.kind,
        targetLabel: action.targetLabel,
        label: action.label,
        mnemonic: action.mnemonic,
        cached: action.cached,
        result: action.result,
        primaryOutputPath: action.primaryOutputPath,
        digestHash: action.digestHash,
        digestSizeBytes: action.digestSizeBytes,
        parents: serializeRelations(action.parentMap),
        children: serializeRelations(action.childMap),
      };
    });

  Array.from(state.targets.values())
    .sort((a, b) => a.label.localeCompare(b.label))
    .forEach((target) => {
      serializedTargets[target.label] = {
        label: target.label,
        actionIds: Array.from(target.actionIds),
        parents: serializeRelations(target.parentMap),
        children: serializeRelations(target.childMap),
      };
    });

  return {
    version: CACHE_VERSION,
    cacheKey,
    createdAtMs: Date.now(),
    actions: serializedActions,
    targets: serializedTargets,
  };
}

function createSpawnSummary(
  actionId: string,
  spawn: tools.protos.ExecLogEntry.Spawn,
  rawOrder: number
): MutableActionSummary {
  const mnemonic = getSpawnDisplayMnemonic(spawn);
  return {
    id: actionId,
    kind: "spawn",
    rawOrder,
    targetLabel: spawn.targetLabel || "",
    label: mnemonic,
    mnemonic,
    cached: Boolean(spawn.cacheHit),
    result: isFailedSpawn(spawn) ? "failed" : "succeeded",
    primaryOutputPath: undefined,
    digestHash: spawn.digest?.hash || undefined,
    digestSizeBytes: spawn.digest ? `${spawn.digest.sizeBytes || 0}` : undefined,
    parentMap: new Map(),
    childMap: new Map(),
  };
}

function createSymlinkSummary(
  actionId: string,
  action: tools.protos.ExecLogEntry.SymlinkAction,
  rawOrder: number
): MutableActionSummary {
  const mnemonic = (action.mnemonic || "Symlink").trim();
  return {
    id: actionId,
    kind: "symlink",
    rawOrder,
    targetLabel: action.targetLabel || "",
    label: mnemonic,
    mnemonic,
    cached: false,
    result: "succeeded",
    primaryOutputPath: undefined,
    parentMap: new Map(),
    childMap: new Map(),
  };
}

function populatePrimaryOutputPath(actionId: string, action: MutableActionSummary, state: IndexBuildState) {
  if (action.kind === "spawn") {
    const spawn = state.spawns.get(parseActionNumericId(actionId));
    action.primaryOutputPath = spawn ? getSpawnPrimaryOutputPath(spawn, state) : undefined;
    return;
  }

  const symlinkAction = state.symlinkActions.get(parseActionNumericId(actionId));
  action.primaryOutputPath = symlinkAction?.outputPath || undefined;
}

function isFailedSpawn(spawn: tools.protos.ExecLogEntry.Spawn): boolean {
  return Boolean(spawn.status) || Number(spawn.exitCode || 0) !== 0;
}

function getSpawnDisplayMnemonic(spawn: tools.protos.ExecLogEntry.Spawn): string {
  const mnemonic = spawn.mnemonic || "Action";
  const details: string[] = [];

  const runNumber = getEnvNumber(spawn, "TEST_RUN_NUMBER");
  if (runNumber && runNumber > 1) {
    details.push(`run ${runNumber}`);
  }

  const shardIndex = getEnvNumber(spawn, "TEST_SHARD_INDEX");
  const totalShards = getEnvNumber(spawn, "TEST_TOTAL_SHARDS");
  if (shardIndex !== undefined && totalShards && totalShards > 0) {
    details.push(`shard ${shardIndex + 1}/${totalShards}`);
  }

  return details.length ? `${mnemonic} (${details.join(", ")})` : mnemonic;
}

function getEnvNumber(spawn: tools.protos.ExecLogEntry.Spawn, name: string): number | undefined {
  const raw = (spawn.envVars || []).find((value) => value.name === name)?.value;
  if (!raw) {
    return undefined;
  }
  const value = Number(raw);
  return Number.isFinite(value) ? Math.floor(value) : undefined;
}

function applyDisambiguatedActionLabels(state: IndexBuildState) {
  for (const target of state.targets.values()) {
    const metas = Array.from(target.actionIds)
      .map((actionId) => getActionLabelMeta(actionId, state))
      .filter(Boolean) as ActionLabelMeta[];

    metas.sort(
      (a, b) =>
        a.mnemonic.localeCompare(b.mnemonic) ||
        a.filename.localeCompare(b.filename) ||
        a.platform.localeCompare(b.platform) ||
        a.actionId.localeCompare(b.actionId)
    );

    const byMnemonic = new Map<string, ActionLabelMeta[]>();
    for (const meta of metas) {
      if (!byMnemonic.has(meta.mnemonic)) {
        byMnemonic.set(meta.mnemonic, []);
      }
      byMnemonic.get(meta.mnemonic)!.push(meta);
    }

    for (const [mnemonic, list] of byMnemonic.entries()) {
      if (list.length === 1) {
        const action = state.actions.get(list[0].actionId);
        if (action) {
          action.label = mnemonic;
        }
        continue;
      }

      const byFilename = new Map<string, ActionLabelMeta[]>();
      for (const meta of list) {
        if (!byFilename.has(meta.filename)) {
          byFilename.set(meta.filename, []);
        }
        byFilename.get(meta.filename)!.push(meta);
      }

      for (const [filename, fileList] of byFilename.entries()) {
        if (filename && fileList.length === 1) {
          const action = state.actions.get(fileList[0].actionId);
          if (action) {
            action.label = `${mnemonic} (${filename})`;
          }
          continue;
        }

        for (const meta of fileList) {
          const action = state.actions.get(meta.actionId);
          if (!action) {
            continue;
          }
          action.label = meta.platform ? `${mnemonic} (${meta.platform})` : mnemonic;
        }
      }
    }
  }
}

function getActionLabelMeta(actionId: string, state: IndexBuildState): ActionLabelMeta | undefined {
  const action = state.actions.get(actionId);
  if (!action) {
    return undefined;
  }

  if (action.kind === "spawn") {
    const spawn = state.spawns.get(parseActionNumericId(actionId));
    if (!spawn) {
      return undefined;
    }
    const primaryOutputPath = getSpawnPrimaryOutputPath(spawn, state);
    return {
      actionId,
      mnemonic: action.mnemonic,
      filename: extractFilename(primaryOutputPath),
      platform: extractPlatform(primaryOutputPath) || getSpawnPlatformSummary(spawn),
    };
  }

  const symlinkAction = state.symlinkActions.get(parseActionNumericId(actionId));
  if (!symlinkAction) {
    return undefined;
  }
  const outputPath = symlinkAction.outputPath || symlinkAction.inputPath;
  return {
    actionId,
    mnemonic: action.mnemonic,
    filename: extractFilename(outputPath),
    platform: extractPlatform(outputPath),
  };
}

function resolveOutputPath(output: tools.protos.ExecLogEntry.Output, state: IndexBuildState): string | undefined {
  if (output.invalidOutputPath) {
    return output.invalidOutputPath;
  }

  const outputId =
    Number(output.outputId || 0) ||
    Number(output.fileId || 0) ||
    Number(output.directoryId || 0) ||
    Number(output.unresolvedSymlinkId || 0);
  if (!outputId) {
    return undefined;
  }

  return (
    state.files.get(outputId)?.path ||
    state.directories.get(outputId)?.path ||
    state.symlinks.get(outputId)?.path ||
    state.runfilesTrees.get(outputId)?.path
  );
}

function getSpawnPrimaryOutputPath(spawn: tools.protos.ExecLogEntry.Spawn, state: IndexBuildState): string | undefined {
  let primaryOutputPath: string | undefined;
  let sawFile = false;

  for (const output of spawn.outputs || []) {
    const path = resolveOutputPath(output, state);
    if (!path) {
      continue;
    }

    const outputId =
      Number(output.outputId || 0) ||
      Number(output.fileId || 0) ||
      Number(output.directoryId || 0) ||
      Number(output.unresolvedSymlinkId || 0);

    if (outputId && state.files.has(outputId)) {
      if (!primaryOutputPath) {
        primaryOutputPath = path;
      }
      sawFile = true;
      continue;
    }

    if (!sawFile && !primaryOutputPath) {
      primaryOutputPath = path;
    }
  }

  return primaryOutputPath;
}

function extractPlatform(path: string | undefined): string {
  if (!path) {
    return "";
  }
  const match = path.match(/^bazel-out\/([^/]+)\//);
  return match?.[1] || "";
}

function extractFilename(path: string | undefined): string {
  if (!path) {
    return "";
  }
  const parts = path.split("/");
  return parts[parts.length - 1] || "";
}

function getSpawnPlatformSummary(spawn: tools.protos.ExecLogEntry.Spawn): string {
  const properties = (spawn.platform?.properties || []).map((property) => [property.name || "", property.value || ""]);
  const relevant = properties
    .filter(([name, value]) => name && value)
    .filter(([name]) => ["OSFamily", "cpu", "container-image", "Pool", "Arch"].includes(name));

  if (!relevant.length) {
    return "";
  }

  return relevant.map(([name, value]) => `${name}=${value}`).join(", ");
}

function getConsumedArtifactsForSpawn(actionId: string, state: IndexBuildState): Set<number> {
  const action = state.actions.get(actionId);
  if (!action || action.kind !== "spawn") {
    return new Set();
  }
  const spawn = state.spawns.get(parseActionNumericId(actionId));
  if (!spawn) {
    return new Set();
  }

  const result = new Set<number>();
  mergeInto(result, expandInputSet(spawn.inputSetId, state));
  mergeInto(result, expandInputSet(spawn.toolSetId, state));
  return result;
}

function getConsumedArtifactsForSymlinkAction(actionId: string, state: IndexBuildState): Set<number> {
  const action = state.actions.get(actionId);
  if (!action || action.kind !== "symlink") {
    return new Set();
  }
  const symlinkAction = state.symlinkActions.get(parseActionNumericId(actionId));
  if (!symlinkAction?.inputPath) {
    return new Set();
  }

  const artifactId = state.pathToArtifactId.get(symlinkAction.inputPath);
  return artifactId ? new Set([artifactId]) : new Set();
}

export function getExecConfiguredPathLookupCandidates(path: string): string[] {
  // Preserve the exact exec-configured path first. Some compact exec logs only
  // record the canonical `*-exec/` artifact path, so also try a lossy fallback
  // that strips the ST hash segment if present.
  const canonicalFallbackPath = path.replace(/-exec-ST-[^/]+\//, "-exec/");
  return canonicalFallbackPath === path ? [path] : [path, canonicalFallbackPath];
}

function resolveSymlinkOutputArtifactId(actionId: string, state: IndexBuildState): number | undefined {
  const symlinkAction = state.symlinkActions.get(parseActionNumericId(actionId));
  if (!symlinkAction?.outputPath) {
    return undefined;
  }

  for (const candidatePath of getExecConfiguredPathLookupCandidates(symlinkAction.outputPath)) {
    const artifactId = state.pathToArtifactId.get(candidatePath);
    if (artifactId) {
      return artifactId;
    }
  }
  return undefined;
}

function expandInputSet(inputSetId: number | undefined, state: IndexBuildState): Set<number> {
  if (!inputSetId) {
    return new Set();
  }

  const cached = state.expandedInputSets.get(inputSetId);
  if (cached) {
    return cached;
  }

  const inputSet = state.inputSets.get(inputSetId);
  if (!inputSet) {
    return new Set();
  }

  const result = new Set<number>();
  for (const transitiveId of inputSet.transitiveSetIds || []) {
    mergeInto(result, expandInputSet(transitiveId, state));
  }

  const directArtifactIds = [
    ...(inputSet.inputIds || []),
    ...(inputSet.fileIds || []),
    ...(inputSet.directoryIds || []),
    ...(inputSet.unresolvedSymlinkIds || []),
  ];
  for (const artifactId of directArtifactIds) {
    collectArtifactDependencies(artifactId, state, result);
  }

  state.expandedInputSets.set(inputSetId, result);
  return result;
}

function collectArtifactDependencies(artifactId: number, state: IndexBuildState, result: Set<number>) {
  if (!artifactId || result.has(artifactId)) {
    return;
  }

  result.add(artifactId);

  const runfilesTree = state.runfilesTrees.get(artifactId);
  if (!runfilesTree) {
    return;
  }

  mergeInto(result, expandInputSet(runfilesTree.inputSetId, state));
  for (const entryId of expandSymlinkEntrySet(runfilesTree.symlinksId, state).values()) {
    collectArtifactDependencies(entryId, state, result);
  }
  for (const entryId of expandSymlinkEntrySet(runfilesTree.rootSymlinksId, state).values()) {
    collectArtifactDependencies(entryId, state, result);
  }
}

function expandSymlinkEntrySet(entrySetId: number | undefined, state: IndexBuildState): Map<string, number> {
  if (!entrySetId) {
    return new Map();
  }

  const cached = state.expandedSymlinkEntrySets.get(entrySetId);
  if (cached) {
    return cached;
  }

  const result = new Map<string, number>();
  const visited = new Set<number>();

  const visit = (currentId: number) => {
    if (!currentId || visited.has(currentId)) {
      return;
    }
    visited.add(currentId);

    const entrySet = state.symlinkEntrySets.get(currentId);
    if (!entrySet) {
      return;
    }

    for (const transitiveId of entrySet.transitiveSetIds || []) {
      visit(transitiveId);
    }

    if (entrySet.directEntries) {
      for (const [relativePath, artifactId] of Object.entries(entrySet.directEntries)) {
        result.set(relativePath, Number(artifactId));
      }
    }
  };

  visit(entrySetId);
  state.expandedSymlinkEntrySets.set(entrySetId, result);
  return result;
}

function addRelation(relationMap: Map<string, Set<string>>, targetLabel: string, actionId: string) {
  let relationSet = relationMap.get(targetLabel);
  if (!relationSet) {
    relationSet = new Set();
    relationMap.set(targetLabel, relationSet);
  }
  relationSet.add(actionId);
}

function getOrCreateTarget(targetLabel: string, state: IndexBuildState): MutableTargetSummary {
  let target = state.targets.get(targetLabel);
  if (!target) {
    target = {
      label: targetLabel,
      actionIds: new Set(),
      parentMap: new Map(),
      childMap: new Map(),
    };
    state.targets.set(targetLabel, target);
  }
  return target;
}

function registerTargetAction(targetLabel: string, actionId: string, state: IndexBuildState) {
  if (!targetLabel) {
    return;
  }
  getOrCreateTarget(targetLabel, state).actionIds.add(actionId);
}

function serializeRelations(relationMap: Map<string, Set<string>>): RelatedTargetSummary[] {
  return Array.from(relationMap.entries())
    .map(([targetLabel, actionIds]) => ({
      targetLabel,
      actionIds: Array.from(actionIds),
    }))
    .sort((a, b) => a.targetLabel.localeCompare(b.targetLabel));
}

function compareActions(a: MutableActionSummary, b: MutableActionSummary): number {
  if (a.targetLabel !== b.targetLabel) {
    return a.targetLabel.localeCompare(b.targetLabel);
  }
  if (a.rawOrder !== b.rawOrder) {
    return a.rawOrder - b.rawOrder;
  }
  return a.id.localeCompare(b.id);
}

function makeActionId(kind: ActionKind, id: number): string {
  return `${kind}:${id}`;
}

function parseActionNumericId(actionId: string): number {
  return Number(actionId.split(":")[1] || 0);
}

function mergeInto(destination: Set<number>, source: Set<number>) {
  source.forEach((value) => destination.add(value));
}

async function readCachedIndex(cacheKey: string): Promise<CompactExecLogEdgesIndex | undefined> {
  const db = await openDb();
  if (!db) {
    return undefined;
  }

  return new Promise((resolve) => {
    const transaction = db.transaction(STORE_NAME, "readonly");
    const store = transaction.objectStore(STORE_NAME);
    const request = store.get(cacheKey);
    request.onsuccess = () => resolve(request.result as CompactExecLogEdgesIndex | undefined);
    request.onerror = () => resolve(undefined);
  });
}

async function writeCachedIndex(index: CompactExecLogEdgesIndex): Promise<void> {
  const db = await openDb();
  if (!db) {
    return;
  }

  await new Promise<void>((resolve) => {
    const transaction = db.transaction(STORE_NAME, "readwrite");
    const store = transaction.objectStore(STORE_NAME);
    store.put(index, index.cacheKey);
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => resolve();
    transaction.onabort = () => resolve();
  });
}

function openDb(): Promise<IDBDatabase | null> {
  if (dbPromise) {
    return dbPromise;
  }

  if (typeof window === "undefined" || !window.indexedDB) {
    dbPromise = Promise.resolve(null);
    return dbPromise;
  }

  dbPromise = new Promise((resolve) => {
    const request = window.indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME);
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(null);
  });

  return dbPromise;
}
