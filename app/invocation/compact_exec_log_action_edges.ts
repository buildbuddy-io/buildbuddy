import { tools } from "../../proto/spawn_ts_proto";

const CACHE_VERSION = 4;
const MAX_CACHED_ACTION_EDGE_INDEXES = 5;
const PERSISTENT_DB_NAME = "buildbuddy-compact-exec-log-action-edges";
const PERSISTENT_DB_VERSION = 1;
const PERSISTENT_STORE_NAME = "indexes";
const PERSISTENT_LAST_ACCESSED_INDEX = "lastAccessedAtMillis";
const MAX_PERSISTENT_ACTION_EDGE_INDEXES = 20;
const memoryCache = new Map<string, CompactExecLogActionEdgesIndex>();
const pendingLoads = new Map<string, Promise<CompactExecLogActionEdgesIndex>>();
let persistentDBPromise: Promise<IDBDatabase | undefined> | undefined;

interface IDigestLike {
  hash?: string | null;
  sizeBytes?: unknown;
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
  parentIds: string[];
  childIds: string[];
  parentEdges: CompactExecLogActionEdge[];
  childEdges: CompactExecLogActionEdge[];
}

export interface CompactExecLogActionEdge {
  actionId: string;
  artifactId: number;
  artifactPath: string;
}

export interface CompactExecLogActionEdgesIndex {
  actions: CompactExecLogActionSummary[];
  actionById: Map<string, CompactExecLogActionSummary>;
}

interface ParsedTargetLabel {
  workspace: string;
  packagePath: string;
  isExternal: boolean;
  isKnownLabel: boolean;
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
  pathToArtifactId: Map<string, number>;
  equivalentPathToArtifactId: Map<string, number>;
  ambiguousEquivalentPaths: Set<string>;
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
  parentIds: Set<string>;
  childIds: Set<string>;
  parentEdges: Map<string, CompactExecLogActionEdge>;
  childEdges: Map<string, CompactExecLogActionEdge>;
}

interface ActionLabelMeta {
  actionId: string;
  mnemonic: string;
  filename: string;
  platform: string;
}

interface PersistentActionEdgesIndexRecord {
  cacheKey: string;
  createdAtMillis: number;
  lastAccessedAtMillis: number;
  actions: CompactExecLogActionSummary[];
}

export async function getCompactExecLogActionEdgesIndex(
  executionLogUri: string,
  loadEntries: () => Promise<tools.protos.ExecLogEntry[]>
): Promise<CompactExecLogActionEdgesIndex> {
  const cacheKey = `v${CACHE_VERSION}:${executionLogUri}`;
  const cached = getCachedIndex(cacheKey);
  if (cached) return cached;

  const pending = pendingLoads.get(cacheKey);
  if (pending) return pending;

  const loadPromise = (async () => {
    const persistentIndex = await getPersistentCachedIndex(cacheKey);
    if (persistentIndex) {
      setCachedIndex(cacheKey, persistentIndex);
      return persistentIndex;
    }

    const entries = await loadEntries();
    const index = buildCompactExecLogActionEdgesIndex(entries);
    setCachedIndex(cacheKey, index);
    setPersistentCachedIndex(cacheKey, index);
    return index;
  })().finally(() => pendingLoads.delete(cacheKey));

  pendingLoads.set(cacheKey, loadPromise);
  return loadPromise;
}

function getCachedIndex(cacheKey: string): CompactExecLogActionEdgesIndex | undefined {
  const cached = memoryCache.get(cacheKey);
  if (!cached) return undefined;
  memoryCache.delete(cacheKey);
  memoryCache.set(cacheKey, cached);
  return cached;
}

function setCachedIndex(cacheKey: string, index: CompactExecLogActionEdgesIndex) {
  memoryCache.delete(cacheKey);
  memoryCache.set(cacheKey, index);
  while (memoryCache.size > MAX_CACHED_ACTION_EDGE_INDEXES) {
    const oldestKey = memoryCache.keys().next().value;
    if (!oldestKey) break;
    memoryCache.delete(oldestKey);
  }
}

function getPersistentCacheDB(): Promise<IDBDatabase | undefined> {
  if (persistentDBPromise) return persistentDBPromise;
  if (typeof window === "undefined" || !window.indexedDB) {
    persistentDBPromise = Promise.resolve(undefined);
    return persistentDBPromise;
  }

  persistentDBPromise = new Promise((resolve) => {
    const request = window.indexedDB.open(PERSISTENT_DB_NAME, PERSISTENT_DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      const store = db.objectStoreNames.contains(PERSISTENT_STORE_NAME)
        ? request.transaction?.objectStore(PERSISTENT_STORE_NAME)
        : db.createObjectStore(PERSISTENT_STORE_NAME, { keyPath: "cacheKey" });
      if (store && !store.indexNames.contains(PERSISTENT_LAST_ACCESSED_INDEX)) {
        store.createIndex(PERSISTENT_LAST_ACCESSED_INDEX, PERSISTENT_LAST_ACCESSED_INDEX);
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(undefined);
    request.onblocked = () => resolve(undefined);
  });
  return persistentDBPromise;
}

async function getPersistentCachedIndex(cacheKey: string): Promise<CompactExecLogActionEdgesIndex | undefined> {
  const db = await getPersistentCacheDB();
  if (!db) return undefined;

  const record = await getPersistentCacheRecord(db, cacheKey);
  if (!record) return undefined;

  updatePersistentCacheLastAccessed(db, cacheKey);
  return hydrateActionEdgesIndex(record.actions);
}

function setPersistentCachedIndex(cacheKey: string, index: CompactExecLogActionEdgesIndex) {
  getPersistentCacheDB().then(async (db) => {
    if (!db) return;
    await putPersistentCacheRecord(db, {
      cacheKey,
      createdAtMillis: Date.now(),
      lastAccessedAtMillis: Date.now(),
      actions: index.actions,
    });
    prunePersistentCache(db);
  });
}

function hydrateActionEdgesIndex(actions: CompactExecLogActionSummary[]): CompactExecLogActionEdgesIndex {
  return {
    actions,
    actionById: new Map(actions.map((action) => [action.id, action])),
  };
}

function getPersistentCacheRecord(
  db: IDBDatabase,
  cacheKey: string
): Promise<PersistentActionEdgesIndexRecord | undefined> {
  return new Promise((resolve) => {
    const transaction = db.transaction(PERSISTENT_STORE_NAME, "readonly");
    const request = transaction.objectStore(PERSISTENT_STORE_NAME).get(cacheKey);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(undefined);
    transaction.onabort = () => resolve(undefined);
    transaction.onerror = () => resolve(undefined);
  });
}

function putPersistentCacheRecord(db: IDBDatabase, record: PersistentActionEdgesIndexRecord): Promise<void> {
  return new Promise((resolve) => {
    const transaction = db.transaction(PERSISTENT_STORE_NAME, "readwrite");
    transaction.objectStore(PERSISTENT_STORE_NAME).put(record);
    transaction.oncomplete = () => resolve();
    transaction.onabort = () => resolve();
    transaction.onerror = () => resolve();
  });
}

function updatePersistentCacheLastAccessed(db: IDBDatabase, cacheKey: string) {
  getPersistentCacheRecord(db, cacheKey).then((record) => {
    if (!record) return;
    putPersistentCacheRecord(db, { ...record, lastAccessedAtMillis: Date.now() });
  });
}

async function prunePersistentCache(db: IDBDatabase) {
  const cacheKeys = await getPersistentCacheKeysByLastAccess(db);
  const deleteCount = cacheKeys.length - MAX_PERSISTENT_ACTION_EDGE_INDEXES;
  if (deleteCount <= 0) return;
  deletePersistentCacheKeys(db, cacheKeys.slice(0, deleteCount));
}

function getPersistentCacheKeysByLastAccess(db: IDBDatabase): Promise<string[]> {
  return new Promise((resolve) => {
    const cacheKeys: string[] = [];
    const transaction = db.transaction(PERSISTENT_STORE_NAME, "readonly");
    const store = transaction.objectStore(PERSISTENT_STORE_NAME);
    const request = store.index(PERSISTENT_LAST_ACCESSED_INDEX).openKeyCursor();
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) return;
      cacheKeys.push(String(cursor.primaryKey));
      cursor.continue();
    };
    request.onerror = () => resolve([]);
    transaction.oncomplete = () => resolve(cacheKeys);
    transaction.onabort = () => resolve([]);
    transaction.onerror = () => resolve([]);
  });
}

function deletePersistentCacheKeys(db: IDBDatabase, cacheKeys: string[]): Promise<void> {
  return new Promise((resolve) => {
    const transaction = db.transaction(PERSISTENT_STORE_NAME, "readwrite");
    const store = transaction.objectStore(PERSISTENT_STORE_NAME);
    for (const cacheKey of cacheKeys) store.delete(cacheKey);
    transaction.oncomplete = () => resolve();
    transaction.onabort = () => resolve();
    transaction.onerror = () => resolve();
  });
}

export function buildCompactExecLogActionEdgesIndex(
  entries: tools.protos.ExecLogEntry[]
): CompactExecLogActionEdgesIndex {
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
    pathToArtifactId: new Map(),
    equivalentPathToArtifactId: new Map(),
    ambiguousEquivalentPaths: new Set(),
    artifactProducers: new Map(),
    expandedInputSets: new Map(),
    expandedSymlinkEntrySets: new Map(),
  };
  let nextSpawnId = 1;
  let nextSymlinkActionId = 1;

  entries.forEach((entry, rawOrder) => {
    const id = Number(entry.id || 0);

    if (entry.file) {
      if (id) state.files.set(id, entry.file);
      if (id && entry.file.path) recordArtifactPath(state, entry.file.path, id);
      return;
    }
    if (entry.directory) {
      if (id) state.directories.set(id, entry.directory);
      if (id && entry.directory.path) recordArtifactPath(state, entry.directory.path, id);
      return;
    }
    if (entry.unresolvedSymlink) {
      if (id) state.symlinks.set(id, entry.unresolvedSymlink);
      if (id && entry.unresolvedSymlink.path) recordArtifactPath(state, entry.unresolvedSymlink.path, id);
      return;
    }
    if (entry.runfilesTree) {
      if (id) state.runfilesTrees.set(id, entry.runfilesTree);
      if (id && entry.runfilesTree.path) recordArtifactPath(state, entry.runfilesTree.path, id);
      return;
    }
    if (entry.inputSet) {
      if (id) state.inputSets.set(id, entry.inputSet);
      return;
    }
    if (entry.symlinkEntrySet) {
      if (id) state.symlinkEntrySets.set(id, entry.symlinkEntrySet);
      return;
    }
    if (entry.spawn) {
      const spawnId = id || nextSpawnId++;
      const actionId = makeActionId("spawn", spawnId);
      state.spawns.set(spawnId, entry.spawn);
      state.actions.set(actionId, createSpawnSummary(actionId, entry.spawn, rawOrder));
      for (const output of entry.spawn.outputs || []) {
        const outputId = getOutputId(output);
        if (outputId) state.artifactProducers.set(outputId, actionId);
      }
      return;
    }
    if (entry.symlinkAction) {
      const symlinkActionId = id || nextSymlinkActionId++;
      const actionId = makeActionId("symlink", symlinkActionId);
      state.symlinkActions.set(symlinkActionId, entry.symlinkAction);
      state.actions.set(actionId, createSymlinkSummary(actionId, entry.symlinkAction, rawOrder));
    }
  });

  for (const [actionId, action] of state.actions.entries()) {
    populatePrimaryOutputPath(actionId, action, state);
  }
  applyDisambiguatedActionLabels(state);

  for (const [actionId, action] of state.actions.entries()) {
    if (action.kind !== "symlink") continue;
    const symlinkArtifactId = resolveSymlinkOutputArtifactId(actionId, state);
    if (symlinkArtifactId) state.artifactProducers.set(symlinkArtifactId, actionId);
  }

  for (const [actionId, action] of state.actions.entries()) {
    const consumedArtifactIds =
      action.kind === "spawn"
        ? getConsumedArtifactsForSpawn(actionId, state)
        : getConsumedArtifactsForSymlinkAction(actionId, state);
    for (const artifactId of consumedArtifactIds) {
      const producerActionId = state.artifactProducers.get(artifactId);
      if (!producerActionId || producerActionId === actionId) continue;
      const artifactPath = getArtifactPath(artifactId, state) || `artifact ${artifactId}`;
      action.parentIds.add(producerActionId);
      action.parentEdges.set(getActionEdgeKey(producerActionId, artifactId), {
        actionId: producerActionId,
        artifactId,
        artifactPath,
      });
      const producerAction = state.actions.get(producerActionId);
      producerAction?.childIds.add(actionId);
      producerAction?.childEdges.set(getActionEdgeKey(actionId, artifactId), {
        actionId,
        artifactId,
        artifactPath,
      });
    }
  }

  const actions = Array.from(state.actions.values())
    .sort(compareActions)
    .map<CompactExecLogActionSummary>((action) => ({
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
      parentIds: sortActionIds(action.parentIds, state),
      childIds: sortActionIds(action.childIds, state),
      parentEdges: sortActionEdges(action.parentEdges.values(), state),
      childEdges: sortActionEdges(action.childEdges.values(), state),
    }));

  return {
    actions,
    actionById: new Map(actions.map((action) => [action.id, action])),
  };
}

export function findCompactExecLogAction(
  index: CompactExecLogActionEdgesIndex | undefined,
  digest: IDigestLike | null | undefined,
  targetLabel?: string,
  mnemonic?: string
): CompactExecLogActionSummary | undefined {
  if (!index || !digest?.hash) return undefined;
  const digestSizeBytes = digest.sizeBytes !== undefined && digest.sizeBytes !== null ? String(digest.sizeBytes) : "";
  let candidates = index.actions.filter((action) => action.digestHash === digest.hash);
  if (digestSizeBytes) {
    const exact = candidates.filter((action) => !action.digestSizeBytes || action.digestSizeBytes === digestSizeBytes);
    if (exact.length) candidates = exact;
  }
  if (targetLabel) {
    const targetMatches = candidates.filter((action) => action.targetLabel === targetLabel);
    if (targetMatches.length) candidates = targetMatches;
  }
  if (mnemonic) {
    const mnemonicMatches = candidates.filter((action) => action.mnemonic === mnemonic || action.label === mnemonic);
    if (mnemonicMatches.length) candidates = mnemonicMatches;
  }
  return candidates[0];
}

export function compareTargetLabelsByRelevance(a: string, b: string, referenceTargetLabel: string | undefined): number {
  const rankA = getTargetLabelRelevanceRank(a, referenceTargetLabel);
  const rankB = getTargetLabelRelevanceRank(b, referenceTargetLabel);
  if (rankA !== rankB) return rankA - rankB;
  return a.localeCompare(b);
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
    digestHash: spawn.digest?.hash || undefined,
    digestSizeBytes: spawn.digest ? String(spawn.digest.sizeBytes || 0) : undefined,
    parentIds: new Set(),
    childIds: new Set(),
    parentEdges: new Map(),
    childEdges: new Map(),
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
    parentIds: new Set(),
    childIds: new Set(),
    parentEdges: new Map(),
    childEdges: new Map(),
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

function getConsumedArtifactsForSpawn(actionId: string, state: IndexBuildState): Set<number> {
  const spawn = state.spawns.get(parseActionNumericId(actionId));
  if (!spawn) return new Set();
  const result = new Set<number>();
  mergeInto(result, expandInputSet(spawn.inputSetId, state));
  mergeInto(result, expandInputSet(spawn.toolSetId, state));
  return result;
}

function getConsumedArtifactsForSymlinkAction(actionId: string, state: IndexBuildState): Set<number> {
  const symlinkAction = state.symlinkActions.get(parseActionNumericId(actionId));
  if (!symlinkAction?.inputPath) return new Set();
  const artifactId = findArtifactIdByPath(state, symlinkAction.inputPath);
  return artifactId ? new Set([artifactId]) : new Set();
}

function expandInputSet(inputSetId: number | undefined, state: IndexBuildState): Set<number> {
  if (!inputSetId) return new Set();
  const cached = state.expandedInputSets.get(inputSetId);
  if (cached) return cached;

  const inputSet = state.inputSets.get(inputSetId);
  if (!inputSet) return new Set();

  const result = new Set<number>();
  for (const transitiveId of inputSet.transitiveSetIds || []) {
    mergeInto(result, expandInputSet(transitiveId, state));
  }
  for (const artifactId of [
    ...(inputSet.inputIds || []),
    ...(inputSet.fileIds || []),
    ...(inputSet.directoryIds || []),
    ...(inputSet.unresolvedSymlinkIds || []),
  ]) {
    collectArtifactDependencies(artifactId, state, result);
  }
  state.expandedInputSets.set(inputSetId, result);
  return result;
}

function collectArtifactDependencies(artifactId: number, state: IndexBuildState, result: Set<number>) {
  if (!artifactId || result.has(artifactId)) return;
  result.add(artifactId);

  const runfilesTree = state.runfilesTrees.get(artifactId);
  if (!runfilesTree) return;

  mergeInto(result, expandInputSet(runfilesTree.inputSetId, state));
  for (const entryId of expandSymlinkEntrySet(runfilesTree.symlinksId, state).values()) {
    collectArtifactDependencies(entryId, state, result);
  }
  for (const entryId of expandSymlinkEntrySet(runfilesTree.rootSymlinksId, state).values()) {
    collectArtifactDependencies(entryId, state, result);
  }
}

function expandSymlinkEntrySet(entrySetId: number | undefined, state: IndexBuildState): Map<string, number> {
  if (!entrySetId) return new Map();
  const cached = state.expandedSymlinkEntrySets.get(entrySetId);
  if (cached) return cached;

  const result = new Map<string, number>();
  const visited = new Set<number>();
  const visit = (currentId: number) => {
    if (!currentId || visited.has(currentId)) return;
    visited.add(currentId);
    const entrySet = state.symlinkEntrySets.get(currentId);
    if (!entrySet) return;
    for (const transitiveId of entrySet.transitiveSetIds || []) {
      visit(transitiveId);
    }
    for (const [relativePath, artifactId] of Object.entries(entrySet.directEntries || {})) {
      result.set(relativePath, Number(artifactId));
    }
  };
  visit(entrySetId);
  state.expandedSymlinkEntrySets.set(entrySetId, result);
  return result;
}

export function getExecConfiguredPathLookupCandidates(path: string): string[] {
  const canonicalFallbackPath = path.replace(/-exec-ST-[^/]+\//, "-exec/");
  return canonicalFallbackPath === path ? [path] : [path, canonicalFallbackPath];
}

function recordArtifactPath(state: IndexBuildState, path: string, artifactId: number) {
  state.pathToArtifactId.set(path, artifactId);
  for (const candidatePath of getExecConfiguredPathLookupCandidates(path)) {
    if (candidatePath === path) continue;
    recordEquivalentArtifactPath(state, candidatePath, artifactId);
  }
}

function recordEquivalentArtifactPath(state: IndexBuildState, path: string, artifactId: number) {
  if (state.ambiguousEquivalentPaths.has(path)) return;
  const existingArtifactId = state.equivalentPathToArtifactId.get(path);
  if (!existingArtifactId || existingArtifactId === artifactId) {
    state.equivalentPathToArtifactId.set(path, artifactId);
    return;
  }

  state.equivalentPathToArtifactId.delete(path);
  state.ambiguousEquivalentPaths.add(path);
}

function findArtifactIdByPath(state: IndexBuildState, path: string): number | undefined {
  for (const candidatePath of getExecConfiguredPathLookupCandidates(path)) {
    const exactArtifactId = state.pathToArtifactId.get(candidatePath);
    if (exactArtifactId) return exactArtifactId;

    if (state.ambiguousEquivalentPaths.has(candidatePath)) continue;
    const equivalentArtifactId = state.equivalentPathToArtifactId.get(candidatePath);
    if (equivalentArtifactId) return equivalentArtifactId;
  }
  return undefined;
}

function resolveSymlinkOutputArtifactId(actionId: string, state: IndexBuildState): number | undefined {
  const symlinkAction = state.symlinkActions.get(parseActionNumericId(actionId));
  if (!symlinkAction?.outputPath) return undefined;

  return findArtifactIdByPath(state, symlinkAction.outputPath);
}

function getSpawnPrimaryOutputPath(spawn: tools.protos.ExecLogEntry.Spawn, state: IndexBuildState): string | undefined {
  let primaryOutputPath: string | undefined;
  let sawFile = false;

  for (const output of spawn.outputs || []) {
    const path = resolveOutputPath(output, state);
    if (!path) continue;

    const outputId = getOutputId(output);
    if (outputId && state.files.has(outputId)) {
      if (!primaryOutputPath) primaryOutputPath = path;
      sawFile = true;
      continue;
    }

    if (!sawFile && !primaryOutputPath) primaryOutputPath = path;
  }
  return primaryOutputPath;
}

function resolveOutputPath(output: tools.protos.ExecLogEntry.Output, state: IndexBuildState): string | undefined {
  if (output.invalidOutputPath) return output.invalidOutputPath;
  const outputId = getOutputId(output);
  if (!outputId) return undefined;
  return getArtifactPath(outputId, state);
}

function getArtifactPath(artifactId: number, state: IndexBuildState): string | undefined {
  return (
    state.files.get(artifactId)?.path ||
    state.directories.get(artifactId)?.path ||
    state.symlinks.get(artifactId)?.path ||
    state.runfilesTrees.get(artifactId)?.path
  );
}

function getOutputId(output: tools.protos.ExecLogEntry.Output): number {
  return (
    Number(output.outputId || 0) ||
    Number(output.fileId || 0) ||
    Number(output.directoryId || 0) ||
    Number(output.unresolvedSymlinkId || 0)
  );
}

function isFailedSpawn(spawn: tools.protos.ExecLogEntry.Spawn): boolean {
  return Boolean(spawn.status) || Number(spawn.exitCode || 0) !== 0;
}

function getSpawnDisplayMnemonic(spawn: tools.protos.ExecLogEntry.Spawn): string {
  const mnemonic = spawn.mnemonic || "Action";
  const details: string[] = [];
  const runNumber = getEnvNumber(spawn, "TEST_RUN_NUMBER");
  if (runNumber && runNumber > 1) details.push(`run ${runNumber}`);

  const shardIndex = getEnvNumber(spawn, "TEST_SHARD_INDEX");
  const totalShards = getEnvNumber(spawn, "TEST_TOTAL_SHARDS");
  if (shardIndex !== undefined && totalShards && totalShards > 0) {
    details.push(`shard ${shardIndex + 1}/${totalShards}`);
  }

  return details.length ? `${mnemonic} (${details.join(", ")})` : mnemonic;
}

function getEnvNumber(spawn: tools.protos.ExecLogEntry.Spawn, name: string): number | undefined {
  const raw = (spawn.envVars || []).find((value) => value.name === name)?.value;
  if (!raw) return undefined;
  const value = Number(raw);
  return Number.isFinite(value) ? Math.floor(value) : undefined;
}

function applyDisambiguatedActionLabels(state: IndexBuildState) {
  const actionsByTargetLabel = new Map<string, MutableActionSummary[]>();
  for (const action of state.actions.values()) {
    const targetActions = actionsByTargetLabel.get(action.targetLabel) || [];
    targetActions.push(action);
    actionsByTargetLabel.set(action.targetLabel, targetActions);
  }

  for (const actions of actionsByTargetLabel.values()) {
    const metas = actions
      .map((action) => getActionLabelMeta(action.id, state))
      .filter((meta): meta is ActionLabelMeta => Boolean(meta));

    metas.sort(
      (a, b) =>
        a.mnemonic.localeCompare(b.mnemonic) ||
        a.filename.localeCompare(b.filename) ||
        a.platform.localeCompare(b.platform) ||
        a.actionId.localeCompare(b.actionId)
    );

    const byMnemonic = new Map<string, ActionLabelMeta[]>();
    for (const meta of metas) {
      const mnemonicActions = byMnemonic.get(meta.mnemonic) || [];
      mnemonicActions.push(meta);
      byMnemonic.set(meta.mnemonic, mnemonicActions);
    }

    for (const [mnemonic, list] of byMnemonic.entries()) {
      if (list.length === 1) {
        const action = state.actions.get(list[0].actionId);
        if (action) action.label = mnemonic;
        continue;
      }

      const byFilename = new Map<string, ActionLabelMeta[]>();
      for (const meta of list) {
        const filenameActions = byFilename.get(meta.filename) || [];
        filenameActions.push(meta);
        byFilename.set(meta.filename, filenameActions);
      }

      for (const [filename, filenameActions] of byFilename.entries()) {
        if (filename && filenameActions.length === 1) {
          const action = state.actions.get(filenameActions[0].actionId);
          if (action) action.label = `${mnemonic} (${filename})`;
          continue;
        }

        for (const meta of filenameActions) {
          const action = state.actions.get(meta.actionId);
          if (action) action.label = meta.platform ? `${mnemonic} (${meta.platform})` : mnemonic;
        }
      }
    }
  }
}

function getActionLabelMeta(actionId: string, state: IndexBuildState): ActionLabelMeta | undefined {
  const action = state.actions.get(actionId);
  if (!action) return undefined;

  if (action.kind === "spawn") {
    const spawn = state.spawns.get(parseActionNumericId(actionId));
    if (!spawn) return undefined;
    const primaryOutputPath = action.primaryOutputPath || getSpawnPrimaryOutputPath(spawn, state);
    return {
      actionId,
      mnemonic: action.mnemonic,
      filename: extractFilename(primaryOutputPath),
      platform: extractPlatform(primaryOutputPath) || getSpawnPlatformSummary(spawn),
    };
  }

  const symlinkAction = state.symlinkActions.get(parseActionNumericId(actionId));
  if (!symlinkAction) return undefined;
  const outputPath = symlinkAction.outputPath || symlinkAction.inputPath;
  return {
    actionId,
    mnemonic: action.mnemonic,
    filename: extractFilename(outputPath),
    platform: extractPlatform(outputPath),
  };
}

function extractFilename(path: string | undefined): string {
  if (!path) return "";
  const parts = path.split("/");
  return parts[parts.length - 1] || "";
}

function extractPlatform(path: string | undefined): string {
  if (!path) return "";
  const match = path.match(/^bazel-out\/([^/]+)\//);
  return match?.[1] || "";
}

function getSpawnPlatformSummary(spawn: tools.protos.ExecLogEntry.Spawn): string {
  const properties = (spawn.platform?.properties || []).map((property) => [property.name || "", property.value || ""]);
  const relevant = properties
    .filter(([name, value]) => name && value)
    .filter(([name]) => ["OSFamily", "cpu", "container-image", "Pool", "Arch"].includes(name));

  return relevant.map(([name, value]) => `${name}=${value}`).join(", ");
}

function sortActionIds(ids: Set<string>, state: IndexBuildState): string[] {
  return Array.from(ids).sort((a, b) => {
    const actionA = state.actions.get(a);
    const actionB = state.actions.get(b);
    if (actionA && actionB) return compareActions(actionA, actionB);
    return a.localeCompare(b);
  });
}

function sortActionEdges(
  edges: Iterable<CompactExecLogActionEdge>,
  state: IndexBuildState
): CompactExecLogActionEdge[] {
  return Array.from(edges).sort((a, b) => {
    if (a.artifactPath !== b.artifactPath) return a.artifactPath.localeCompare(b.artifactPath);
    const actionA = state.actions.get(a.actionId);
    const actionB = state.actions.get(b.actionId);
    if (actionA && actionB) return compareActions(actionA, actionB);
    if (a.actionId !== b.actionId) return a.actionId.localeCompare(b.actionId);
    return a.artifactId - b.artifactId;
  });
}

function compareActions(a: MutableActionSummary, b: MutableActionSummary): number {
  if (a.targetLabel !== b.targetLabel) return a.targetLabel.localeCompare(b.targetLabel);
  if (a.rawOrder !== b.rawOrder) return a.rawOrder - b.rawOrder;
  return a.id.localeCompare(b.id);
}

function getTargetLabelRelevanceRank(label: string, referenceTargetLabel: string | undefined): number {
  const parsed = parseTargetLabel(label);
  const reference = parseTargetLabel(referenceTargetLabel || "");

  if (label && label === referenceTargetLabel) return 0;
  if (parsed.isKnownLabel && reference.isKnownLabel && parsed.workspace === reference.workspace) {
    if (parsed.packagePath === reference.packagePath) return 1;
    return 2;
  }

  if (parsed.isKnownLabel && !parsed.isExternal) return 3;
  if (parsed.isKnownLabel && parsed.isExternal) return 4;
  return 5;
}

function parseTargetLabel(label: string): ParsedTargetLabel {
  const trimmed = label.trim();
  const mainWorkspaceMatch = trimmed.match(/^\/\/([^:]*)(?::(.*))?$/);
  if (mainWorkspaceMatch) {
    return {
      workspace: "",
      packagePath: mainWorkspaceMatch[1] || "",
      isExternal: false,
      isKnownLabel: true,
    };
  }

  const externalMatch = trimmed.match(/^(@@?[^/]*)\/\/([^:]*)(?::(.*))?$/);
  if (externalMatch) {
    const workspace = externalMatch[1] === "@" || externalMatch[1] === "@@" ? "" : externalMatch[1];
    return {
      workspace,
      packagePath: externalMatch[2] || "",
      isExternal: Boolean(workspace),
      isKnownLabel: true,
    };
  }

  return {
    workspace: "",
    packagePath: "",
    isExternal: false,
    isKnownLabel: false,
  };
}

function getActionEdgeKey(actionId: string, artifactId: number): string {
  return `${actionId}:${artifactId}`;
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
