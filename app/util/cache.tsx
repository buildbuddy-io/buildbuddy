import { build } from "../../proto/remote_execution_ts_proto";
import { resource } from "../../proto/resource_ts_proto";
import Long from "long";

const Digest = build.bazel.remote.execution.v2.Digest;
type Digest = build.bazel.remote.execution.v2.Digest;
type IDigest = build.bazel.remote.execution.v2.IDigest;
const Compressor = build.bazel.remote.execution.v2.Compressor.Value;
type Compressor = build.bazel.remote.execution.v2.Compressor.Value;
const DigestFunction = build.bazel.remote.execution.v2.DigestFunction.Value;
type DigestFunction = build.bazel.remote.execution.v2.DigestFunction.Value;

/**
 * Parses a Digest proto from a string like "HASH/SIZE".
 */
export function parseDigest(raw: string): Digest {
  const [hash, rawSize] = raw.split("/");
  if (!hash || !rawSize) {
    throw new Error(`invalid digest string "${raw}"`);
  }
  return new Digest({
    hash,
    sizeBytes: Long.fromString(rawSize),
  });
}

/**
 * Parses an action ID, which may either be a string like "HASH/SIZE"
 * or just a "HASH" (since the SIZE is not required for AC requests).
 */
export function parseActionDigest(raw: string): Digest {
  let [hash, rawSize] = raw.split("/");
  if (!hash) {
    throw new Error(`invalid digest string "${raw}"`);
  }
  rawSize ??= "1";
  return new Digest({
    hash,
    sizeBytes: Long.fromString(rawSize),
  });
}

/**
 * Converts a digest to its string representation (HASH/SIZE).
 */
export function digestToString(digest: IDigest): string {
  return `${digest.hash}/${digest.sizeBytes}`;
}

/**
 * Converts a compressor name like "zstd" to a `Compressor.Value` proto.
 */
export function parseCompressor(name: string): Compressor {
  if (name === "") {
    return Compressor.IDENTITY;
  }
  const value = Compressor[name.toUpperCase() as keyof typeof Compressor];
  if (value === undefined) {
    throw new Error(`unrecognized compressor "${name}"`);
  }
  return value;
}

/**
 * Converts a digest function name like "blake3" to a `DigestFunction.Value` proto.
 */
export function parseDigestFunction(name: string): DigestFunction {
  if (name === "") {
    return DigestFunction.SHA256;
  }
  const value = DigestFunction[name.toUpperCase() as keyof typeof DigestFunction];
  if (value === undefined) {
    throw new Error(`unrecognized digest function "${name}"`);
  }
  return value;
}

/**
 * Converts a cache resource name to a string that can be fetched with the
 * bytestream API.
 *
 * Note: the "actioncache://" prefix is specially supported by BuildBuddy's HTTP
 * file streaming API and is not part of the official Bazel remote API
 * specification.
 */
export function resourceNameToString(address: string, name: resource.IResourceName): string {
  return [
    (name.cacheType === resource.CacheType.AC ? "actioncache://" : "bytestream://") + address,
    name.instanceName ?? "",
    blobsURLPart(name),
    name.cacheType === resource.CacheType.AC ? "ac" : "",
    digestFunctionURLPart(name),
    name.digest?.hash ?? "",
    name.digest?.sizeBytes?.toString() ?? "",
  ]
    .filter((part) => part !== "")
    .join("/");
}

function blobsURLPart(cache: resource.IResourceName) {
  switch (cache.compressor) {
    case undefined:
    case Compressor.IDENTITY:
      return "blobs";
    case Compressor.ZSTD:
      return "compressed-blobs/zstd";
    default:
      throw new Error("unsupported compressor");
  }
}

function digestFunctionURLPart(cache: resource.IResourceName) {
  switch (cache.digestFunction) {
    case undefined:
    case DigestFunction.UNKNOWN:
    case DigestFunction.SHA256:
      return "";
    case DigestFunction.BLAKE3:
      return "blake3";
    default:
      throw new Error("unsupported digest function");
  }
}
