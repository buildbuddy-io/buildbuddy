import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
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

function lowercaseEnumNames(e: Record<string | number, string | number>): string[] {
  return Object.keys(e).map((name) => String(name).toLowerCase());
}

const DIGEST_FUNCTION_NAMES = lowercaseEnumNames(DigestFunction);
const COMPRESSOR_NAMES = lowercaseEnumNames(Compressor);

const BYTESTREAM_URL_PATTERN = new RegExp(
  // Start of string
  `^` +
    // Protocol + "//" separator (required)
    `bytestream://` +
    // Address + "/" (required)
    `(?<address>[^/]+)/` +
    // Instance name + "/" (optional)
    `((?<instanceName>.+)/)?` +
    // blobs/ or compressed-blobs/<compressor>/ (required)
    `(blobs|compressed-blobs/(?<compressor>${COMPRESSOR_NAMES.join("|")}))/` +
    // Action cache identifier + "/" (optional)
    `((?<ac>ac)/)?` +
    // Digest function + "/" (optional - defaults to sha256)
    `((?<digestFunction>${DIGEST_FUNCTION_NAMES.join("|")})/)?` +
    // Digest hash/size (required)
    `(?<hash>[0-9a-f]+)/(?<sizeBytes>\\d+)` +
    // End of string
    `$`
);

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

type RequiredNonNullable<T> = { [k in keyof T]: NonNullable<T[k]> };

/** A fully-qualified bytestream URL. */
export interface BytestreamURL extends RequiredNonNullable<resource.IResourceName> {
  /** Bytestream service address host or host:port. */
  address: string;
}

/**
 * Returns a fully-qualified bytestream URL from the given "bytestream://..." string.
 * Throws a `TypeError` if the given URL is not a valid bytestream URL (similar to
 * the `new URL(...)` constructor).
 */
export function parseBytestreamURL(url: string): BytestreamURL {
  // Use a better type for regexp match groups (default is Record<string, string>)
  const match = url.match(BYTESTREAM_URL_PATTERN) as { groups?: Record<string, string | undefined> } | undefined;
  if (!match?.groups) {
    throw new TypeError("Invalid bytestream URL");
  }
  return {
    address: match.groups.address!,
    cacheType: match.groups.ac ? resource.CacheType.AC : resource.CacheType.CAS,
    instanceName: match.groups.instanceName ?? "",
    compressor: parseCompressor(match.groups.compressor ?? ""),
    digestFunction: parseDigestFunction(match.groups.digestFunction ?? ""),
    digest: new Digest({
      hash: match.groups.hash!,
      sizeBytes: Long.fromString(match.groups.sizeBytes!),
    }),
  };
}

/**
 * Returns whether the given value is a valid bytestream URL string.
 */
export function isBytestreamURL(url: any): url is string {
  if (typeof url !== "string") return false;
  return BYTESTREAM_URL_PATTERN.test(url);
}

/**
 * Returns a digest for the given file using the digest/length fields if
 * present, otherwise tries to parse the bytestream URL.
 */
export function getFileDigest(file: build_event_stream.IFile): Digest | undefined {
  if (file.digest) {
    return new Digest({ hash: file.digest, sizeBytes: file.length ?? Long.ZERO });
  }
  if (isBytestreamURL(file.uri)) {
    return parseBytestreamURL(file.uri).digest;
  }
  return undefined;
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
