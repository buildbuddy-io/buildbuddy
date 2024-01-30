import Long from "long";
import { BytestreamURL, parseBytestreamURL } from "./cache";
import { build } from "../../proto/remote_execution_ts_proto";
import { resource } from "../../proto/resource_ts_proto";

const Digest = build.bazel.remote.execution.v2.Digest;
const DigestFunction = build.bazel.remote.execution.v2.DigestFunction;
const Compressor = build.bazel.remote.execution.v2.Compressor;

const HOST = "remote.buildbuddy.io";
const INSTANCE_NAME = "test/instance_name";
const DIGEST_HASH = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
const DIGEST_SIZE = Long.fromNumber(4);
const DIGEST = `${DIGEST_HASH}/${DIGEST_SIZE}`;

describe("parseBytestreamURL", () => {
  it("should parse a minimal URL", () => {
    const url = `bytestream://${HOST}/blobs/${DIGEST}`;
    expect(parseBytestreamURL(url)).toEqual({
      address: HOST,
      cacheType: resource.CacheType.CAS,
      instanceName: "",
      compressor: Compressor.Value.IDENTITY,
      digestFunction: DigestFunction.Value.SHA256,
      digest: new Digest({
        hash: DIGEST_HASH,
        sizeBytes: DIGEST_SIZE,
      }),
    } as BytestreamURL);
  });

  it("should parse a URL with instance name", () => {
    const url = `bytestream://${HOST}/${INSTANCE_NAME}/blobs/${DIGEST}`;
    expect(parseBytestreamURL(url)).toEqual({
      address: HOST,
      cacheType: resource.CacheType.CAS,
      instanceName: INSTANCE_NAME,
      compressor: Compressor.Value.IDENTITY,
      digestFunction: DigestFunction.Value.SHA256,
      digest: new Digest({
        hash: DIGEST_HASH,
        sizeBytes: DIGEST_SIZE,
      }),
    } as BytestreamURL);
  });

  it("should parse a URL with compressor", () => {
    const url = `bytestream://${HOST}/compressed-blobs/zstd/${DIGEST}`;
    expect(parseBytestreamURL(url)).toEqual({
      address: HOST,
      cacheType: resource.CacheType.CAS,
      instanceName: "",
      compressor: Compressor.Value.ZSTD,
      digestFunction: DigestFunction.Value.SHA256,
      digest: new Digest({
        hash: DIGEST_HASH,
        sizeBytes: DIGEST_SIZE,
      }),
    } as BytestreamURL);
  });

  it("should parse a URL with digest function", () => {
    const url = `bytestream://${HOST}/blobs/blake3/${DIGEST}`;
    expect(parseBytestreamURL(url)).toEqual({
      address: HOST,
      cacheType: resource.CacheType.CAS,
      instanceName: "",
      compressor: Compressor.Value.IDENTITY,
      digestFunction: DigestFunction.Value.BLAKE3,
      digest: new Digest({
        hash: DIGEST_HASH,
        sizeBytes: DIGEST_SIZE,
      }),
    } as BytestreamURL);
  });

  it("should parse an URL with AC cache type", () => {
    const url = `bytestream://${HOST}/blobs/ac/${DIGEST}`;
    expect(parseBytestreamURL(url)).toEqual({
      address: HOST,
      cacheType: resource.CacheType.AC,
      instanceName: "",
      compressor: Compressor.Value.IDENTITY,
      digestFunction: DigestFunction.Value.SHA256,
      digest: new Digest({
        hash: DIGEST_HASH,
        sizeBytes: DIGEST_SIZE,
      }),
    } as BytestreamURL);
  });

  it("should parse a potentially ambiguous instance name", () => {
    const url = `bytestream://${HOST}/blobs/blobs/${DIGEST}`;
    expect(parseBytestreamURL(url)).toEqual({
      address: HOST,
      cacheType: resource.CacheType.CAS,
      instanceName: "blobs",
      compressor: Compressor.Value.IDENTITY,
      digestFunction: DigestFunction.Value.SHA256,
      digest: new Digest({
        hash: DIGEST_HASH,
        sizeBytes: DIGEST_SIZE,
      }),
    } as BytestreamURL);
  });
});
