import rpcService from "../service/rpc_service";
import capabilities from "../capabilities/capabilities";
import { build } from "../../proto/remote_execution_ts_proto";
import { downloadString } from "./download";

/**
 * Downloads an ActionResult from the Action Cache by Action digest and optional
 * instance name, in JSON proto format.
 */
export function downloadActionResultJSON(hash: string, sizeBytes: number, instanceName = "") {
  const prefix = cachePrefix(capabilities.config.cacheApiUrl, instanceName);
  const actionResultUrl = `actioncache://${prefix}/blobs/ac/${hash}/${sizeBytes}`;
  rpcService
    .fetchBytestreamFile(actionResultUrl, /*invocationId=*/ "", "arraybuffer")
    .then((buffer: any) => {
      const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
      const formattedResult = JSON.stringify(actionResult, null, 2);
      downloadString(formattedResult, `ActionResult_${hash}_${sizeBytes}.json`, { mimeType: "application/json" });
    })
    .catch((e) => console.error("Failed to fetch action result:", e));
}

/**
 * Downloads a blob from the Content Addressable Storage (CAS) by digest and
 * optional instance name.
 */
export function downloadBlob(hash: string, sizeBytes: number, instanceName = "") {
  const prefix = cachePrefix(capabilities.config.cacheApiUrl, instanceName);
  const blobUrl = `bytestream://${prefix}/blobs/${hash}/${sizeBytes}`;
  rpcService.downloadBytestreamFile(`Blob_${hash}_${sizeBytes}.bin`, blobUrl, /*invocationId=*/ "");
}

/**
 * Returns the cache prefix suitable for bytestream:// or actioncache:// URLs.
 */
export function cachePrefix(address: string, instanceName: string) {
  address = address.replace(/^grpcs?:\/\//, "");
  if (!instanceName) return address;
  return `${address}/${instanceName}`;
}

/**
 * Makes the download utils available in the global scope so that they can
 * invoked via devtools.
 */
export function registerConsoleUtils() {
  (window as any)._downloadActionResultJSON = downloadActionResultJSON;
  (window as any)._downloadBlob = downloadBlob;
}
