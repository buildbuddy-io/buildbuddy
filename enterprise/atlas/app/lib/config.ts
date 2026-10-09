import { atlas } from "../../../../proto/atlas_ts_proto";

declare global {
  var atlasConfig: object | undefined;
}

/** The FrontendConfig the server renders into the page. */
export const config = atlas.FrontendConfig.fromObject(globalThis.atlasConfig ?? {});
