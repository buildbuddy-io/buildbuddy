import * as path from "path";
import { existsSync } from "node:fs";

// browserifyPathPlugin remaps the nodejs "path" module to "path-browserify".
// This is needed for libsodium: https://github.com/evanw/esbuild/issues/1786
let browserifyPathPlugin = {
  name: "path-browserify",
  setup(build) {
    let entry = path.join(process.cwd(), "node_modules");
    // detect if we are building from buildbuddy-internal repo and
    // adjust 'node_modules' path accordingly
    if (!existsSync(entry)) {
      entry = path.join(process.cwd(), "external/com_github_buildbuddy_io_buildbuddy", "node_modules");
    }
    build.onResolve({ filter: /^path$/ }, () => ({
      // Note: the plugin API currently doesn't have a way to just replace
      // require("path") with require("path-browserify"); we have to resolve the
      // import to an actual file path.
      path: entry + "/path-browserify/index.js",
    }));
  },
};

export default {
  plugins: [browserifyPathPlugin],
};
