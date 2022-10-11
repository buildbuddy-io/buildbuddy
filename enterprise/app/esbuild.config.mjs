import * as fs from "fs";
import * as path from "path";

// browserifyPathPlugin remaps the nodejs "path" module to "path-browserify".
// This is needed for libsodium: https://github.com/evanw/esbuild/issues/1786
let browserifyPathPlugin = {
  name: "path-browserify",
  setup(build) {
    build.onResolve({ filter: /^path$/ }, () => {
      // Note: the plugin API currently doesn't have a way to just replace
      // require("path") with require("path-browserify"). Instead, we have to
      // resolve the import to an actual file path.
      let modulePath = "node_modules/path-browserify/index.js";

      if (!fs.existsSync(modulePath)) {
        // If path-browserify doesn't exist, check the external directory (in
        // case we're building externally).
        const externalModulePath = "external/com_github_buildbuddy_io_buildbuddy/node_modules/path-browserify/index.js";
        if (fs.existsSync(externalModulePath)) {
          modulePath = externalModulePath;
        } else {
          console.error(
            "ERROR: path-browserify esbuild plugin: could not locate path-browserify module in " + process.cwd()
          );
          process.exit(1);
        }
      }

      return {
        path: path.join(process.cwd(), modulePath),
      };
    });
  },
};

export default {
  resolveExtensions: [".mjs", ".js"],
  loader: {
    ".ttf": "binary",
    ".css": "binary",
  },
  plugins: [browserifyPathPlugin],
};
