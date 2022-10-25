import * as path from "path";

// browserifyPathPlugin remaps the nodejs "path" module to "path-browserify".
// This is needed for libsodium: https://github.com/evanw/esbuild/issues/1786
let browserifyPathPlugin = {
  name: "path-browserify",
  setup(build) {
    build.onResolve({ filter: /^path$/ }, () => ({
      // Note: the plugin API currently doesn't have a way to just replace
      // require("path") with require("path-browserify"); we have to resolve the
      // import to an actual file path.
      path: path.join(process.cwd(), "node_modules/path-browserify/index.js"),
    }));
  },
};

export default {
  plugins: [browserifyPathPlugin],
};
