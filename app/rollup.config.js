const node = require("rollup-plugin-node-resolve");
const commonjs = require("rollup-plugin-commonjs");
const replace = require("rollup-plugin-replace");

module.exports = {
  plugins: [
    node({
      browser: true,
      mainFields: ["browser", "es2015", "module", "jsnext:main", "main"],
      preferBuiltins: false,
    }),
    commonjs({
      namedExports: {
        "protobufjs/minimal": ["rpc", "roots", "util", "Reader", "Writer"],
        "node_modules/react-dom/index.js": ["findDOMNode"],
        "node_modules/react/index.js": [
          "createElement",
          "PureComponent",
          "Children",
          "Component",
          "cloneElement",
          "isValidElement",
        ],
        "node_modules/react-lazylog/build/index.js": ["LazyLog"],
      },
      sourceMap: false,
      transformMixedEsModules: true,
    }),
    replace({
      "process.env.NODE_ENV": JSON.stringify("production"),
    }),
  ],
  onwarn: function (message) {
    if (
      message.code === "EVAL" ||
      message.code === "CIRCULAR_DEPENDENCY" ||
      message.code === "THIS_IS_UNDEFINED" ||
      message.code === "NON_EXISTENT_EXPORT"
    ) {
      return;
    }
    console.warn(message);
  },
};
