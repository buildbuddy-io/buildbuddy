const node = require('rollup-plugin-node-resolve');
const commonjs = require('rollup-plugin-commonjs');
const replace = require('rollup-plugin-replace');

module.exports = {
  plugins: [
    node({
      mainFields: ['browser', 'es2015', 'module', 'jsnext:main', 'main'],
    }),
    commonjs({
      namedExports: { "protobufjs/minimal": ["rpc", "roots", "util", "Reader", "Writer"] },
      sourceMap: false
    }),
    replace({
      'process.env.NODE_ENV': JSON.stringify('production')
    }),
  ],
  onwarn: function (message) {
    if (message.code === 'EVAL' || message.code === 'CIRCULAR_DEPENDENCY') {
      return;
    }
    console.warn(message);
  }
};