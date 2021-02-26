const child_process = require("child_process");
const tty = require("tty");

const FORMATTED_EXTENSIONS = ["ts", "tsx", "js", "jsx", "css", "json", "yaml", "html", "xml"];
const FORMATTED_EXTENSIONS_REGEX = new RegExp(`.(${FORMATTED_EXTENSIONS.join("|")})$`);

function getWorkspacePath() {
  const directory = process.env["BUILD_WORKSPACE_DIRECTORY"];
  if (!directory) {
    console.error("error: BUILD_WORKSPACE_DIRECTORY is unset");
    process.exit(1);
  }
  return directory;
}

function getModifiedFilePaths(baseBranch = "master") {
  return child_process
    .execSync(`git diff --name-only --diff-filter=AMRCT '${baseBranch}'`, {
      cwd: getWorkspacePath(),
      encoding: "utf-8",
    })
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function getPrettierrcPath() {
  return `${getWorkspacePath()}/.prettierrc`;
}

function getFilePathsToFormat() {
  return getModifiedFilePaths().filter((path) => FORMATTED_EXTENSIONS_REGEX.test(path));
}

function ansiColor(code, text) {
  if (!tty.isatty(1)) return text;
  return `\x1b[${code}m${text}\x1b[0m`;
}

const color = {
  red: ansiColor.bind(null, 31),
  green: ansiColor.bind(null, 32),
  yellow: ansiColor.bind(null, 33),
  blue: ansiColor.bind(null, 34),
};

module.exports = {
  getFilePathsToFormat,
  getPrettierrcPath,
  getWorkspacePath,
  color,
};
