import prettier from "prettier";
import child_process from "child_process";

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

function getPrettierignorePath() {
  return `${getWorkspacePath()}/.prettierignore`;
}

async function getFilePathsToFormat() {
  const paths = [];
  for (const path of getModifiedFilePaths()) {
    if (!FORMATTED_EXTENSIONS_REGEX.test(path)) continue;

    const info = await prettier.getFileInfo(`${getWorkspacePath()}/${path}`, { ignorePath: getPrettierignorePath() });
    if (info.ignored) continue;

    paths.push(path);
  }
  return paths;
}

module.exports = {
  getFilePathsToFormat,
  getPrettierrcPath,
  getPrettierignorePath,
  getWorkspacePath,
};
