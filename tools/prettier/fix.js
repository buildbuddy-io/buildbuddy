const prettier = require("prettier");
const { getFilePathsToFormat, getPrettierrcPath, getWorkspacePath, color } = require("./common");
const fs = require("fs");

async function main() {
  const paths = getFilePathsToFormat();
  const workspacePath = getWorkspacePath();
  const config = await prettier.resolveConfig(getPrettierrcPath());
  for (const path of paths) {
    const absolutePath = `${workspacePath}/${path}`;
    const source = fs.readFileSync(absolutePath, { encoding: "utf-8" });
    const options = { ...config, filepath: path };
    if (!prettier.check(source, options)) {
      fs.writeFileSync(absolutePath, prettier.format(source, options));
      process.stdout.write(`${path} ${color.blue("FIXED")}\n`);
    }
  }
}

main();
