const prettier = require("prettier");
const { getFilePathsToFormat, getPrettierrcPath, getWorkspacePath, color } = require("./common");
const fs = require("fs");

async function main() {
  const paths = getFilePathsToFormat();
  const workspacePath = getWorkspacePath();
  const config = await prettier.resolveConfig(getPrettierrcPath());
  const failedPaths = [];
  for (const path of paths) {
    const source = fs.readFileSync(`${workspacePath}/${path}`, { encoding: "utf-8" });
    process.stdout.write(`${path}\t`);
    if (
      !prettier.check(source, {
        ...config,
        filepath: path,
      })
    ) {
      process.stdout.write(color.red("FORMAT_ERRORS") + "\n");
      failedPaths.push(path);
    } else {
      process.stdout.write(color.green("OK") + "\n");
    }
  }

  if (failedPaths.length) {
    process.stderr.write(
      `\n${color.yellow(
        "Some files need formatting; to fix, run:\nbazel run //tools/prettier:format_modified_files"
      )}\n`
    );
    process.exit(1);
  }
}

main();
