import chalk from "chalk";
import fs from "fs";
import prettier from "prettier";
import { getFilePathsToFormat, getWorkspacePath } from "./common";

async function main() {
  const paths = await getFilePathsToFormat();
  const workspacePath = getWorkspacePath();
  for (const path of paths) {
    const absolutePath = `${workspacePath}/${path}`;
    const config = await prettier.resolveConfig(absolutePath);
    const source = fs.readFileSync(absolutePath, { encoding: "utf-8" });
    const options = { ...config, filepath: path };
    if (!prettier.check(source, options)) {
      fs.writeFileSync(absolutePath, prettier.format(source, options));
      process.stdout.write(`${path} ${chalk.blue("FIXED")}\n`);
    }
  }
}

main();
