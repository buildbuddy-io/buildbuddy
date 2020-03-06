export class Capabilities {
  name: string;
  paths: Set<string>;

  auth: string;

  register(name: string, paths: Array<string>) {
    this.name = name;
    this.paths = new Set(paths);
  }

  canNavigateToPath(path: string) {
    return this.paths.has(path);
  }
}

export default new Capabilities();