declare var window: any;

export class Capabilities {
  name: string;
  version: string;
  paths: Set<string>;

  auth: string;

  register(name: string, version: string, paths: Array<string>) {
    this.name = name;
    this.version = version;
    this.paths = new Set(paths);
    window.gtag('set', {
      'app_name': this.name,
      'app_version': this.version,
      'app_installer_id': window.location.host
    });
    window.gtag('config', 'UA-156160991-2');
    this.didNavigateToPath();
  }

  canNavigateToPath(path: string) {
    return this.paths.has(path);
  }

  didNavigateToPath() {
    window.gtag('event', 'path', {
      event_category: 'navigate',
      event_label: window.location.pathname + window.location.search + window.location.hash
    });
  }
}

export default new Capabilities();
