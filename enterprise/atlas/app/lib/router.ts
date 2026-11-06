/** Identifies one object: its cluster, resource type and name. */
export interface ObjectRef {
  cluster: string;
  group: string;
  version: string;
  resource: string;
  namespace: string;
  name: string;
}

export type Route =
  | { kind: "home" }
  | { kind: "search"; query: string }
  | { kind: "object"; ref: ObjectRef }
  | { kind: "clusters" }
  | { kind: "unknown"; path: string };

/** Paths within the app. */
export const paths = {
  home: "/",
  clusters: "/clusters",
  search: (query: string) => (query.trim() ? `/search?q=${encodeURIComponent(query)}` : "/"),
  object: (ref: ObjectRef) =>
    "/object/" +
    [ref.cluster, ref.group || "-", ref.version, ref.resource, ref.namespace || "-", ref.name]
      .map(encodeURIComponent)
      .join("/"),
};

export function parseRoute(pathname: string, search: string): Route {
  const parts = pathname.split("/").filter(Boolean).map(decodeURIComponent);
  if (parts.length === 0) {
    return { kind: "home" };
  }
  if (parts.length === 1 && parts[0] === "search") {
    const query = new URLSearchParams(search).get("q") ?? "";
    return query.trim() ? { kind: "search", query } : { kind: "home" };
  }
  if (parts.length === 1 && parts[0] === "clusters") {
    return { kind: "clusters" };
  }
  if (parts.length === 7 && parts[0] === "object") {
    const [, cluster, group, version, resource, namespace, name] = parts;
    return {
      kind: "object",
      ref: {
        cluster,
        group: group === "-" ? "" : group,
        version,
        resource,
        namespace: namespace === "-" ? "" : namespace,
        name,
      },
    };
  }
  return { kind: "unknown", path: pathname };
}

/**
 * History-based routing in the main app's style: pushState/replaceState are
 * patched so every navigation notifies subscribers.
 */
class Router {
  private listeners = new Set<() => void>();

  constructor() {
    const notify = () => this.listeners.forEach((listener) => listener());
    const pushState = history.pushState.bind(history);
    const replaceState = history.replaceState.bind(history);
    history.pushState = (...args: Parameters<History["pushState"]>) => {
      pushState(...args);
      notify();
    };
    history.replaceState = (...args: Parameters<History["replaceState"]>) => {
      replaceState(...args);
      notify();
    };
    window.addEventListener("popstate", notify);
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** The route for the current location. */
  current(): Route {
    return parseRoute(window.location.pathname, window.location.search);
  }

  navigateTo(path: string) {
    history.pushState(null, "", path);
  }

  /** Rewrites the current entry rather than adding one, e.g. as a query is typed. */
  replace(path: string) {
    history.replaceState(null, "", path);
  }
}

export default new Router();
