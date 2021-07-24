import { Path } from "./router";

type Route = {
  prefix: string;

  /**
   * Intercepts the navigation and potentially does something else.
   * Returns true if navigation was intercepted.
   */
  intercept?: (url: string) => boolean;
};

export const ROUTES = [
  {
    prefix: Path.setupPath,
  },
  {},
  // Home route should remain last in this list (since it is the "catch-all" route).
  {
    prefix: "/",
  },
];
