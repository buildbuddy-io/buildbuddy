import createRouter, { Route } from "router5";
import browserPlugin from "router5-plugin-browser";

const routes = [
  { name: "home", path: "/" },
  { name: "query", path: "/q/:id?sort&sortDir" },
] as const;

export type RouteName = (typeof routes)[number]["name"];

type Mutable<T> = {
  -readonly [K in keyof T]: T[K];
};

const router = createRouter(routes as any as Mutable<typeof routes>);
router.usePlugin(browserPlugin());
router.start();

export default router;
