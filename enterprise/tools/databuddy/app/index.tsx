import React from "react";
import { RouterProvider, useRoute } from "react-router5";
import Home from "./Home";
import Query from "./Query";
import router, { RouteName } from "./router";
import { Toast } from "./toast";
import ReactDOM from "react-dom";

export default function App() {
  return (
    <RouterProvider router={router}>
      <Page />
      <Toast />
    </RouterProvider>
  );
}

/**
 * Mapping of route names to view components.
 */
const views: Record<RouteName, React.FC | React.ComponentClass> = {
  home: Home,
  query: Query,
};

function Page() {
  const { route } = useRoute();
  const ComponentForRoute = views[(route.name as RouteName) ?? ""] || views.home;
  return <ComponentForRoute />;
}

ReactDOM.render(<App />, document.querySelector("#root")!);
