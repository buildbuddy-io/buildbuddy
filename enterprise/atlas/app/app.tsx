import { LucideProvider } from "lucide-react";
import React from "react";
import ReactDOM from "react-dom";

import RootComponent from "./root/root";

ReactDOM.render(
  <LucideProvider className="icon">
    <RootComponent />
  </LucideProvider>,
  document.getElementById("app") as HTMLElement
);
