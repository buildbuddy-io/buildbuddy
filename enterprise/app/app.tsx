import { LucideProvider } from "lucide-react";
import React from "react";
import ReactDOM from "react-dom";

import EnterpriseRootComponent from "./root/root";

ReactDOM.render(
  <LucideProvider className="icon">
    <EnterpriseRootComponent />
  </LucideProvider>,
  document.getElementById("app") as HTMLElement
);
