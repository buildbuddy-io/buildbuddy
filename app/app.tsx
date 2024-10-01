import React from "react";
import ReactDOM from "react-dom";

import RootComponent from "./root/root";

/* Monaco loads some libraries using require(), ignore these for now. */
// @ts-ignore
window.require = () => {};

ReactDOM.render(<RootComponent />, document.getElementById("app") as HTMLElement);
