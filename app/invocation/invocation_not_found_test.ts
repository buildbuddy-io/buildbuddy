import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import capabilities from "../capabilities/capabilities";
import InvocationNotFoundComponent from "./invocation_not_found";

describe("InvocationNotFoundComponent", () => {
  it("renders missing API key guidance without redirecting to login", () => {
    const previousAuth = capabilities.auth;
    capabilities.auth = "configured";

    try {
      const html = renderToStaticMarkup(
        React.createElement(InvocationNotFoundComponent, {
          invocationId: "invocation-id",
          error: null,
          missingAPIKey: true,
        })
      );

      expect(html).toContain("Missing API Key");
      expect(html).toContain("Configure an API key");
      expect(html).not.toContain("Invocation not found!");
    } finally {
      capabilities.auth = previousAuth;
    }
  });
});
