import { config } from "../../proto/config_ts_proto";
import capabilities from "../capabilities/capabilities";
import rpcService from "./rpc_service";

describe("regional execution requests", () => {
  const usServer = "https://app.buildbuddy.io";
  const euServer = "https://app.europe.buildbuddy.io";
  let previousRegions: config.Region[];
  let previousServices: typeof rpcService.regionalServices;
  let previousWindow: Window;
  const usService = Object.create(rpcService.service);
  const euService = Object.create(rpcService.service);

  beforeEach(() => {
    previousRegions = capabilities.config.regions;
    previousServices = rpcService.regionalServices;
    previousWindow = (globalThis as any).window;
    (globalThis as any).window = { location: { href: "https://test-org.buildbuddy.io/invocation/example" } };
    // Build endpoints resolve to the configured app servers, not to themselves.
    capabilities.config.regions = [
      new config.Region({ name: "US", server: usServer, subdomains: "https://*.buildbuddy.io" }),
      new config.Region({ name: "Europe", server: euServer, subdomains: "https://*.europe.buildbuddy.io" }),
    ];
    rpcService.regionalServices = new Map([
      ["US", usService],
      ["Europe", euService],
    ]);
  });

  afterEach(() => {
    capabilities.config.regions = previousRegions;
    rpcService.regionalServices = previousServices;
    (globalThis as any).window = previousWindow;
  });

  for (const { name, endpoint, expected } of [
    { name: "public US endpoint", endpoint: "grpcs://remote.buildbuddy.io", expected: usServer },
    { name: "bare public US endpoint", endpoint: "remote.buildbuddy.io", expected: usServer },
    { name: "bare public Europe endpoint", endpoint: "remote.europe.buildbuddy.io", expected: euServer },
    {
      name: "bare Europe organization default port",
      endpoint: "test-org.europe.buildbuddy.io:443",
      expected: euServer,
    },
    { name: "public Europe endpoint", endpoint: "grpcs://remote.europe.buildbuddy.io", expected: euServer },
    { name: "US organization endpoint", endpoint: "grpcs://test-org.buildbuddy.io", expected: usServer },
    { name: "Europe organization endpoint", endpoint: "grpcs://test-org.europe.buildbuddy.io", expected: euServer },
    { name: "public US default port", endpoint: "grpcs://remote.buildbuddy.io:443", expected: usServer },
    { name: "public Europe default port", endpoint: "grpcs://remote.europe.buildbuddy.io:443", expected: euServer },
    { name: "US organization default port", endpoint: "grpcs://test-org.buildbuddy.io:443", expected: usServer },
    {
      name: "Europe organization default port",
      endpoint: "grpcs://test-org.europe.buildbuddy.io:443",
      expected: euServer,
    },
    { name: "secure HTTP Europe endpoint", endpoint: "https://remote.europe.buildbuddy.io:443", expected: euServer },
    { name: "canonical US app server", endpoint: usServer, expected: usServer },
    { name: "canonical Europe app server", endpoint: euServer, expected: euServer },
  ]) {
    it(`maps the ${name} to its configured app and named service`, () => {
      expect(rpcService.getRegionalServerOrDefault(endpoint)).toBe(expected);
      expect(rpcService.getRegionalServiceOrDefault(endpoint)).toBe(expected === euServer ? euService : usService);
    });
  }

  for (const { name, endpoint } of [
    { name: "insecure gRPC", endpoint: "grpc://remote.europe.buildbuddy.io" },
    { name: "unconfigured HTTP", endpoint: "http://remote.europe.buildbuddy.io" },
    { name: "unconfigured port", endpoint: "grpcs://test-org.europe.buildbuddy.io:8443" },
    { name: "lookalike domain", endpoint: "grpcs://test-org.evilbuildbuddy.io" },
    { name: "path", endpoint: "grpcs://remote.europe.buildbuddy.io/path" },
    { name: "query", endpoint: "grpcs://remote.europe.buildbuddy.io?query=true" },
    { name: "fragment", endpoint: "grpcs://remote.europe.buildbuddy.io#fragment" },
    { name: "backslash path", endpoint: "grpcs://remote.europe.buildbuddy.io\\path" },
    { name: "userinfo", endpoint: "grpcs://user@remote.europe.buildbuddy.io" },
    { name: "bare-host path boundary", endpoint: "remote.europe.buildbuddy.io/path" },
    { name: "bare-host query boundary", endpoint: "remote.europe.buildbuddy.io?query=true" },
    { name: "bare-host userinfo boundary", endpoint: "user@remote.europe.buildbuddy.io" },
    { name: "bare-host unconfigured port boundary", endpoint: "remote.europe.buildbuddy.io:8443" },
    { name: "missing endpoint", endpoint: "" },
  ]) {
    it(`keeps an endpoint with ${name} same-origin`, () => {
      expect(rpcService.getRegionalServerOrDefault(endpoint)).toBe("");
      expect(rpcService.getRegionalServiceOrDefault(endpoint)).toBe(rpcService.service);
    });
  }

  for (const { name, server, endpoint } of [
    {
      name: "secure nondefault port",
      server: "https://regional.example:8443",
      endpoint: "grpcs://regional.example:8443",
    },
    {
      name: "plaintext HTTP endpoint",
      server: "http://regional.example:8080",
      endpoint: "http://regional.example:8080",
    },
    {
      name: "plaintext gRPC endpoint",
      server: "http://regional.example:8080",
      endpoint: "grpc://regional.example:8080",
    },
    {
      name: "explicit secure default port",
      server: "https://regional.example:443",
      endpoint: "grpcs://regional.example",
    },
    { name: "trailing server slash", server: "https://regional.example/", endpoint: "grpcs://regional.example" },
    {
      name: "trailing default-port server slash",
      server: "https://regional.example:443/",
      endpoint: "grpcs://regional.example",
    },
  ]) {
    it(`supports private-deployment ${name} compatibility`, async () => {
      capabilities.config.regions.push(new config.Region({ name: "custom", server }));
      rpcService.regionalServices.set("custom", euService);
      expect(rpcService.getRegionalServerOrDefault(endpoint)).toBe(server);
      expect(rpcService.getRegionalServiceOrDefault(endpoint)).toBe(euService);
      expect(rpcService.getRegionalServerOrDefault("https://regional.example:9443")).toBe("");
      expect(rpcService.getRegionalServerOrDefault("grpc://regional.example:9443")).toBe("");
      const fetch = spyOn(rpcService, "fetch").and.returnValue(Promise.resolve("profile"));
      for (const view of [false, true]) {
        const url = rpcService.getDownloadUrl({ artifact: "execution_profile" }, view, endpoint);
        expect(new URL(url).origin).toBe(new URL(server).origin);
        expect(new URL(url).pathname).toBe(`/file/${view ? "view" : "download"}`);
        await rpcService.fetchFile(url);
        expect(fetch.calls.mostRecent().args[2]?.credentials).toBe("include");
      }
    });
  }

  for (const { scheme, grpcScheme, port, wrongScheme, wrongGRPCScheme, wrongPort } of [
    {
      scheme: "http",
      grpcScheme: "grpc",
      port: "8080",
      wrongScheme: "https",
      wrongGRPCScheme: "grpcs",
      wrongPort: "8081",
    },
    {
      scheme: "https",
      grpcScheme: "grpcs",
      port: "8443",
      wrongScheme: "http",
      wrongGRPCScheme: "grpc",
      wrongPort: "8444",
    },
  ]) {
    it(`preserves compatibility with configured ${scheme} wildcard scheme and port`, () => {
      const server = `${scheme}://app.example.local:${port}`;
      const endpoint = `${scheme}://runner.example.local:${port}`;
      capabilities.config.regions.push(
        new config.Region({ name: "custom", server, subdomains: `${scheme}://*.example.local:${port}` })
      );
      rpcService.regionalServices.set("custom", euService);
      expect(rpcService.getRegionalServerOrDefault(endpoint)).toBe(server);
      expect(rpcService.getRegionalServiceOrDefault(endpoint)).toBe(euService);
      const grpcEndpoint = `${grpcScheme}://runner.example.local:${port}`;
      expect(rpcService.getRegionalServerOrDefault(grpcEndpoint)).toBe(server);
      expect(rpcService.getRegionalServiceOrDefault(grpcEndpoint)).toBe(euService);
      expect(new URL(rpcService.getDownloadUrl({ artifact: "execution_profile" }, false, endpoint)).origin).toBe(
        server
      );
      for (const disallowedEndpoint of [
        `${wrongScheme}://runner.example.local:${port}`,
        `${scheme}://runner.example.local:${wrongPort}`,
        `${scheme}://runner.example.local`,
        `${wrongGRPCScheme}://runner.example.local:${port}`,
      ]) {
        expect(rpcService.getRegionalServerOrDefault(disallowedEndpoint)).toBe("");
        expect(rpcService.getRegionalServiceOrDefault(disallowedEndpoint)).toBe(rpcService.service);
      }
    });
  }

  for (const { scheme, grpcScheme, port } of [
    { scheme: "http", grpcScheme: "grpc", port: "80" },
    { scheme: "https", grpcScheme: "grpcs", port: "443" },
  ]) {
    it(`normalizes configured ${scheme} wildcard default-port compatibility`, () => {
      const server = `${scheme}://app.example.local:${port}`;
      capabilities.config.regions.push(
        new config.Region({ name: "custom", server, subdomains: `${scheme}://*.example.local:${port}` })
      );
      rpcService.regionalServices.set("custom", euService);
      for (const endpoint of [`${grpcScheme}://runner.example.local`, `${grpcScheme}://runner.example.local:${port}`]) {
        expect(rpcService.getRegionalServerOrDefault(endpoint)).toBe(server);
        expect(rpcService.getRegionalServiceOrDefault(endpoint)).toBe(euService);
        expect(new URL(rpcService.getDownloadUrl({ artifact: "execution_profile" }, false, endpoint)).origin).toBe(
          `${scheme}://app.example.local`
        );
      }
    });
  }

  for (const { name, subdomains } of [
    { name: "path", subdomains: "https://*.example.local/path" },
    { name: "query", subdomains: "https://*.example.local?query=true" },
    { name: "userinfo", subdomains: "https://user@*.example.local" },
  ]) {
    it(`rejects a configured wildcard ${name} boundary without allowing its origin`, () => {
      capabilities.config.regions.push(
        new config.Region({ name: "invalid", server: "https://app.example.local", subdomains })
      );
      rpcService.regionalServices.set("invalid", euService);
      expect(rpcService.getRegionalServerOrDefault("grpcs://runner.example.local")).toBe("");
      expect(rpcService.getRegionalServiceOrDefault("grpcs://runner.example.local")).toBe(rpcService.service);
    });
  }

  it("keeps the bare-host TLS boundary from selecting a configured plaintext region", () => {
    capabilities.config.regions.push(
      new config.Region({
        name: "plaintext",
        server: "http://app.example.local:8080",
        subdomains: "http://*.example.local:8080",
      })
    );
    rpcService.regionalServices.set("plaintext", euService);
    expect(rpcService.getRegionalServerOrDefault("grpc://runner.example.local:8080")).toBe(
      "http://app.example.local:8080"
    );
    expect(rpcService.getRegionalServiceOrDefault("grpc://runner.example.local:8080")).toBe(euService);
    expect(rpcService.getRegionalServerOrDefault("runner.example.local:8080")).toBe("");
    expect(rpcService.getRegionalServiceOrDefault("runner.example.local:8080")).toBe(rpcService.service);
  });

  it("rejects a configuration security boundary that would downgrade a secure endpoint", () => {
    capabilities.config.regions.push(
      new config.Region({
        name: "downgrade",
        server: "http://app.example.local",
        subdomains: "https://*.example.local",
      })
    );
    rpcService.regionalServices.set("downgrade", euService);
    expect(rpcService.getRegionalServerOrDefault("grpcs://runner.example.local")).toBe("");
    expect(rpcService.getRegionalServiceOrDefault("grpcs://runner.example.local")).toBe(rpcService.service);
  });

  it("handles a malformed-config boundary without dropping valid-region credentials", async () => {
    capabilities.config.regions.unshift(new config.Region({ name: "invalid", server: "not an app URL" }));
    const endpoint = "grpcs://test-org.europe.buildbuddy.io:443";
    expect(rpcService.getRegionalServerOrDefault(endpoint)).toBe(euServer);
    expect(rpcService.getRegionalServiceOrDefault(endpoint)).toBe(euService);
    const url = rpcService.getDownloadUrl({ artifact: "execution_profile" }, false, endpoint);
    expect(new URL(url).origin).toBe(euServer);
    const fetch = spyOn(rpcService, "fetch").and.returnValue(Promise.resolve("profile"));
    await rpcService.fetchFile(url);
    expect(fetch.calls.mostRecent().args[2]?.credentials).toBe("include");
    await rpcService.fetchFile("https://untrusted.example/file/download");
    expect(fetch.calls.mostRecent().args[2]?.credentials).toBeUndefined();
  });

  it("selects services indexed by region name", () => {
    expect(rpcService.getRegionalServiceOrDefault("grpcs://remote.europe.buildbuddy.io")).toBe(
      rpcService.regionalServices.get("Europe")!
    );
    expect(rpcService.regionalServices.has(euServer)).toBe(false);
  });

  it("keeps unconfigured endpoints same-origin", () => {
    capabilities.config.regions = [];
    expect(rpcService.getRegionalServerOrDefault("grpcs://remote.europe.buildbuddy.io")).toBe("");
  });

  for (const { name, pageOrigin, endpoint, appServer } of [
    {
      name: "global organization page to Europe execution",
      pageOrigin: "https://test-org.buildbuddy.io",
      endpoint: "grpcs://test-org.europe.buildbuddy.io:443",
      appServer: euServer,
    },
    {
      name: "Europe organization page to US execution",
      pageOrigin: "https://test-org.europe.buildbuddy.io",
      endpoint: "grpcs://test-org.buildbuddy.io:443",
      appServer: usServer,
    },
  ]) {
    it(`routes profile downloads and views from a ${name}`, async () => {
      (globalThis as any).window = { location: { href: `${pageOrigin}/invocation/example` } };
      const params = { invocation_id: "example", artifact: "execution_profile" };
      const fetch = spyOn(rpcService, "fetch").and.returnValue(Promise.resolve("profile"));
      for (const view of [false, true]) {
        const localUrl = rpcService.getDownloadUrl(params, view);
        const regionalUrl = rpcService.getDownloadUrl(params, view, endpoint);
        expect(regionalUrl).toBe(`${appServer}${localUrl}`);
        expect(new URL(regionalUrl).pathname).toBe(`/file/${view ? "view" : "download"}`);
        expect(new URL(regionalUrl).searchParams.get("request_context")).not.toBeNull();
        expect(rpcService.getDownloadUrl(params, view, "https://untrusted.example")).toBe(localUrl);
        await rpcService.fetchFile(regionalUrl);
        expect(fetch.calls.mostRecent().args[2]?.credentials).toBe("include");
      }
    });
  }

  for (const credentials of ["omit", "same-origin"] as const) {
    it(`preserves an explicit ${credentials} credential policy for regional profile downloads`, async () => {
      const endpoint = "grpcs://test-org.europe.buildbuddy.io:443";
      const url = rpcService.getDownloadUrl({ artifact: "execution_profile" }, false, endpoint);
      expect(new URL(url).origin).toBe(euServer);
      const fetch = spyOn(rpcService, "fetch").and.returnValue(Promise.resolve("profile"));
      await rpcService.fetchFile(url, "text", { credentials });
      expect(fetch.calls.mostRecent().args[2]?.credentials).toBe(credentials);
    });
  }

  it("includes credentials only for configured app origins", async () => {
    const fetch = spyOn(rpcService, "fetch").and.returnValue(Promise.resolve("profile"));
    const cases: { url: string; credentials: RequestCredentials | undefined }[] = [
      { url: `${usServer}/file/download`, credentials: "include" },
      { url: `${euServer}/file/download`, credentials: "include" },
      { url: `${euServer}:443/file/view`, credentials: "include" },
      { url: "https://untrusted.example/file/download", credentials: undefined },
      { url: "https://app.europe.buildbuddy.io.evil.example/file/download", credentials: undefined },
      { url: "http://app.europe.buildbuddy.io/file/download", credentials: undefined },
      { url: "https://app.europe.buildbuddy.io:8443/file/download", credentials: undefined },
    ];
    for (const { url, credentials } of cases) {
      await rpcService.fetchFile(url);
      expect(fetch.calls.mostRecent().args[2]?.credentials).toBe(credentials);
    }
  });
});
