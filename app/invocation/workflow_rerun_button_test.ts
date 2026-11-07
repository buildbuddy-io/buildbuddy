import React from "react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { config } from "../../proto/config_ts_proto";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { firecracker } from "../../proto/firecracker_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { workflow } from "../../proto/workflow_ts_proto";
import capabilities from "../capabilities/capabilities";
import errorService from "../errors/error_service";
import router from "../router/router";
import rpcService, { CancelablePromise, ExtendedBuildBuddyService } from "../service/rpc_service";
import InvocationModel from "./invocation_model";
import WorkflowRerunButton from "./workflow_rerun_button";

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const resolved = <T>(value: T) => new CancelablePromise(Promise.resolve(value));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new CancelablePromise<T>(
    new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    })
  );
  return { promise, resolve, reject };
}

function elements(node: React.ReactNode): React.ReactElement<any>[] {
  const result: React.ReactElement<any>[] = [];
  React.Children.forEach(node, (child) => {
    if (!React.isValidElement(child)) return;
    result.push(child);
    result.push(...elements((child.props as any).children));
  });
  return result;
}

function text(node: React.ReactNode): string {
  let result = "";
  React.Children.forEach(node, (child) => {
    if (typeof child === "string") result += child;
    else if (React.isValidElement(child)) result += text((child.props as any).children);
  });
  return result;
}

for (const { name, endpoint, executionHost, region, separateCacheEndpoint } of [
  {
    name: "global public",
    endpoint: "grpcs://remote.buildbuddy.io",
    executionHost: "remote.buildbuddy.io",
    region: "US",
    separateCacheEndpoint: "grpcs://remote.europe.buildbuddy.io",
  },
  {
    name: "global organization",
    endpoint: "grpcs://test-org.buildbuddy.io",
    executionHost: "test-org.buildbuddy.io",
    region: "US",
    separateCacheEndpoint: "grpcs://remote.europe.buildbuddy.io",
  },
  {
    name: "Europe public",
    region: "Europe",
    separateCacheEndpoint: "grpcs://remote.buildbuddy.io",
    endpoint: "grpcs://remote.europe.buildbuddy.io",
    executionHost: "remote.europe.buildbuddy.io",
  },
  {
    name: "Europe organization",
    region: "Europe",
    separateCacheEndpoint: "grpcs://remote.buildbuddy.io",
    endpoint: "grpcs://test-org.europe.buildbuddy.io",
    executionHost: "test-org.europe.buildbuddy.io",
  },
  {
    name: "Europe organization bare hostname",
    region: "Europe",
    separateCacheEndpoint: "remote.buildbuddy.io",
    endpoint: "test-org.europe.buildbuddy.io",
    executionHost: "test-org.europe.buildbuddy.io",
  },
  {
    name: "Europe organization bare hostname with default port",
    region: "Europe",
    separateCacheEndpoint: "remote.buildbuddy.io:443",
    endpoint: "test-org.europe.buildbuddy.io:443",
    executionHost: "test-org.europe.buildbuddy.io:443",
  },
]) {
  describe(`WorkflowRerunButton ${name} reruns`, () => {
    let button: WorkflowRerunButton;
    let regional: ExtendedBuildBuddyService;
    let getExecution: jasmine.Spy;
    let invalidateSnapshot: jasmine.Spy;
    let executeWorkflow: jasmine.Spy;
    let fetchFile: jasmine.Spy;
    let navigate: jasmine.Spy;
    let handleError: jasmine.Spy;
    let selectRegion: jasmine.Spy;
    let previousRegions: config.Region[];
    let previousServices: typeof rpcService.regionalServices;
    const snapshotKey = new firecracker.SnapshotKey({ instanceName: "workflow", snapshotId: "snapshot" });

    beforeEach(() => {
      const model = new InvocationModel(new invocation.Invocation({ invocationId: "original", role: "CI_RUNNER" }));
      model.optionsMap.set("rbe_backend", endpoint);
      model.optionsMap.set("cache_backend", endpoint);
      model.optionsMap.set("bes_backend", endpoint);
      model.workflowConfigured = new build_event_stream.WorkflowConfigured({
        workflowId: "workflow",
        actionName: "test",
        pushedRepoUrl: "https://example.com/repo",
        pushedBranch: "main",
        commitSha: "commit",
        targetRepoUrl: "https://example.com/repo",
        targetBranch: "main",
      });
      button = new WorkflowRerunButton({ model });
      spyOn(button, "setState").and.callFake((update: any) => {
        button.state = {
          ...button.state,
          ...(typeof update === "function" ? update(button.state, button.props) : update),
        };
      });
      regional = Object.create(rpcService.service);
      getExecution = regional.getExecution = jasmine.createSpy().and.returnValue(
        resolved(
          execution_stats.GetExecutionResponse.fromObject({
            execution: [{ executeResponseDigest: { hash: "response", sizeBytes: 1 } }],
          })
        )
      );
      invalidateSnapshot = regional.invalidateSnapshot = jasmine
        .createSpy()
        .and.returnValue(resolved(new workflow.InvalidateSnapshotResponse()));
      executeWorkflow = regional.executeWorkflow = jasmine.createSpy().and.returnValue(
        resolved(
          workflow.ExecuteWorkflowResponse.fromObject({
            actionStatuses: [{ actionName: "test", invocationId: "rerun", status: { code: 0 } }],
          })
        )
      );
      previousRegions = capabilities.config.regions;
      previousServices = rpcService.regionalServices;
      capabilities.config.regions = [
        new config.Region({ name: "US", server: "https://app.buildbuddy.io", subdomains: "https://*.buildbuddy.io" }),
        new config.Region({
          name: "Europe",
          server: "https://app.europe.buildbuddy.io",
          subdomains: "https://*.europe.buildbuddy.io",
        }),
      ];
      rpcService.regionalServices = new Map([[region, regional]]);
      selectRegion = spyOn(rpcService, "getRegionalServiceOrDefault").and.callThrough();
      fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValue(resolved(executeResponseBytes(true)));
      navigate = spyOn(router, "navigateTo");
      handleError = spyOn(errorService, "handleError");
      spyOn(rpcService.service, "getExecution");
      spyOn(rpcService.service, "executeWorkflow");
      spyOn(rpcService.service, "invalidateSnapshot");
    });

    afterEach(() => {
      button.componentWillUnmount();
      capabilities.config.regions = previousRegions;
      rpcService.regionalServices = previousServices;
    });

    function executeResponseBytes(withSnapshot: boolean): ArrayBuffer {
      const response = build.bazel.remote.execution.v2.ExecuteResponse.fromObject({
        result: {
          executionMetadata: {
            auxiliaryMetadata: withSnapshot
              ? [
                  {
                    typeUrl: "type.googleapis.com/firecracker.VMMetadata",
                    value: firecracker.VMMetadata.encode(new firecracker.VMMetadata({ snapshotKey })).finish(),
                  },
                ]
              : [],
          },
        },
      });
      const bytes = build.bazel.remote.execution.v2.ActionResult.encode(
        new build.bazel.remote.execution.v2.ActionResult({
          stdoutRaw: build.bazel.remote.execution.v2.ExecuteResponse.encode(response).finish(),
        })
      ).finish();
      return new Uint8Array(bytes).buffer;
    }

    function click(label: string) {
      const element = elements(button.render()).find(
        (element) => element.props.onClick && text(element.props.children) === label
      );
      if (!element) throw new Error(`Missing rendered button: ${label}`);
      expect(element.props.disabled).not.toBe(true);
      return element.props.onClick();
    }

    function cleanRerun() {
      const menuButton = elements(button.render()).find((element) => element.props.className === "icon-button");
      if (!menuButton) throw new Error("Missing clean-rerun menu button");
      menuButton.props.onClick();
      expect(button.state.isMenuOpen).toBe(true);
      click("Re-run from clean workspace");
      expect(button.state.isDialogOpen).toBe(true);
      return click("OK");
    }

    it("reads and reruns the enclosing workflow in its execution region", async () => {
      await click("Re-run");
      await flush();
      expect(getExecution.calls.first().args[0].executionLookup.invocationId).toBe("original");
      expect(executeWorkflow.calls.first().args[0].workflowId).toBe("workflow");
      expect(executeWorkflow.calls.first().args[0].actionNames).toEqual(["test"]);
      expect(selectRegion.calls.allArgs().every(([selected]) => selected === endpoint)).toBe(true);
      expect(rpcService.service.getExecution).not.toHaveBeenCalled();
      expect(rpcService.service.executeWorkflow).not.toHaveBeenCalled();
      expect(fetchFile).not.toHaveBeenCalled();
      expect(invalidateSnapshot).not.toHaveBeenCalled();
      expect(navigate).toHaveBeenCalledWith("/invocation/rerun?queued=true");
      expect(button.state.isLoading).toBe(false);
    });

    it("preserves manual-dispatch environment overrides from runner CAS when the build cache is in another region", async () => {
      const model = button.props.model;
      model.optionsMap.set("cache_backend", separateCacheEndpoint);
      model.optionsMap.set("remote_instance_name", "workflow-instance");
      model.optionsMap.set("digest_function", "BLAKE3");
      model.workflowConfigured!.actionTriggerEvent = "manual_dispatch";
      const actionDigest = build.bazel.remote.execution.v2.Digest.fromObject({ hash: "action", sizeBytes: 10 });
      const commandDigest = build.bazel.remote.execution.v2.Digest.fromObject({ hash: "command", sizeBytes: 20 });
      getExecution.and.returnValue(
        resolved(
          new execution_stats.GetExecutionResponse({ execution: [new execution_stats.Execution({ actionDigest })] })
        )
      );
      const actionURL = `bytestream://${executionHost}/workflow-instance/blobs/blake3/action/10`;
      const commandURL = `bytestream://${executionHost}/workflow-instance/blobs/blake3/command/20`;
      const env = { WORKFLOW_MODE: "manual", TEST_SHARD: "2" };
      const actionBytes = build.bazel.remote.execution.v2.Action.encode(
        new build.bazel.remote.execution.v2.Action({ commandDigest })
      ).finish();
      const commandBytes = build.bazel.remote.execution.v2.Command.encode(
        build.bazel.remote.execution.v2.Command.fromObject({
          environmentVariables: Object.entries(env).map(([name, value]) => ({ name, value })),
        })
      ).finish();
      fetchFile.and.callFake((url: string) => {
        if (url === actionURL) return resolved(new Uint8Array(actionBytes).buffer);
        if (url === commandURL) return resolved(new Uint8Array(commandBytes).buffer);
        return new CancelablePromise(Promise.reject(new Error(`Artifact is not in the build cache: ${url}`)));
      });

      await click("Re-run");
      await flush();

      expect(model.getCacheEndpoint()).toBe(separateCacheEndpoint);
      expect(fetchFile.calls.allArgs()).toEqual([
        [actionURL, "original", "arraybuffer"],
        [commandURL, "original", "arraybuffer"],
      ]);
      expect(getExecution).toHaveBeenCalledTimes(1);
      expect(executeWorkflow).toHaveBeenCalledTimes(1);
      expect(executeWorkflow.calls.first().args[0].env).toEqual(env);
      expect(selectRegion.calls.allArgs().every(([selected]) => selected === endpoint)).toBe(true);
      expect(rpcService.service.getExecution).not.toHaveBeenCalled();
      expect(rpcService.service.executeWorkflow).not.toHaveBeenCalled();
      expect(navigate).toHaveBeenCalledWith("/invocation/rerun?queued=true");
      expect(handleError).not.toHaveBeenCalled();
      expect(button.state.isLoading).toBe(false);
    });

    it("waits for regional snapshot invalidation before launching a clean rerun with a global cache backend", async () => {
      button.props.model.optionsMap.set("cache_backend", "grpcs://remote.buildbuddy.io");
      const pending = deferred<workflow.InvalidateSnapshotResponse>();
      invalidateSnapshot.and.returnValue(pending.promise);
      const rerun = cleanRerun();
      await flush();
      expect(invalidateSnapshot.calls.first().args[0].snapshotKey).toEqual(snapshotKey);
      expect(fetchFile.calls.first().args[0]).toContain(`actioncache://${executionHost}/`);
      expect(rpcService.service.invalidateSnapshot).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(button.state.isLoading).toBe(true);
      pending.resolve(new workflow.InvalidateSnapshotResponse());
      await rerun;
      await flush();
      expect(executeWorkflow).toHaveBeenCalledTimes(1);
      expect(navigate).toHaveBeenCalledWith("/invocation/rerun?queued=true");
      expect(handleError).not.toHaveBeenCalled();
    });

    it("does not launch when regional snapshot invalidation fails", async () => {
      const pending = deferred<workflow.InvalidateSnapshotResponse>();
      invalidateSnapshot.and.returnValue(pending.promise);
      const rerun = cleanRerun();
      await flush();
      pending.reject(new Error("invalidation failed"));
      await rerun;
      expect(executeWorkflow).not.toHaveBeenCalled();
      expect(navigate).not.toHaveBeenCalled();
      expect(handleError.calls.first().args[0]).toContain("Failed to invalidate snapshot");
      expect(button.state.isLoading).toBe(false);
    });

    it("uses shared repository invalidation when the workflow has no VM snapshot", async () => {
      fetchFile.and.returnValue(resolved(executeResponseBytes(false)));
      const pending = deferred<workflow.InvalidateAllSnapshotsForRepoResponse>();
      const invalidateRepo = spyOn(rpcService.service, "invalidateAllSnapshotsForRepo").and.returnValue(
        pending.promise
      );
      const rerun = cleanRerun();
      await flush();
      expect(invalidateRepo.calls.first().args[0].repoUrl).toBe("https://example.com/repo");
      expect(invalidateSnapshot).not.toHaveBeenCalled();
      expect(executeWorkflow).not.toHaveBeenCalled();
      pending.resolve(new workflow.InvalidateAllSnapshotsForRepoResponse());
      await rerun;
      await flush();
      expect(executeWorkflow).toHaveBeenCalledTimes(1);
    });
  });
}
