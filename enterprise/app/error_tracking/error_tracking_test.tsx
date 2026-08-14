import { error_tracking } from "../../../proto/error_tracking_ts_proto";
import rpcService from "../../../app/service/rpc_service";
import Long from "long";
import ErrorTrackingComponent, {
  displayCount,
  frequencyBarHeights,
  frequencyDescription,
  errorGroupSortDescription,
  errorGroupSortParam,
  errorGroupSortProto,
  errorOriginLabel,
  errorOriginScopeParam,
  errorOriginScopeProto,
  fingerprintExplanation,
  fingerprintLabel,
  fingerprintSourceLabel,
  fingerprintTechnicalDetails,
  groupRowTarget,
  groupOccurrencesByInvocation,
  isTargetRedundant,
  issueTitle,
  paginationTimeWindow,
} from "./error_tracking";

describe("displayCount", () => {
  it("renders protobuf int64 values as text instead of React objects", () => {
    expect(displayCount(Long.fromNumber(42))).toBe("42");
  });
});

describe("error origin scope", () => {
  it("defaults to actual Bazel failures", () => {
    const search = new URLSearchParams();

    expect(errorOriginScopeParam(search)).toBe("bazel");
    expect(errorOriginScopeProto(search)).toBe(error_tracking.ErrorOrigin.ERROR_ORIGIN_BAZEL);
  });

  it("maps the URL-backed Workflow scope and labels Workflow Bazel children", () => {
    const search = new URLSearchParams("kind=workflow");

    expect(errorOriginScopeParam(search)).toBe("workflow");
    expect(errorOriginScopeProto(search)).toBe(error_tracking.ErrorOrigin.ERROR_ORIGIN_WORKFLOW);
    expect(errorOriginLabel(error_tracking.ErrorOrigin.ERROR_ORIGIN_WORKFLOW_BAZEL_CHILD)).toBe(
      "Bazel · Workflow child"
    );
  });

  it("falls back to Bazel for an unknown scope", () => {
    expect(errorOriginScopeParam(new URLSearchParams("kind=unexpected"))).toBe("bazel");
  });
});

describe("error group sorting", () => {
  it("defaults to affected builds", () => {
    const search = new URLSearchParams();

    expect(errorGroupSortParam(search)).toBe("affected");
    expect(errorGroupSortProto(search)).toBe(error_tracking.ErrorGroupSort.ERROR_GROUP_SORT_AFFECTED_BUILDS);
    expect(errorGroupSortDescription("affected")).toContain("affected builds");
  });

  it("maps explicit URL choices to the RPC sort", () => {
    expect(errorGroupSortProto(new URLSearchParams("sort=recent"))).toBe(
      error_tracking.ErrorGroupSort.ERROR_GROUP_SORT_LAST_SEEN
    );
    expect(errorGroupSortProto(new URLSearchParams("sort=frequency"))).toBe(
      error_tracking.ErrorGroupSort.ERROR_GROUP_SORT_RECENT_FREQUENCY
    );
  });

  it("falls back to affected builds for an unknown URL value", () => {
    expect(errorGroupSortParam(new URLSearchParams("sort=unexpected"))).toBe("affected");
  });
});

describe("frequency histogram", () => {
  const buckets = [0, 1, 3, 6].map(
    (count, index) =>
      new error_tracking.ErrorFrequencyBucket({
        startTimeUsec: Long.fromNumber(index * 100),
        endTimeUsec: Long.fromNumber(index * 100 + 99),
        affectedInvocationCount: Long.fromNumber(count),
      })
  );

  it("normalizes each issue history while retaining zero buckets", () => {
    expect(frequencyBarHeights(buckets)).toEqual([0, 17, 50, 100]);
  });

  it("uses a shared scale so bar heights are comparable across issues", () => {
    expect(frequencyBarHeights(buckets, 12)).toEqual([0, 8, 25, 50]);
  });

  it("describes exact counts from oldest to newest", () => {
    expect(frequencyDescription(buckets)).toBe(
      "Frequency, oldest to newest: 0, 1, 3, 6 affected builds. 6 in the latest bucket."
    );
    expect(frequencyDescription(buckets, "workflow runs")).toContain("affected workflow runs");
  });
});

describe("groupOccurrencesByInvocation", () => {
  it("groups contexts by invocation while retaining attempts", () => {
    const occurrences = [
      new error_tracking.ErrorOccurrence({
        invocationId: "invocation-a",
        eventTimeUsec: Long.fromNumber(100),
        testName: "fails",
        testAttempt: 1,
        testCachedLocally: true,
      }),
      new error_tracking.ErrorOccurrence({
        invocationId: "invocation-b",
        eventTimeUsec: Long.fromNumber(300),
        testName: "other failure",
      }),
      new error_tracking.ErrorOccurrence({
        invocationId: "invocation-a",
        eventTimeUsec: Long.fromNumber(200),
        testName: "fails",
        testAttempt: 2,
      }),
    ];

    const groups = groupOccurrencesByInvocation(occurrences);

    expect(groups.map((group) => group.invocationId)).toEqual(["invocation-b", "invocation-a"]);
    expect(groups[1].latestEventTimeUsec.toString()).toBe("200");
    expect(groups[1].occurrences.map((occurrence) => occurrence.testAttempt)).toEqual([1, 2]);
  });

  it("keeps distinct contexts from the same invocation", () => {
    const occurrences = [
      new error_tracking.ErrorOccurrence({ invocationId: "invocation-a", targetLabel: "//a:test" }),
      new error_tracking.ErrorOccurrence({ invocationId: "invocation-a", targetLabel: "//b:test" }),
    ];

    const groups = groupOccurrencesByInvocation(occurrences);

    expect(groups).toHaveSize(1);
    expect(groups[0].occurrences.map((occurrence) => occurrence.targetLabel)).toEqual(["//a:test", "//b:test"]);
  });
});

describe("paginationTimeWindow", () => {
  it("keeps the list window stable while loading another page", () => {
    const first = paginationTimeWindow(undefined, false, 1_000_000);
    const next = paginationTimeWindow(first, true, 2_000_000);

    expect(next).toBe(first);
    expect(next.endTimeUsec.toString()).toBe("1000000000");
  });

  it("keeps the detail window stable while loading more invocation contexts", () => {
    const detail = paginationTimeWindow(undefined, false, 3_000_000);
    const moreContexts = paginationTimeWindow(detail, true, 4_000_000);

    expect(moreContexts).toEqual(detail);
    expect(paginationTimeWindow(detail, false, 4_000_000).endTimeUsec.toString()).toBe("4000000000");
  });
});

describe("pagination query state", () => {
  it("keeps the URL-backed applied query when the input draft changes", () => {
    const component = new ErrorTrackingComponent({ search: new URLSearchParams("q=applied") });
    component.state.query = "draft";

    expect((component as any).appliedQuery()).toBe("applied");
  });
});

describe("fingerprintLabel", () => {
  it("does not call every low-confidence action diagnostic a log fallback", () => {
    expect(fingerprintLabel("action_fallback:v1", "action_event")).toBe("BES diagnostic");
    expect(fingerprintLabel("test_fallback:v2", "test_result_fallback")).toBe("Conservative fallback");
    expect(fingerprintSourceLabel("test_result_fallback")).toBe("test.log");
  });
});

describe("issueTitle", () => {
  it("prioritizes an actionable structured test identity over generic assertion text", () => {
    expect(
      issueTitle(
        new error_tracking.ErrorGroup({
          errorType: "test/failure",
          sampleMessage: "Assertion failed at test.ts:42",
          sampleTestSuite: "checkout",
          sampleTestClass: "CartTest",
          sampleTestName: "removes expired items",
        })
      )
    ).toBe("checkout › CartTest › removes expired items");
  });

  it("uses the first non-empty diagnostic line for build failures", () => {
    expect(
      issueTitle(
        new error_tracking.ErrorGroup({ errorType: "action/non_zero_exit", sampleMessage: "\ncompiler failed" })
      )
    ).toBe("compiler failed");
  });

  it("uses the Workflow action as the title for orchestration failures", () => {
    expect(
      issueTitle(
        new error_tracking.ErrorGroup({
          origin: error_tracking.ErrorOrigin.ERROR_ORIGIN_WORKFLOW,
          sampleInvocationPattern: "Check style",
          sampleMessage: "target steps[0] failed to build",
        })
      )
    ).toBe("Check style");
  });
});

describe("fingerprintExplanation", () => {
  it("explains which volatile structured test fields do not split an issue", () => {
    const explanation = fingerprintExplanation("test:v2", "test_xml");

    expect(explanation).toContain("source line numbers do not split the issue");
    expect(explanation).toContain("Run, shard, attempt");
  });

  it("describes fallback grouping as target-scoped", () => {
    expect(fingerprintExplanation("test_fallback:v2", "test_result_fallback")).toContain(
      "scoped to the Bazel test target"
    );
  });

  it("shows the exact structured-test basis and exclusions", () => {
    const details = fingerprintTechnicalDetails("test:v2", "test_xml");

    expect(details.formula).toContain("target, suite, class, test");
    expect(details.formula).toContain("stable app frame");
    expect(details.normalizedOrExcluded.join(" ")).toContain("Run, shard, attempt");
  });

  it("includes the target only in the generic action fallback basis", () => {
    expect(fingerprintTechnicalDetails("action_fallback:v1", "action_output_fallback").formula).toContain(
      "mnemonic, target"
    );
    expect(fingerprintTechnicalDetails("compiler:v1", "action_output").formula).not.toContain("target");
  });

  it("explains Workflow fingerprints as action-scoped wrappers", () => {
    const details = fingerprintTechnicalDetails("workflow:v1", "workflow_bes");

    expect(details.formula).toContain("workflow action");
    expect(fingerprintExplanation("workflow:v1", "workflow_bes")).toContain("different workflow actions separate");
  });
});

describe("target de-duplication", () => {
  it("recognizes target-derived test identities", () => {
    expect(isTargetRedundant("//.codex/demo:shell_failure_test", ".codex/demo/shell_failure_test › fails")).toBeTrue();
  });

  it("does not hide a generic target basename found inside an unrelated identity", () => {
    expect(isTargetRedundant("//foo:test", "Checkout › CartTest")).toBeFalse();
  });

  it("omits an already represented target from the issue row", () => {
    const group = new error_tracking.ErrorGroup({
      sampleTargetLabel: "//.codex/demo:shell_failure_test",
      sampleTestSuite: ".codex/demo/shell_failure_test",
      sampleTestName: "fails",
    });

    expect(groupRowTarget(group)).toBe("");
  });

  it("retains a distinct target for an action diagnostic", () => {
    const group = new error_tracking.ErrorGroup({
      sampleTargetLabel: "//app:bundle",
      sampleMessage: "TypeScript compilation failed",
    });

    expect(groupRowTarget(group)).toBe("//app:bundle");
  });
});

describe("ErrorTrackingComponent refresh", () => {
  it("refetches the applied URL-backed view and unsubscribes on unmount", () => {
    const component = new ErrorTrackingComponent({ search: new URLSearchParams("q=applied"), enabled: true });
    const fetchGroups = spyOn<any>(component, "fetchGroups");

    component.componentDidMount();
    expect(fetchGroups).toHaveBeenCalledTimes(1);
    rpcService.events.next("refresh");
    expect(fetchGroups).toHaveBeenCalledWith("", "applied");
    expect(fetchGroups).toHaveBeenCalledTimes(2);

    component.componentWillUnmount();
    rpcService.events.next("refresh");
    expect(fetchGroups).toHaveBeenCalledTimes(2);
  });

  it("does not fetch when the route exists but the feature is disabled", () => {
    const component = new ErrorTrackingComponent({ search: new URLSearchParams(), enabled: false });
    const fetchGroups = spyOn<any>(component, "fetchGroups");

    component.componentDidMount();
    expect(fetchGroups).not.toHaveBeenCalled();
    expect(component.state.loading).toBeFalse();
    component.componentWillUnmount();
  });
});
