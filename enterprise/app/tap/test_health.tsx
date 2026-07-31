import React from "react";
import errorService from "../../../app/errors/error_service";
import format from "../../../app/format/format";
import rpcService from "../../../app/service/rpc_service";
import type { Cancelable } from "../../../app/service/rpc_service";
import { test_health } from "../../../proto/test_health_ts_proto";

interface Props {
  repo: string;
}

interface State {
  packagePrefix: string;
  repository?: test_health.GetRepositoryHealthResponse;
  targets: test_health.TestTargetSummary[];
  selected?: test_health.GetTestTargetResponse;
  selectedCases: test_health.TestSummary[];
  loading: boolean;
  loadingCases: boolean;
}

export default class TestHealthComponent extends React.Component<Props, State> {
  private targetsStream?: Cancelable;
  private casesStream?: Cancelable;

  state: State = {
    packagePrefix: "",
    targets: [],
    selectedCases: [],
    loading: true,
    loadingCases: false,
  };

  componentDidMount() {
    this.loadRepository();
    this.loadTargets();
  }

  componentWillUnmount() {
    this.targetsStream?.cancel();
    this.casesStream?.cancel();
  }

  private loadRepository() {
    rpcService.testBuddyService
      .getRepositoryHealth(test_health.GetRepositoryHealthRequest.create({ repoUrl: this.props.repo }))
      .then((repository) => this.setState({ repository }))
      .catch((error) => errorService.handleError(error));
  }

  private loadTargets() {
    this.targetsStream?.cancel();
    this.casesStream?.cancel();
    this.setState({ loading: true, targets: [], selected: undefined, selectedCases: [], loadingCases: false });
    this.targetsStream = rpcService.testBuddyService.getTestTargets(
      test_health.GetTestTargetsRequest.create({
        repoUrl: this.props.repo,
        packagePrefix: this.state.packagePrefix,
      }),
      {
        next: (response) => this.setState((state) => ({ targets: [...state.targets, ...response.targets] })),
        error: (error) => {
          this.setState({ loading: false });
          errorService.handleError(error);
        },
        complete: () => this.setState({ loading: false }),
      }
    );
  }

  private loadCases(target: test_health.TestTargetIdentity) {
    this.casesStream?.cancel();
    this.setState({ selectedCases: [], loadingCases: true });
    this.casesStream = rpcService.testBuddyService.getTests(
      test_health.GetTestsRequest.create({
        repoUrl: this.props.repo,
        targetLabel: target.targetLabel,
      }),
      {
        next: (response) => this.setState((state) => ({ selectedCases: [...state.selectedCases, ...response.tests] })),
        error: (error) => {
          this.setState({ loadingCases: false });
          errorService.handleError(error);
        },
        complete: () => this.setState({ loadingCases: false }),
      }
    );
  }

  private selectTarget(target: test_health.TestTargetSummary) {
    const identity = target.identity;
    if (!identity) return;
    this.casesStream?.cancel();
    rpcService.testBuddyService
      .getTestTarget(test_health.GetTestTargetRequest.create({ identity }))
      .then((selected) => {
        this.setState({ selected });
        this.loadCases(identity);
      })
      .catch((error) => {
        this.setState({ loading: false });
        errorService.handleError(error);
      });
  }

  private clearTarget() {
    this.casesStream?.cancel();
    this.setState({ selected: undefined, selectedCases: [], loadingCases: false });
  }

  private renderSummary(name: string, summary?: test_health.TestHealthSummary) {
    if (!summary) return null;
    return (
      <div className="test-health-summary">
        <h3>{name}</h3>
        <div className="test-health-stats">
          <Stat name="Total" value={summary.totalCount.toString()} />
          <Stat name="Flaky" value={summary.flakyCount.toString()} />
          <Stat name="Timed out" value={summary.timedOutCount.toString()} />
          <Stat name="Healthy" value={summary.healthyCount.toString()} />
          <Stat name="Insufficient data" value={summary.insufficientDataCount.toString()} />
          <Stat name="Unknown" value={summary.unknownCount.toString()} />
          <Stat name="Pass rate" value={percent(summary.passRate)} />
          <Stat name="Mean duration" value={format.durationUsec(summary.meanDurationUsec)} />
        </div>
      </div>
    );
  }

  private renderTarget() {
    const response = this.state.selected;
    const target = response?.target;
    if (!response || !target) return null;
    return (
      <section className="test-health-target-detail">
        <button className="test-health-back" onClick={() => this.clearTarget()}>
          Back to targets
        </button>
        <h2>{target.identity?.targetLabel}</h2>
        <div className="test-health-stats">
          <Stat name="Health" value={healthName(target.health)} />
          <Stat name="Pass rate" value={percent(target.passRate)} />
          <Stat name="Mean duration" value={format.durationUsec(target.meanDurationUsec)} />
          <Stat name="Passes" value={target.passCount.toString()} />
          <Stat name="Failures" value={target.failCount.toString()} />
          <Stat name="Timeouts" value={target.timeoutCount.toString()} />
        </div>
        <h3>Cases</h3>
        <table className="test-health-table">
          <thead>
            <tr>
              <th>Case</th>
              <th>Health</th>
              <th>Pass rate</th>
              <th>Mean duration</th>
              <th>Pass</th>
              <th>Fail</th>
              <th>Timeout</th>
            </tr>
          </thead>
          <tbody>
            {this.state.selectedCases.map((testCase) => (
              <tr key={testCase.identity?.caseName}>
                <td>{testCase.identity?.caseName}</td>
                <td>{healthName(testCase.health)}</td>
                <td>{percent(testCase.passRate)}</td>
                <td>{format.durationUsec(testCase.meanDurationUsec)}</td>
                <td>{testCase.passCount.toString()}</td>
                <td>{testCase.failCount.toString()}</td>
                <td>{testCase.timeoutCount.toString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {this.state.loadingCases && <div className="test-health-empty">Loading cases…</div>}
        {!this.state.loadingCases && this.state.selectedCases.length === 0 && (
          <div className="test-health-empty">No cases found.</div>
        )}
        <h3>Recent target results</h3>
        <table className="test-health-table">
          <thead>
            <tr>
              <th>Invocation</th>
              <th>Outcome</th>
              <th>Duration</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody>
            {response.recentResults.map((result, index) => (
              <tr key={`${result.invocationId}-${index}`}>
                <td>
                  <a href={`/invocation/${result.invocationId}`}>{result.invocationId}</a>
                </td>
                <td>{outcomeName(result.outcome)}</td>
                <td>{format.durationUsec(result.durationUsec)}</td>
                <td>{result.failureMessage}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <h3>State changes</h3>
        <table className="test-health-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Previous</th>
              <th>Current</th>
            </tr>
          </thead>
          <tbody>
            {response.transitions.map((transition, index) => (
              <tr key={`${transition.eventTimeUsec}-${index}`}>
                <td>{new Date(Number(transition.eventTimeUsec) / 1_000).toLocaleString()}</td>
                <td>{healthName(transition.previousHealth)}</td>
                <td>{healthName(transition.health)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    );
  }

  render() {
    if (this.state.selected) return <div className="container test-health">{this.renderTarget()}</div>;
    return (
      <div className="container test-health">
        <h1>TestBuddy</h1>
        <div className="test-health-repo">{this.props.repo}</div>
        <div className="test-health-repository-summaries">
          {this.renderSummary("Targets", this.state.repository?.targets)}
          {this.renderSummary("Cases", this.state.repository?.cases)}
        </div>
        <form
          className="test-health-search"
          onSubmit={(event) => {
            event.preventDefault();
            this.loadTargets();
          }}>
          <input
            aria-label="Bazel package"
            placeholder="Package or directory, for example server/test_health"
            value={this.state.packagePrefix}
            onChange={(event) => this.setState({ packagePrefix: event.target.value })}
          />
          <button type="submit">Search</button>
        </form>
        <table className="test-health-table">
          <thead>
            <tr>
              <th>Target</th>
              <th>Health</th>
              <th>Pass rate</th>
              <th>Mean duration</th>
            </tr>
          </thead>
          <tbody>
            {this.state.targets.map((target) => (
              <tr key={target.identity?.targetLabel}>
                <td>
                  <button className="test-health-target-link" onClick={() => this.selectTarget(target)}>
                    {target.identity?.targetLabel}
                  </button>
                </td>
                <td>{healthName(target.health)}</td>
                <td>{percent(target.passRate)}</td>
                <td>{format.durationUsec(target.meanDurationUsec)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {this.state.loading && <div className="test-health-empty">Loading targets…</div>}
        {!this.state.loading && this.state.targets.length === 0 && (
          <div className="test-health-empty">No targets found.</div>
        )}
      </div>
    );
  }
}

function Stat({ name, value }: { name: string; value: string }) {
  return (
    <div className="test-health-stat">
      <div>{name}</div>
      <strong>{value}</strong>
    </div>
  );
}

function healthName(health: test_health.TestHealth) {
  if (health === test_health.TestHealth.TEST_HEALTH_UNKNOWN) return "NOT REPORTED";
  return test_health.TestHealth[health].replace("TEST_HEALTH_", "");
}

function outcomeName(outcome: test_health.TestOutcome) {
  return test_health.TestOutcome[outcome].replace("TEST_OUTCOME_", "");
}

function percent(value: number) {
  return `${format.percent(value)}%`;
}
