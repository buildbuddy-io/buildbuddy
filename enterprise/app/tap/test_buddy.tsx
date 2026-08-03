import { ChevronDown, Filter, Search, X } from "lucide-react";
import React from "react";
import { FilledButton, OutlinedButton } from "../../../app/components/button/button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Popup, { PopupContainer } from "../../../app/components/popup/popup";
import Select from "../../../app/components/select/select";
import errorService from "../../../app/errors/error_service";
import format from "../../../app/format/format";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import type { Cancelable } from "../../../app/service/rpc_service";
import { test_buddy } from "../../../proto/test_buddy_ts_proto";

interface Props {
  repo: string;
  targetLabel: string;
}

interface State {
  packagePrefix: string;
  searchOpen: boolean;
  healthFilters: test_buddy.TestHealth[];
  healthFilterOpen: boolean;
  repositories: test_buddy.TestRepository[];
  repository?: test_buddy.GetRepositoryHealthResponse;
  targets: test_buddy.TestTargetSummary[];
  selected?: test_buddy.GetTestTargetResponse;
  selectedCases: test_buddy.TestCaseSummary[];
  dispositionMenu?: string;
  updatingDisposition?: string;
  loading: boolean;
  loadingCases: boolean;
}

export default class TestBuddyComponent extends React.Component<Props, State> {
  private targetsStream?: Cancelable;
  private casesStream?: Cancelable;

  state: State = {
    packagePrefix: "",
    searchOpen: false,
    healthFilters: [],
    healthFilterOpen: false,
    repositories: [],
    targets: [],
    selectedCases: [],
    loading: true,
    loadingCases: false,
  };

  componentDidMount() {
    this.loadRepositories();
    if (this.props.repo) this.loadSelectedRepository();
  }

  componentDidUpdate(previous: Props) {
    if (previous.repo !== this.props.repo) {
      if (this.props.repo) this.loadSelectedRepository();
      return;
    }
    if (previous.targetLabel === this.props.targetLabel) return;
    if (this.props.targetLabel) {
      this.loadTarget(this.props.targetLabel);
    } else {
      this.casesStream?.cancel();
      this.setState({ selected: undefined, selectedCases: [], loadingCases: false });
    }
  }

  private loadRepositories() {
    rpcService.testBuddyService
      .getTestRepositories(test_buddy.GetTestRepositoriesRequest.create())
      .then((response) => {
        this.setState({ repositories: response.repositories });
        if (!this.props.repo && response.repositories[0]?.repoUrl) {
          this.selectRepository(response.repositories[0].repoUrl);
        } else if (!this.props.repo) {
          this.setState({ loading: false });
        }
      })
      .catch((error) => errorService.handleError(error));
  }

  private loadSelectedRepository() {
    this.targetsStream?.cancel();
    this.casesStream?.cancel();
    this.setState({
      repository: undefined,
      packagePrefix: "",
      searchOpen: false,
      targets: [],
      selected: undefined,
      selectedCases: [],
      loading: true,
      loadingCases: false,
    });
    this.loadRepository();
    if (this.props.targetLabel) this.loadTarget(this.props.targetLabel);
  }

  private selectRepository(repository: string) {
    router.navigateTo(`?repo=${encodeURIComponent(repository)}`);
  }

  componentWillUnmount() {
    this.targetsStream?.cancel();
    this.casesStream?.cancel();
  }

  private loadRepository() {
    const repositoryURL = this.props.repo;
    rpcService.testBuddyService
      .getRepositoryHealth(test_buddy.GetRepositoryHealthRequest.create({ repoUrl: repositoryURL }))
      .then((repository) => {
        if (this.props.repo !== repositoryURL) return;
        const searchOnly = Number(repository.targets?.totalCount ?? 0) > repositoryTargetListLimit;
        this.setState({ repository, searchOpen: searchOnly }, () => {
          if (this.props.targetLabel) return;
          if (searchOnly) {
            this.setState({ loading: false });
          } else {
            this.loadTargets();
          }
        });
      })
      .catch((error) => {
        this.setState({ loading: false });
        errorService.handleError(error);
      });
  }

  private loadTargets() {
    if (
      Number(this.state.repository?.targets?.totalCount ?? 0) > repositoryTargetListLimit &&
      !this.state.packagePrefix.trim()
    ) {
      this.setState({ loading: false, targets: [] });
      return;
    }
    this.targetsStream?.cancel();
    this.casesStream?.cancel();
    this.setState({ loading: true, targets: [], selected: undefined, selectedCases: [], loadingCases: false });
    this.targetsStream = rpcService.testBuddyService.getTestTargets(
      test_buddy.GetTestTargetsRequest.create({
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

  private loadCases(target: test_buddy.TestTargetIdentity) {
    this.casesStream?.cancel();
    this.setState({ selectedCases: [], loadingCases: true });
    this.casesStream = rpcService.testBuddyService.getTests(
      test_buddy.GetTestsRequest.create({
        repoUrl: this.props.repo,
        target,
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

  private selectTarget(target: test_buddy.TestTargetSummary) {
    if (!target.identity?.targetLabel) return;
    router.navigateToQueryParam("target", target.identity.targetLabel);
  }

  private renderHealthFilter(label = "Health") {
    const options = [
      test_buddy.TestHealth.TEST_HEALTH_FAILING,
      test_buddy.TestHealth.TEST_HEALTH_FLAKY,
      test_buddy.TestHealth.TEST_HEALTH_TIMEOUT,
      test_buddy.TestHealth.TEST_HEALTH_HEALTHY,
      test_buddy.TestHealth.TEST_HEALTH_INSUFFICIENT_DATA,
      test_buddy.TestHealth.TEST_HEALTH_UNKNOWN,
    ];
    const filtering = this.state.healthFilters.length > 0;
    return (
      <div className={`global-filter test-buddy-health-filter ${filtering ? "is-filtering" : ""}`}>
        {filtering && (
          <FilledButton
            className="square"
            title="Clear health filters"
            type="button"
            onClick={() => this.setState({ healthFilters: [] })}>
            <X className="white" />
          </FilledButton>
        )}
        <div className="popup-wrapper">
          <OutlinedButton
            className={`filter-menu-button icon-text-button ${filtering ? "" : "square"}`}
            title={label}
            type="button"
            onClick={() => this.setState({ healthFilterOpen: true })}>
            <Filter />
            {this.state.healthFilters.map((health) => (
              <HealthLabel health={health} key={health} />
            ))}
          </OutlinedButton>
          <Popup
            anchor="center-right"
            isOpen={this.state.healthFilterOpen}
            onRequestClose={() => this.setState({ healthFilterOpen: false })}
            className="filter-menu-popup">
            <div className="option-group">
              <div className="option-group-title">{label}</div>
              <div className="option-group-options">
                {options.map((health) => (
                  <label key={health}>
                    <Checkbox
                      checked={this.state.healthFilters.includes(health)}
                      onChange={() => this.toggleHealthFilter(health)}
                    />
                    <HealthLabel health={health} />
                  </label>
                ))}
              </div>
            </div>
          </Popup>
        </div>
      </div>
    );
  }

  private toggleHealthFilter(health: test_buddy.TestHealth) {
    this.setState((state) => ({
      healthFilters: state.healthFilters.includes(health)
        ? state.healthFilters.filter((value) => value !== health)
        : [...state.healthFilters, health],
    }));
  }

  private matchesHealth(summary?: test_buddy.TestSummary | null) {
    return (
      this.state.healthFilters.length === 0 ||
      this.state.healthFilters.includes(summary?.health ?? test_buddy.TestHealth.TEST_HEALTH_UNKNOWN)
    );
  }

  private renderDispositionControl(
    key: string,
    disposition: test_buddy.TestExecutionDisposition,
    setDisposition: (disposition: test_buddy.TestExecutionDisposition) => void
  ) {
    const saving = this.state.updatingDisposition === key;
    return (
      <PopupContainer className="test-buddy-disposition">
        <OutlinedButton
          className="icon-text-button"
          disabled={saving}
          type="button"
          onClick={() => this.setState({ dispositionMenu: key })}>
          {saving ? "Saving…" : dispositionName(disposition)}
          <ChevronDown />
        </OutlinedButton>
        <Popup
          isOpen={this.state.dispositionMenu === key}
          onRequestClose={() => this.setState({ dispositionMenu: undefined })}>
          <Menu>
            {[
              test_buddy.TestExecutionDisposition.TEST_EXECUTION_DISPOSITION_AUTOMATIC,
              test_buddy.TestExecutionDisposition.TEST_EXECUTION_DISPOSITION_ENABLED,
              test_buddy.TestExecutionDisposition.TEST_EXECUTION_DISPOSITION_DISABLED,
            ].map((value) => (
              <MenuItem
                disabled={value === disposition}
                key={value}
                onClick={() => setDisposition(value)}>
                {dispositionName(value)}
              </MenuItem>
            ))}
          </Menu>
        </Popup>
      </PopupContainer>
    );
  }

  private setTargetDisposition(disposition: test_buddy.TestExecutionDisposition) {
    const identity = this.state.selected?.target?.identity;
    if (!identity?.targetLabel) return;
    const key = "target";
    this.setState({ dispositionMenu: undefined, updatingDisposition: key });
    rpcService.testBuddyService
      .setTestExecutionDisposition(
        test_buddy.SetTestExecutionDispositionRequest.create({
          repoUrl: this.props.repo,
          target: identity,
          disposition,
        })
      )
      .then((response) =>
        this.setState((state) => ({
          selected:
            state.selected?.target?.identity?.targetLabel === identity.targetLabel
              ? test_buddy.GetTestTargetResponse.create({
                  ...state.selected,
                  target: test_buddy.TestTargetSummary.create({
                    ...state.selected.target,
                    disposition: response.disposition,
                  }),
                })
              : state.selected,
          updatingDisposition: state.updatingDisposition === key ? undefined : state.updatingDisposition,
        }))
      )
      .catch((error) => {
        this.setState({ updatingDisposition: undefined });
        errorService.handleError(error);
      });
  }

  private setCaseDisposition(testCase: test_buddy.TestCaseSummary, disposition: test_buddy.TestExecutionDisposition) {
    const identity = testCase.identity;
    if (!identity?.target?.targetLabel || !identity.caseName) return;
    const key = `case:${identity.caseName}`;
    this.setState({ dispositionMenu: undefined, updatingDisposition: key });
    rpcService.testBuddyService
      .setTestExecutionDisposition(
        test_buddy.SetTestExecutionDispositionRequest.create({
          repoUrl: this.props.repo,
          testCase: identity,
          disposition,
        })
      )
      .then((response) =>
        this.setState((state) => ({
          selectedCases: state.selectedCases.map((candidate) =>
            candidate.identity?.target?.targetLabel === identity.target?.targetLabel &&
            candidate.identity?.caseName === identity.caseName
              ? test_buddy.TestCaseSummary.create({ ...candidate, disposition: response.disposition })
              : candidate
          ),
          updatingDisposition: state.updatingDisposition === key ? undefined : state.updatingDisposition,
        }))
      )
      .catch((error) => {
        this.setState({ updatingDisposition: undefined });
        errorService.handleError(error);
      });
  }

  // The component navigates by target label, so the identity message is built
  // here rather than at each call site.
  private loadTarget(targetLabel: string) {
    this.casesStream?.cancel();
    const identity = test_buddy.TestTargetIdentity.create({ targetLabel });
    rpcService.testBuddyService
      .getTestTarget(test_buddy.GetTestTargetRequest.create({ repoUrl: this.props.repo, identity }))
      .then((selected) => {
        if (this.props.targetLabel !== targetLabel) return;
        this.setState({ selected });
        this.loadCases(identity);
      })
      .catch((error) => {
        if (this.props.targetLabel !== targetLabel) return;
        this.setState({ loading: false });
        errorService.handleError(error);
      });
  }

  private renderSummary(name: string, summary?: test_buddy.TestHealthSummary | null) {
    if (!summary) return null;
    return (
      <div className="test-buddy-summary">
        <h3>{name}</h3>
        <div className="test-buddy-stats">
          <Stat name="Total" value={summary.totalCount.toString()} />
          <Stat name="Failing" value={summary.failingCount.toString()} />
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
    const summary = target.summary;
    const cases = this.state.selectedCases.filter((testCase) => this.matchesHealth(testCase.summary));
    return (
      <section className="test-buddy-target-detail">
        <h2>{target.identity?.targetLabel}</h2>
        <div className="test-buddy-stats">
          <Stat name="Target health" value={<HealthLabel health={summary?.health} />} />
          <Stat name="Pass rate" value={percent(summary?.passRate ?? 0)} />
          <Stat name="Mean duration" value={format.durationUsec(summary?.meanDurationUsec ?? 0)} />
          <Stat name="Passes" value={(summary?.passCount ?? 0).toString()} />
          <Stat name="Failures" value={(summary?.failCount ?? 0).toString()} />
          <Stat name="Timeouts" value={(summary?.timeoutCount ?? 0).toString()} />
          <div className="test-buddy-stat">
            <div>Run policy</div>
            {this.renderDispositionControl("target", target.disposition, (disposition) =>
              this.setTargetDisposition(disposition)
            )}
          </div>
        </div>
        <h3>Cases</h3>
        {this.renderHealthFilter("Case health")}
        <table className="test-buddy-table">
          <thead>
            <tr>
              <th>Case</th>
              <th>Health</th>
              <th>Pass rate</th>
              <th>Mean duration</th>
              <th>Pass</th>
              <th>Fail</th>
              <th>Timeout</th>
              <th>Run policy</th>
            </tr>
          </thead>
          <tbody>
            {cases.map((testCase) => (
              <tr key={testCase.identity?.caseName}>
                <td>{testCase.identity?.caseName}</td>
                <td>
                  <HealthLabel health={testCase.summary?.health} />
                </td>
                <td>{percent(testCase.summary?.passRate ?? 0)}</td>
                <td>{format.durationUsec(testCase.summary?.meanDurationUsec ?? 0)}</td>
                <td>{(testCase.summary?.passCount ?? 0).toString()}</td>
                <td>{(testCase.summary?.failCount ?? 0).toString()}</td>
                <td>{(testCase.summary?.timeoutCount ?? 0).toString()}</td>
                <td>
                  {this.renderDispositionControl(`case:${testCase.identity?.caseName}`, testCase.disposition, (disposition) =>
                    this.setCaseDisposition(testCase, disposition)
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {this.state.loadingCases && <div className="test-buddy-empty">Loading cases…</div>}
        {!this.state.loadingCases && cases.length === 0 && (
          <div className="test-buddy-empty">No cases found.</div>
        )}
        <h3>Recent target results</h3>
        <table className="test-buddy-table">
          <thead>
            <tr>
              <th>Source</th>
              <th>Outcome</th>
              <th>Duration</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody>
            {response.recentResults.map((result, index) => (
              <tr key={`${result.sourceUrl}-${index}`}>
                <td>
                  <a href={result.sourceUrl}>View result</a>
                </td>
                <td>{outcomeName(result.outcome)}</td>
                <td>{format.durationUsec(result.durationUsec)}</td>
                <td>{result.failureMessage}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <h3>State changes</h3>
        <table className="test-buddy-table">
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
                <td>
                  <HealthLabel health={transition.previousHealth} />
                </td>
                <td>
                  <HealthLabel health={transition.health} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    );
  }

  render() {
    if (this.state.selected) return <div className="container test-buddy">{this.renderTarget()}</div>;
    const targets = this.state.targets
      .map((target) => ({ target, row: targetRow(target) }))
      .filter(({ row }) => this.state.healthFilters.length === 0 || this.state.healthFilters.includes(row.health))
      .sort(
        (a, b) =>
          healthRank(a.row.health) - healthRank(b.row.health) ||
          b.row.meanDurationUsec - a.row.meanDurationUsec ||
          a.row.passRate - b.row.passRate ||
          (a.target.identity?.targetLabel ?? "").localeCompare(b.target.identity?.targetLabel ?? "")
      );
    const searchOnly = Number(this.state.repository?.targets?.totalCount ?? 0) > repositoryTargetListLimit;
    return (
      <div className="container test-buddy">
        <div className="test-buddy-title-row">
          <div className="test-buddy-title-actions">
            {this.renderHealthFilter()}
            <OutlinedButton
              className="square"
              title="Search tests by package or directory"
              type="button"
              onClick={() => this.setState((state) => ({ searchOpen: !state.searchOpen }))}>
              <Search />
            </OutlinedButton>
          </div>
        </div>
        <label className="test-buddy-repo">
          Repository
          <Select value={this.props.repo} onChange={(event) => this.selectRepository(event.target.value)}>
            {!this.props.repo && <option value="">No reported repositories</option>}
            {this.state.repositories.map((repository) => (
              <option value={repository.repoUrl} key={repository.repoUrl}>
                {repository.repoUrl}
              </option>
            ))}
          </Select>
        </label>
        <div className="test-buddy-repository-summaries">
          {this.renderSummary("Targets", this.state.repository?.targets)}
          {this.renderSummary("Cases", this.state.repository?.cases)}
        </div>
        {this.state.searchOpen && (
          <form
            className="test-buddy-search"
            onSubmit={(event) => {
              event.preventDefault();
              this.loadTargets();
            }}>
            <input
              aria-label="Bazel package"
              placeholder="Package or directory, for example server/test_buddy"
              value={this.state.packagePrefix}
              onChange={(event) => this.setState({ packagePrefix: event.target.value })}
            />
            <button className="test-buddy-search-submit" type="submit">
              Search
            </button>
          </form>
        )}
        <table className="test-buddy-table">
          <thead>
            <tr>
              <th>Target</th>
              <th>Health</th>
              <th>Pass rate</th>
              <th>Mean duration</th>
            </tr>
          </thead>
          <tbody>
            {targets.map(({ target, row }) => {
              return (
                <tr key={target.identity?.targetLabel}>
                  <td>
                    <button className="test-buddy-target-link" onClick={() => this.selectTarget(target)}>
                      {target.identity?.targetLabel}
                    </button>
                  </td>
                  <td>
                    <HealthLabel health={row.health} />
                  </td>
                  <td>{percent(row.passRate)}</td>
                  <td>{format.durationUsec(row.meanDurationUsec)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {this.state.loading && <div className="test-buddy-empty">Loading targets…</div>}
        {!this.state.loading && searchOnly && !this.state.packagePrefix && targets.length === 0 && (
          <div className="test-buddy-empty">
            This repository has more than {repositoryTargetListLimit.toLocaleString()} test targets. Search by package
            or directory to load a bounded cone.
          </div>
        )}
        {!this.state.loading && targets.length === 0 && !(searchOnly && !this.state.packagePrefix) && (
          <div className="test-buddy-empty">No targets found.</div>
        )}
      </div>
    );
  }
}

const repositoryTargetListLimit = 100_000;

function Stat({ name, value }: { name: string; value: React.ReactNode }) {
  return (
    <div className="test-buddy-stat">
      <div>{name}</div>
      <strong>{value}</strong>
    </div>
  );
}

function healthName(health = test_buddy.TestHealth.TEST_HEALTH_UNKNOWN) {
  if (health === test_buddy.TestHealth.TEST_HEALTH_UNKNOWN) return "NOT REPORTED";
  return test_buddy.TestHealth[health].replace("TEST_HEALTH_", "");
}

function healthClass(health = test_buddy.TestHealth.TEST_HEALTH_UNKNOWN) {
  if (health === test_buddy.TestHealth.TEST_HEALTH_HEALTHY) return "healthy";
  if (health === test_buddy.TestHealth.TEST_HEALTH_FLAKY) return "flaky";
  if (health === test_buddy.TestHealth.TEST_HEALTH_FAILING) return "failing";
  if (health === test_buddy.TestHealth.TEST_HEALTH_TIMEOUT) return "timeout";
  return "neutral";
}

function HealthLabel({ health }: { health?: test_buddy.TestHealth }) {
  return <span className={`test-buddy-health ${healthClass(health)}`}>{healthName(health)}</span>;
}

function targetRow(target: test_buddy.TestTargetSummary) {
  const targetHealth = target.summary?.health ?? test_buddy.TestHealth.TEST_HEALTH_UNKNOWN;
  const cases = target.cases;
  const caseHealth = caseRollupHealth(cases);
  const health = healthRank(caseHealth) < healthRank(targetHealth) ? caseHealth : targetHealth;
  const caseSampleCount = Number(cases?.passCount ?? 0) + Number(cases?.failCount ?? 0) + Number(cases?.timeoutCount ?? 0);
  return {
    health,
    passRate: caseSampleCount > 0 ? cases?.passRate ?? 0 : target.summary?.passRate ?? 0,
    meanDurationUsec: Number(caseSampleCount > 0 ? cases?.meanDurationUsec ?? 0 : target.summary?.meanDurationUsec ?? 0),
  };
}

function caseRollupHealth(summary?: test_buddy.TestHealthSummary | null) {
  if (Number(summary?.failingCount) > 0) return test_buddy.TestHealth.TEST_HEALTH_FAILING;
  if (Number(summary?.flakyCount) > 0) return test_buddy.TestHealth.TEST_HEALTH_FLAKY;
  if (Number(summary?.timedOutCount) > 0) return test_buddy.TestHealth.TEST_HEALTH_TIMEOUT;
  if (Number(summary?.insufficientDataCount) > 0) return test_buddy.TestHealth.TEST_HEALTH_INSUFFICIENT_DATA;
  if (Number(summary?.healthyCount) > 0) return test_buddy.TestHealth.TEST_HEALTH_HEALTHY;
  return test_buddy.TestHealth.TEST_HEALTH_UNKNOWN;
}

function healthRank(health: test_buddy.TestHealth) {
  if (health === test_buddy.TestHealth.TEST_HEALTH_FAILING) return 0;
  if (health === test_buddy.TestHealth.TEST_HEALTH_FLAKY) return 1;
  if (health === test_buddy.TestHealth.TEST_HEALTH_TIMEOUT) return 2;
  if (health === test_buddy.TestHealth.TEST_HEALTH_INSUFFICIENT_DATA) return 3;
  if (health === test_buddy.TestHealth.TEST_HEALTH_HEALTHY) return 4;
  return 5;
}

function outcomeName(outcome: test_buddy.TestOutcome) {
  return test_buddy.TestOutcome[outcome].replace("TEST_OUTCOME_", "");
}

function dispositionName(disposition: test_buddy.TestExecutionDisposition) {
  if (disposition === test_buddy.TestExecutionDisposition.TEST_EXECUTION_DISPOSITION_ENABLED) return "Always run";
  if (disposition === test_buddy.TestExecutionDisposition.TEST_EXECUTION_DISPOSITION_DISABLED) return "Disabled";
  return "Automatic";
}

function percent(value: number) {
  return `${format.percent(value)}%`;
}
