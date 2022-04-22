import React from "react";
import InvocationModel from "./invocation_model";
import { X, ArrowUp, ArrowDown, ArrowLeftRight, ChevronRight, Check } from "lucide-react";
import { cache } from "../../proto/cache_ts_proto";
import rpc_service from "../service/rpc_service";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";
import { durationToMillis, timestampToDate } from "../util/proto";
import error_service from "../errors/error_service";
import Button from "../components/button/button";
import Spinner from "../components/spinner/spinner";
import { formatTimestampMillis, durationMillis } from "../format/format";

export interface CacheRequestsCardProps {
  model: InvocationModel;
}

interface State {
  loading: boolean;
  results: cache.ScoreCard.IResult[];
  nextPageToken: string;
}

/**
 * CacheRequestsCardComponent shows all BuildBuddy cache requests for an invocation in a tabular form.
 */
export default class CacheRequestsCardComponent extends React.Component<CacheRequestsCardProps, State> {
  state: State = {
    loading: true,
    results: [],
    nextPageToken: "",
  };

  componentDidMount() {
    if (this.props.model.isComplete()) {
      this.fetchResults();
    }
  }

  componentDidUpdate(prevProps: Readonly<CacheRequestsCardProps>) {
    if (!prevProps.model.isComplete() && this.props.model.isComplete()) {
      this.fetchResults();
    }
  }

  private fetchResults() {
    this.setState({ loading: true });
    rpc_service.service
      .getCacheScoreCard({
        invocationId: this.props.model.getId(),
        pageToken: this.state.nextPageToken,
      })
      .then((response) => {
        this.setState({
          results: [...this.state.results, ...response.results],
          nextPageToken: response.nextPageToken,
        });
      })
      .catch((e: any) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private getActionUrl(digestHash: string) {
    return `/invocation/${this.props.model.getId()}?actionDigest=${digestHash}#action`;
  }

  private renderWaterfallBar(
    result: cache.ScoreCard.IResult,
    timelineStartTimeMillis: number,
    timelineDurationMillis: number
  ) {
    const resultStartTimeMillis = timestampToDate(result.startTime).getTime();
    const resultDurationMillis = durationToMillis(result.duration);

    const startWeight = (resultStartTimeMillis - timelineStartTimeMillis) / timelineDurationMillis;
    const eventWeight = resultDurationMillis / timelineDurationMillis;
    const afterWeight = 1 - startWeight - eventWeight;

    // TODO(bduffany): Show a nicer hovercard for the whole row
    return (
      <div
        className="waterfall-column"
        title={`${formatTimestampMillis(resultStartTimeMillis)} for ${durationMillis(resultDurationMillis)}`}>
        <div className="waterfall-gridlines">
          <div />
          <div />
          <div />
          <div />
        </div>
        <div style={{ flexGrow: startWeight }} />
        <div style={{ flexGrow: eventWeight }} className="waterfall-bar" />
        <div style={{ flexGrow: afterWeight }} />
      </div>
    );
  }

  /**
   * Returns the start timestamp and duration for the waterfall chart.
   */
  private getStartTimestampAndDurationMillis(): [number, number] {
    const earliestStartTimeMillis = this.state.results
      .map((result) => timestampToDate(result.startTime).getTime())
      .reduce((acc, cur) => Math.min(acc, cur), Number.MAX_SAFE_INTEGER);
    const invocationStartTimeMillis = this.props.model.getStartTimeDate().getTime();
    const startTimeMillis = Math.min(earliestStartTimeMillis, invocationStartTimeMillis);

    const latestEndTimeMillis = this.state.results
      .map((result) => timestampToDate(result.startTime).getTime() + durationToMillis(result.duration))
      .reduce((acc, cur) => Math.max(acc, cur), earliestStartTimeMillis);
    const invocationEndTimeMillis = this.props.model.getEndTimeDate().getTime();
    const endTimeMillis = Math.max(latestEndTimeMillis, invocationEndTimeMillis);

    return [startTimeMillis, endTimeMillis - startTimeMillis];
  }

  private onClickLoadMore() {
    this.fetchResults();
  }

  render() {
    if (this.state.loading && !this.state.results.length) {
      return (
        <RequestsCardContainer>
          <div className="loading" />
        </RequestsCardContainer>
      );
    }

    if (!this.state.results.length) {
      return <RequestsCardContainer>No cache requests found.</RequestsCardContainer>;
    }

    const groups = groupResultsByActionId(this.state.results);
    const [startTimeMillis, durationMillis] = this.getStartTimestampAndDurationMillis();

    return (
      <RequestsCardContainer>
        {groups.map((group) => (
          <div className="group">
            <div className="group-title action-id row">
              {looksLikeDigest(group.actionId) ? (
                <Link className="action-id" href={this.getActionUrl(group.results[0]?.actionId)}>
                  <DigestComponent
                    hashWidth="168px"
                    digest={{ hash: group.actionId, sizeBytes: null }}
                    expandOnHover={false}
                  />
                </Link>
              ) : (
                <span className="action-id">{group.actionId}</span>
              )}
              <div className="row action-label">
                <div>{group.results[0]?.targetId}</div>
                {group.results[0]?.actionMnemonic && (
                  <>
                    <ChevronRight className="icon chevron" />
                    <div className="action-mnemonic">{group.results[0]?.actionMnemonic}</div>
                  </>
                )}
              </div>
            </div>
            <div className="group-contents results-list column">
              {group.results.map((result) => (
                <div className="row">
                  <div>
                    <DigestComponent hashWidth="96px" sizeWidth="72px" digest={result.digest} expandOnHover={false} />
                  </div>
                  <div className="cache-type-column" title={cacheTypeTitle(result.cacheType)}>
                    {renderCacheType(result.cacheType)}
                  </div>
                  <div className="status-column column-with-icon">{renderStatus(result)}</div>
                  {this.renderWaterfallBar(result, startTimeMillis, durationMillis)}
                </div>
              ))}
            </div>
          </div>
        ))}
        <div className="table-footer-controls">
          {this.state.nextPageToken && (
            <Button
              className="load-more-button"
              onClick={this.onClickLoadMore.bind(this)}
              disabled={this.state.loading}>
              <span>Load more</span>
              {this.state.loading && <Spinner className="white" />}
            </Button>
          )}
        </div>
      </RequestsCardContainer>
    );
  }
}

const RequestsCardContainer: React.FC = ({ children }) => (
  <div className="card cache-requests-card">
    <div className="content">
      <div className="title">
        <ArrowLeftRight className="icon" />
        <span>Cache requests</span>
      </div>
      <div className="details">{children}</div>
    </div>
  </div>
);

function renderCacheType(cacheType: cache.CacheType): React.ReactNode {
  switch (cacheType) {
    case cache.CacheType.CAS:
      return "CAS";
    case cache.CacheType.AC:
      return "AC";
    default:
      return "";
  }
}

function cacheTypeTitle(cacheType: cache.CacheType): string | undefined {
  switch (cacheType) {
    case cache.CacheType.CAS:
      return "Content addressable storage";
    case cache.CacheType.AC:
      return "Action cache";
    default:
      return undefined;
  }
}

function renderStatus(result: cache.ScoreCard.IResult): React.ReactNode {
  if (result.requestType === cache.RequestType.READ && result.cacheType === cache.CacheType.AC) {
    if (result.status.code !== 0 /*=OK*/) {
      return (
        <>
          <X className="icon red" />
          <span>Miss</span>
          {/* TODO: Show "Error" if status code is something other than NotFound */}
        </>
      );
    }

    return (
      <>
        <Check className="icon green" />
        <span>Hit</span>
      </>
    );
  }

  if (result.requestType === cache.RequestType.READ) {
    return (
      <>
        <ArrowDown className="icon green" />
        <span>Read</span>
      </>
    );
  }

  if (result.requestType === cache.RequestType.WRITE) {
    return (
      <>
        <ArrowUp className="icon red" />
        <span>Write</span>
      </>
    );
  }

  return "";
}

type ActionResults = {
  actionId: string;
  results: cache.ScoreCard.IResult[];
};

function groupResultsByActionId(results: cache.ScoreCard.IResult[]): ActionResults[] {
  return groupByKey(results, "actionId").map((group) => ({
    actionId: group.key,
    results: group.values,
  }));
}

type Group<T, U> = {
  key: U;
  values: T[];
};

function groupByKey<K extends string, T extends Pick<T, K>>(list: T[], key: K): Group<T, T[K]>[] {
  const groupsByKeyValue: Record<T[K], T[]> = {};
  for (const item of list) {
    const keyValue = item[key];
    let values = groupsByKeyValue[keyValue];
    if (!values) {
      values = [];
      groupsByKeyValue[keyValue] = values;
    }
    values.push(item);
  }

  const out: Group<T, T[K]>[] = [];
  for (const [key, values] of Object.entries(groupsByKeyValue) as [T[K], T[]][]) {
    out.push({ key, values });
  }

  return out;
}

/**
 * Bazel includes some action IDs like "bes-upload" so we use this logic to try
 * and tell those apart from digests.
 */
function looksLikeDigest(actionId: string) {
  return actionId.length === 64;
}
