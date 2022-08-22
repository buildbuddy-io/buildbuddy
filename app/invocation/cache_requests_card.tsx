import React from "react";
import router from "../router/router";
import InvocationModel from "./invocation_model";
import { X, ArrowUp, ArrowDown, ArrowLeftRight, ChevronRight, Check, SortAsc, SortDesc } from "lucide-react";
import { cache } from "../../proto/cache_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import rpc_service from "../service/rpc_service";
import DigestComponent from "../components/digest/digest";
import Link, { TextLink } from "../components/link/link";
import { durationToMillis, timestampToDate } from "../util/proto";
import error_service from "../errors/error_service";
import Button, { OutlinedButton } from "../components/button/button";
import Spinner from "../components/spinner/spinner";
import Select, { Option } from "../components/select/select";
import { FilterInput } from "../components/filter_input/filter_input";
import * as format from "../format/format";
import * as proto from "../util/proto";
import { pinBottomMiddleToMouse, Tooltip } from "../components/tooltip/tooltip";
import { BuildBuddyError } from "../util/errors";

export interface CacheRequestsCardProps {
  model: InvocationModel;
  search: URLSearchParams;
}

interface State {
  searchText: string;

  loading: boolean;
  results: cache.ScoreCard.IResult[];
  nextPageToken: string;
  didInitialFetch: boolean;
}

const SEARCH_DEBOUNCE_INTERVAL_MS = 300;

/** How long to wait before retrying the initial fetch. */
const RETRY_FETCH_DELAY_MS = 1000;

/**
 * Represents a labeled collection of cache request filters.
 */
type PresetFilter = {
  label: string;
  values: {
    cache: cache.CacheType;
    request: cache.RequestType;
    response: cache.ResponseType;
  };
};

// Preset filters presented to the user, to avoid presenting too many query options as separate dropdowns.
const filters: PresetFilter[] = [
  {
    label: "All",
    values: { cache: 0, request: 0, response: 0 },
  },
  {
    label: "AC Hits",
    values: { cache: cache.CacheType.AC, request: cache.RequestType.READ, response: cache.ResponseType.OK },
  },
  {
    label: "AC Misses",
    values: { cache: cache.CacheType.AC, request: cache.RequestType.READ, response: cache.ResponseType.NOT_FOUND },
  },
  {
    label: "CAS Hits",
    values: { cache: cache.CacheType.CAS, request: cache.RequestType.READ, response: cache.ResponseType.OK },
  },
  {
    label: "CAS Writes",
    values: { cache: cache.CacheType.CAS, request: cache.RequestType.WRITE, response: cache.ResponseType.OK },
  },
  {
    label: "Errors",
    values: { cache: 0, request: 0, response: cache.ResponseType.ERROR },
  },
];

const defaultFilterIndex = 2; // AC Misses

/**
 * CacheRequestsCardComponent shows all BuildBuddy cache requests for an invocation in a tabular form.
 */
export default class CacheRequestsCardComponent extends React.Component<CacheRequestsCardProps, State> {
  state: State = {
    searchText: "",
    loading: false,
    results: [],
    nextPageToken: "",
    didInitialFetch: false,
  };

  constructor(props: CacheRequestsCardProps) {
    super(props);
    this.state.searchText = this.props.search.get("search") || "";
  }

  componentDidMount() {
    if (areResultsAvailable(this.props.model)) {
      this.fetchResults();
    }
  }

  componentDidUpdate(prevProps: Readonly<CacheRequestsCardProps>) {
    if (!areResultsAvailable(prevProps.model) && areResultsAvailable(this.props.model)) {
      this.fetchResults();
      return;
    }
    if (prevProps.search.toString() !== this.props.search.toString()) {
      // Re-fetch from the beginning when sorting/filtering parameters change
      this.fetchResults(/*pageToken=*/ "");
      return;
    }
  }

  private fetchResults(pageToken = this.state.nextPageToken) {
    this.setState({ loading: true });

    const filterFields: string[] = [];

    const filter = filters[this.getFilterIndex()].values;
    if (filter.cache) filterFields.push("cache_type");
    if (filter.request) filterFields.push("request_type");
    if (filter.response) filterFields.push("response_type");

    if (this.getSearch()) filterFields.push("search");

    const isInitialFetch = !this.state.didInitialFetch;

    rpc_service.service
      .getCacheScoreCard({
        invocationId: this.props.model.getId(),
        orderBy: this.getOrderBy(),
        descending: this.getDescending(),
        groupBy: this.getGroupBy(),
        filter: {
          mask: { paths: filterFields },
          cacheType: filter.cache,
          requestType: filter.request,
          responseType: filter.response,
          search: this.getSearch(),
        },
        pageToken,
      })
      .then((response) => {
        this.setState({
          results: [...(pageToken && this.state.results), ...response.results],
          nextPageToken: response.nextPageToken,
        });
      })
      .catch((e: any) => this.handleFetchError(e, isInitialFetch))
      .finally(() => this.setState({ loading: false, didInitialFetch: true }));
  }

  private handleFetchError(e: any, isInitialFetch: boolean) {
    const error = BuildBuddyError.parse(e);

    // Retry NotFound errors on initial page load up to once, since stats may
    // still be getting finalized. If that still fails, just log to the console
    // since this is non-critical.
    if (error.code === "NotFound") {
      console.warn(e);
      if (isInitialFetch) {
        setTimeout(() => {
          this.fetchResults();
        }, RETRY_FETCH_DELAY_MS);
      }
      return;
    }

    error_service.handleError(e);
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
    const beforeDurationMillis = resultStartTimeMillis - timelineStartTimeMillis;
    return (
      <div className="waterfall-column">
        <div className="waterfall-gridlines">
          <div />
          <div />
          <div />
          <div />
        </div>
        <div
          style={{
            marginLeft: `${(beforeDurationMillis / timelineDurationMillis) * 100}%`,
            width: `${(resultDurationMillis / timelineDurationMillis) * 100}%`,
          }}
          className="waterfall-bar"
        />
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

  private getOrderBy() {
    return Number(this.props.search.get("sort")) as cache.GetCacheScoreCardRequest.OrderBy;
  }
  private getDescending() {
    return (this.props.search.get("desc") || "false") === "true";
  }
  private getFilterIndex() {
    return Number(this.props.search.get("filter") || defaultFilterIndex);
  }
  private getGroupBy() {
    return Number(
      this.props.search.get("groupBy") || cache.GetCacheScoreCardRequest.GroupBy.GROUP_BY_TARGET
    ) as cache.GetCacheScoreCardRequest.GroupBy;
  }
  private getSearch() {
    return this.props.search.get("search") || "";
  }

  private onChangeOrderBy(event: React.ChangeEvent<HTMLSelectElement>) {
    const value = Number(event.target.value) as cache.GetCacheScoreCardRequest.OrderBy;
    // When changing the sort order, set direction according to a more useful
    // default, to save an extra click. Asc is more useful for start time; Desc
    // is more useful for duration and size.
    const desc = value !== cache.GetCacheScoreCardRequest.OrderBy.ORDER_BY_START_TIME;
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      sort: String(value),
      desc: String(desc),
    });
  }
  private onToggleDescending() {
    router.setQueryParam("desc", !this.getDescending());
  }
  private onChangeFilter(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam("filter", event.target.value);
  }
  private onChangeGroupBy(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam("groupBy", event.target.value);
  }
  private searchTimeout = 0;
  private onChangeSearch(event: React.ChangeEvent<HTMLInputElement>) {
    const searchText = event.target.value;
    // Update the search box state immediately, but debounce the query update so
    // we don't fetch on each keystroke.
    this.setState({ searchText });
    clearTimeout(this.searchTimeout);
    this.searchTimeout = window.setTimeout(
      () => router.setQueryParam("search", searchText),
      SEARCH_DEBOUNCE_INTERVAL_MS
    );
  }

  private onClickLoadMore() {
    this.fetchResults();
  }

  private renderControls() {
    return (
      <>
        <div className="controls row">
          {/* Sorting controls */}
          <label>Sort by</label>
          <Select value={this.getOrderBy()} onChange={this.onChangeOrderBy.bind(this)}>
            <Option value={cache.GetCacheScoreCardRequest.OrderBy.ORDER_BY_START_TIME}>Start time</Option>
            <Option value={cache.GetCacheScoreCardRequest.OrderBy.ORDER_BY_DURATION}>Duration</Option>
            <Option value={cache.GetCacheScoreCardRequest.OrderBy.ORDER_BY_SIZE}>Size</Option>
          </Select>
          <OutlinedButton className="icon-button" onClick={this.onToggleDescending.bind(this)}>
            {this.getDescending() ? <SortDesc className="icon" /> : <SortAsc className="icon" />}
          </OutlinedButton>
          {/* Filtering controls */}
          <div className="separator" />
          <label>Show</label>
          <Select value={this.getFilterIndex()} onChange={this.onChangeFilter.bind(this)}>
            {filters.map((filter, i) => (
              <Option key={filter.label} value={i}>
                {filter.label}
              </Option>
            ))}
          </Select>
          {/* Grouping controls */}
          <div className="separator" />
          <label>Group by</label>
          <Select value={this.getGroupBy()} onChange={this.onChangeGroupBy.bind(this)}>
            <Option value={0}>(None)</Option>
            <Option value={cache.GetCacheScoreCardRequest.GroupBy.GROUP_BY_TARGET}>Target</Option>
            <Option value={cache.GetCacheScoreCardRequest.GroupBy.GROUP_BY_ACTION}>Action</Option>
          </Select>
        </div>
        <div className="controls row">
          <FilterInput value={this.state.searchText} onChange={this.onChangeSearch.bind(this)} />
        </div>
      </>
    );
  }

  private renderResults(
    results: cache.ScoreCard.IResult[],
    startTimeMillis: number,
    durationMillis: number,
    groupTarget: string | null = null,
    groupActionId: string | null = null
  ) {
    return results.map((result) => (
      <Tooltip
        className="row result-row"
        pin={pinBottomMiddleToMouse}
        renderContent={() => this.renderResultHovercard(result, startTimeMillis)}>
        {(groupTarget === null || groupActionId === null) && (
          <div className="name-column" title={result.targetId ? `${result.targetId} › ${result.actionMnemonic}` : ""}>
            {/* bes-upload events don't have a target ID or action mnemonic. */}
            {result.targetId || result.actionMnemonic ? (
              <TextLink href={this.getActionUrl(result.actionId)}>
                {groupTarget === null && result.targetId}
                {groupTarget === null && groupActionId === null && " › "}
                {groupActionId === null && result.actionMnemonic}
              </TextLink>
            ) : (
              <>{result.name ? result.name : result.actionId}</>
            )}
          </div>
        )}
        <div className="cache-type-column" title={cacheTypeTitle(result.cacheType)}>
          {renderCacheType(result.cacheType)}
        </div>
        <div className="status-column column-with-icon">{renderStatus(result)}</div>
        <div>
          <DigestComponent hashWidth="96px" sizeWidth="72px" digest={result.digest} expandOnHover={false} />
        </div>
        {this.isCompressedSizeColumnVisible() && (
          <div className={`compressed-size-column ${!result.compressor ? "uncompressed" : ""}`}>
            {(console.log(result), result.compressor) ? (
              <>
                <span>{renderCompressionSavings(result)}</span>
              </>
            ) : (
              <span>&nbsp;</span>
            )}
          </div>
        )}
        <div className="duration-column">{format.compactDurationMillis(proto.durationToMillis(result.duration))}</div>
        {this.renderWaterfallBar(result, startTimeMillis, durationMillis)}
      </Tooltip>
    ));
  }

  private renderResultHovercard(result: cache.ScoreCard.IResult, startTimeMillis: number) {
    return (
      <div className="cache-result-hovercard">
        {result.targetId ? (
          <>
            <b>Target</b> <span>{result.targetId}</span>
          </>
        ) : null}
        {result.actionMnemonic || result.actionId ? (
          <>
            <b>{result.actionMnemonic ? "Action" : "Digest"}</b>
            <span>{result.actionMnemonic || result.actionId}</span>
          </>
        ) : null}
        {result.name ? (
          <>
            <b>File</b>{" "}
            <span>
              {result.pathPrefix ? result.pathPrefix + "/" : ""}
              {result.name}
            </span>
          </>
        ) : null}
        <>
          <b>Size</b>
          <span>{format.bytes(result.digest.sizeBytes)}</span>
        </>
        {result.compressor ? (
          <>
            <b>Compressed</b>{" "}
            <span>
              {format.bytes(result.transferredSizeBytes)} {renderCompressionSavings(result)}
            </span>
          </>
        ) : null}
        <>
          {/* Timestamp relative to the invocation start time */}
          <b>Started at</b>{" "}
          <span>{format.durationMillis(proto.timestampToDate(result.startTime).getTime() - startTimeMillis)}</span>
        </>
        <>
          <b>Duration</b> <span>{format.durationMillis(proto.durationToMillis(result.duration))}</span>
        </>
      </div>
    );
  }

  private isCompressedSizeColumnVisible() {
    return (
      this.props.model.isCacheCompressionEnabled() && filters[this.getFilterIndex()].values.cache !== cache.CacheType.AC
    );
  }

  render() {
    if (this.state.loading && !this.state.results.length) {
      return (
        <RequestsCardContainer>
          {this.renderControls()}
          <div className="loading" />
        </RequestsCardContainer>
      );
    }

    if (!areResultsAvailable(this.props.model)) {
      return (
        <RequestsCardContainer>
          <div>Cache requests will be shown here when the invocation has completed.</div>
        </RequestsCardContainer>
      );
    }

    if (!this.state.results.length) {
      return (
        <RequestsCardContainer>
          {this.renderControls()}
          <div>No cache requests found.</div>
        </RequestsCardContainer>
      );
    }

    const groups = this.getGroupBy() ? groupResults(this.state.results, this.getGroupBy()) : null;
    const [startTimeMillis, durationMillis] = this.getStartTimestampAndDurationMillis();

    return (
      <RequestsCardContainer
        className={
          this.getGroupBy() === cache.GetCacheScoreCardRequest.GroupBy.GROUP_BY_TARGET ? "group-by-target" : ""
        }>
        {this.renderControls()}
        <div className="results-table">
          <div className="row column-headers">
            {this.getGroupBy() !== cache.GetCacheScoreCardRequest.GroupBy.GROUP_BY_ACTION && (
              <div className="name-column">Name</div>
            )}
            <div className="cache-type-column">Cache</div>
            <div className="status-column">Status</div>
            <div className="digest-column">Digest (hash/size)</div>
            {this.isCompressedSizeColumnVisible() && <div className="compressed-size-column">Compression</div>}
            <div className="duration-column">Duration</div>
            <div className="waterfall-column">Waterfall</div>
          </div>
          {groups === null && (
            <div className="results-list column">
              {this.renderResults(this.state.results, startTimeMillis, durationMillis)}
            </div>
          )}
          {groups?.map((group) => (
            <div className="group">
              <div className="group-title action-id row">
                <div className="row action-label">
                  <div>{group.results[0]?.targetId || group.results[0]?.actionId}</div>
                  {group.actionId && group.results[0]?.actionMnemonic && (
                    <>
                      <ChevronRight className="icon chevron" />
                      <TextLink className="action-mnemonic" href={this.getActionUrl(group.actionId)}>
                        {group.results[0]?.actionMnemonic}
                      </TextLink>
                    </>
                  )}
                </div>
              </div>
              <div className="group-contents results-list column">
                {this.renderResults(group.results, startTimeMillis, durationMillis, group.targetId, group.actionId)}
              </div>
            </div>
          ))}
        </div>
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

function areResultsAvailable(model: InvocationModel): boolean {
  return Boolean(
    model.invocations[0] &&
      model.invocations[0].invocationStatus !== invocation.Invocation.InvocationStatus.PARTIAL_INVOCATION_STATUS
  );
}

const RequestsCardContainer: React.FC<JSX.IntrinsicElements["div"]> = ({ className, children, ...props }) => (
  <div className={`card cache-requests-card ${className || ""}`} {...props}>
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

function renderCompressionSavings(result: cache.ScoreCard.IResult) {
  const compressionSavings = 1 - Number(result.transferredSizeBytes) / Number(result.digest.sizeBytes);
  return (
    <span className={`compression-savings ${compressionSavings > 0 ? "positive" : "negative"}`}>
      {compressionSavings <= 0 ? "+" : ""}
      {(-compressionSavings * 100).toPrecision(3)}%
    </span>
  );
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
        <span>Hit</span>
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

type ResultGroup = {
  // Target ID or null if not grouping by anything.
  targetId: string | null;
  // Action ID or null if not grouping by anything / grouping only by target.
  actionId: string | null;
  results: cache.ScoreCard.IResult[];
};

function getGroupKey(result: cache.ScoreCard.IResult, groupBy: cache.GetCacheScoreCardRequest.GroupBy): string | null {
  if (!groupBy) return null;
  if (groupBy === cache.GetCacheScoreCardRequest.GroupBy.GROUP_BY_ACTION) return result.actionId;
  return result.targetId;
}

/**
 * The server groups into contiguous runs of results with the same group key.
 * This un-flattens the runs into a list of groups.
 */
function groupResults(
  results: cache.ScoreCard.IResult[],
  groupBy: cache.GetCacheScoreCardRequest.GroupBy
): ResultGroup[] {
  const out: ResultGroup[] = [];
  let curRun: ResultGroup | null = null;
  let curGroup: string | null = null;
  for (const result of results) {
    const groupKey = getGroupKey(result, groupBy);
    if (!curRun || groupKey !== curGroup) {
      curRun = {
        targetId: groupBy ? result.targetId : null,
        actionId: groupBy === cache.GetCacheScoreCardRequest.GroupBy.GROUP_BY_ACTION ? result.actionId : null,
        results: [],
      };
      curGroup = groupKey;
      out.push(curRun);
    }
    curRun.results.push(result);
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
