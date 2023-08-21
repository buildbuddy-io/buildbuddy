import moment from "moment";

import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { getProtoFilterParams } from "../filter/filter_util";
import { stats } from "../../../proto/stats_ts_proto";
import TrendsModel from "./trends_model";
import capabilities from "../../../app/capabilities/capabilities";

function getKey(prefix: string, request: { toJSON(): Object }, ...extras: any[]): string {
  return `${prefix}|${JSON.stringify(request.toJSON())}|${JSON.stringify(extras)}`;
}

type TrendsDataUpdateCallback = (model: TrendsModel) => void;

/**
 * Fetch trends data for the provided search params, optionally using a cache.
 * @param search the current search params for the trends request.
 * @param callback a callback that will receive status updates as the fetch
 * proceeds, including loading and error states.
 * @param rpcCache a cache to use, if caching is desired.
 * @returns a CancelablePromise--if you don't care about loading state, this
 * can be used to wait for the final server or cache response.  Otherwise,
 * this can be used as a handle to stop subsequent calls to the provided
 * callback, for example if search params change.
 */
export function fetchTrends(
  search: URLSearchParams,
  callback: TrendsDataUpdateCallback,
  rpcCache?: TrendsRpcCache
): CancelablePromise<stats.GetTrendResponse | undefined> {
  // Set state to loading.
  callback(new TrendsModel(true));

  let request = new stats.GetTrendRequest();
  request.query = new stats.TrendQuery();

  const filterParams = getProtoFilterParams(search, rpcCache?.getCurrentNowTime());
  if (filterParams.role) {
    request.query.role = filterParams.role;
  } else {
    // Note: Technically we're filtering out workflows and unknown roles,
    // even though the user has selected "All roles". But we do this to
    // avoid double-counting build times for workflows and their nested CI runs.
    request.query.role = ["", "CI"];
  }

  if (filterParams.host) request.query.host = filterParams.host;
  if (filterParams.user) request.query.user = filterParams.user;
  if (filterParams.repo) request.query.repoUrl = filterParams.repo;
  if (filterParams.branch) request.query.branchName = filterParams.branch;
  if (filterParams.commit) request.query.commitSha = filterParams.commit;
  if (filterParams.command) request.query.command = filterParams.command;
  if (capabilities.config.patternFilterEnabled && filterParams.pattern) request.query.pattern = filterParams.pattern;
  if (filterParams.tags) request.query.tags = filterParams.tags;
  if (filterParams.status) request.query.status = filterParams.status;

  request.query.updatedBefore = filterParams.updatedBefore;
  request.query.updatedAfter = filterParams.updatedAfter;

  const key = getKey("GetTrends", request);
  const cached = rpcCache?.get(key);
  if (cached) {
    callback(new TrendsModel(false, undefined, request, cached));
    return new CancelablePromise(Promise.resolve(cached));
  }

  return rpcService.service
    .getTrend(request)
    .then((response) => {
      console.log(key);
      rpcCache?.set(key, response);
      callback(new TrendsModel(false, undefined, request, response));
      return response;
    })
    .catch((error) => {
      callback(new TrendsModel(false, error));
      return undefined;
    });
}

export class TrendsRpcCache {
  private nowTime = moment();
  private rpcCache: Map<string, stats.GetTrendResponse> = new Map<string, stats.GetTrendResponse>();

  // When handling requests for time intervals like "last 30 days", we
  // currently just take now()-30d, which ensures that we never generate the
  // same request twice.  This function can be used by cache-users to ensure
  // that all requests using the cache agree on what time it is "right now".
  getCurrentNowTime(): moment.Moment {
    if (moment().diff(this.nowTime, "minutes", true) > 10) {
      this.nowTime = moment();
    }
    return this.nowTime;
  }

  set(key: string, response: stats.GetTrendResponse) {
    this.rpcCache.set(key, response);
  }

  get(key: string): stats.GetTrendResponse | undefined {
    return this.rpcCache.get(key);
  }
}
