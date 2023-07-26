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

export enum TrendsRequestType {
  GET_TRENDS,
  GET_HEATMAP,
}

export default class TrendsRequestHelper {
  private nowTime = moment();
  private rpcCache: Map<string, stats.GetTrendResponse> = new Map<string, stats.GetTrendResponse>();

  public fetchTrends(search: URLSearchParams, callback: TrendsDataUpdateCallback) {
    if (moment().diff(this.nowTime, "minutes", true) > 10) {
      this.nowTime = moment();
    }
    // Set state to loading.
    callback(new TrendsModel(true));

    let request = new stats.GetTrendRequest();
    request.query = new stats.TrendQuery();

    const filterParams = getProtoFilterParams(search, this.nowTime);
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
    const cached = this.rpcCache.get(key);
    if (cached) {
      callback(new TrendsModel(false, undefined, request, cached));
      return new CancelablePromise(Promise.resolve(cached));
    }

    return rpcService.service
      .getTrend(request)
      .then((response) => {
        console.log(key);
        this.rpcCache.set(key, response);
        console.log(this.rpcCache);
        callback(new TrendsModel(false, undefined, request, response));
        return response;
      })
      .catch((error) => {
        callback(new TrendsModel(false, error));
      });
  }
}
