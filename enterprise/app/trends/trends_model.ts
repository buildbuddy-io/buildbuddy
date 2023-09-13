import { timestampToDate } from "../../../app/util/proto";
import { stats } from "../../../proto/stats_ts_proto";
import { timeHour, timeDay } from "d3-time";

/**
 * This is a shared model class for "trends" data, which currently just means
 * whatever data is jammed into GetTrendRequest/Response.  Its primary purpose
 * is to abstract away a few of the rough edges in the GetTrendResponse.
 *
 * Creating a TrendsModel is cheap, so we currently rely on RPC caching and
 * utility functions to create separate TrendsModel instances for each tab in
 * the Trends UI.  This makes life easier than coordinating loading state
 * between the different tabs that use the same RPC request/response pairs.
 */
export default class TrendsModel {
  private loading: boolean;
  private error?: string;
  private data: stats.GetTrendResponse;
  private timeKeys: number[];
  private timeToStatMap: Map<number, stats.ITrendStat>;
  private timeToExecutionStatMap: Map<number, stats.IExecutionStat>;

  constructor(loading: boolean, error?: string, request?: stats.GetTrendRequest, response?: stats.GetTrendResponse) {
    this.loading = loading;
    this.error = error;
    this.data = response ?? stats.GetTrendResponse.create({});

    if (request) {
      const domain: [Date, Date] = [
        // Note that start date should always be defined, even though we aren't asserting here.
        timestampToDate(request.query?.updatedAfter ?? {}),
        // End date may not be defined -- default to today.
        request?.query?.updatedBefore ? timestampToDate(request.query.updatedBefore) : new Date(),
      ];
      this.timeKeys = computeTimeKeys(domain);
    } else {
      this.timeKeys = [];
    }

    this.timeToStatMap = new Map<number, stats.ITrendStat>();
    for (let stat of response?.trendStat ?? []) {
      const time = new Date(stat.name + " 00:00").getTime();
      this.timeToStatMap.set(time, stat);
    }
    this.timeToExecutionStatMap = new Map<number, stats.IExecutionStat>();
    for (let stat of response?.executionStat ?? []) {
      const time = new Date(stat.name + " 00:00").getTime();
      this.timeToExecutionStatMap.set(time, stat);
    }
  }

  public hasInvocationStatPercentiles() {
    return this.data.hasInvocationStatPercentiles;
  }

  public getStats() {
    return this.data.trendStat;
  }

  public getStat(time: number): stats.ITrendStat {
    return this.timeToStatMap.get(time) || {};
  }

  public hasExecutionStats() {
    return this.timeToExecutionStatMap.size > 0;
  }

  public getExecutionStat(time: number): stats.IExecutionStat {
    return this.timeToExecutionStatMap.get(time) || {};
  }

  public getTimeKeys() {
    return this.timeKeys;
  }

  public getCurrentSummary() {
    return this.data.currentSummary;
  }

  public getPreviousSummary() {
    return this.data.previousSummary;
  }

  public isLoading(): boolean {
    return !this.error && this.loading;
  }

  public isError() {
    return this.error !== undefined;
  }

  public getError() {
    return this.error;
  }
}

// TODO(jdhollen): support specifying a mod to skip specific ticks
function computeTimeKeys(domain: [Date, Date]): number[] {
  if (domain[1].getTime() - domain[0].getTime() > 1000 * 60 * 60 * 48) {
    return timeDay.range(timeDay.floor(domain[0]), domain[1]).map((d: Date) => d.getTime());
  } else {
    return timeHour.range(timeHour.floor(domain[0]), domain[1]).map((d: Date) => d.getTime());
  }
}
