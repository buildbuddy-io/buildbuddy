import moment from "moment";

import { timestampToDate } from "../../../app/util/proto";
import { stats } from "../../../proto/stats_ts_proto";

function getDatesBetween(start: Date, end: Date): string[] {
  const endMoment = moment(end);
  const formattedDates: string[] = [];
  for (let date = moment(start); date.isBefore(endMoment); date = date.add(1, "days")) {
    formattedDates.push(date.format("YYYY-MM-DD"));
  }
  return formattedDates;
}

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
  private dates: string[];
  private dateToStatMap: Map<string, stats.ITrendStat>;
  private dateToExecutionStatMap: Map<string, stats.IExecutionStat>;

  constructor(loading: boolean, error?: string, request?: stats.GetTrendRequest, response?: stats.GetTrendResponse) {
    this.loading = loading;
    this.error = error;
    this.data = response ?? stats.GetTrendResponse.create({});

    this.dates = getDatesBetween(
      // Note that start date should always be defined, even though we aren't asserting here.
      timestampToDate(request?.query?.updatedAfter ?? {}),
      // End date may not be defined -- default to today.
      request?.query?.updatedBefore ? timestampToDate(request.query.updatedBefore) : new Date()
    );

    this.dateToStatMap = new Map<string, stats.ITrendStat>();
    for (let stat of response?.trendStat ?? []) {
      this.dateToStatMap.set(stat.name, stat);
    }
    this.dateToExecutionStatMap = new Map<string, stats.IExecutionStat>();
    for (let stat of response?.executionStat ?? []) {
      this.dateToExecutionStatMap.set(stat.name, stat);
    }
  }

  public hasInvocationStatPercentiles() {
    return this.data.hasInvocationStatPercentiles;
  }

  public getStats() {
    return this.data.trendStat;
  }

  public getStat(date: string): stats.ITrendStat {
    return this.dateToStatMap.get(date) || {};
  }

  public hasExecutionStats() {
    return this.dateToExecutionStatMap.size > 0;
  }

  public getExecutionStat(date: string): stats.IExecutionStat {
    return this.dateToExecutionStatMap.get(date) || {};
  }

  public getDates() {
    return this.dates;
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
