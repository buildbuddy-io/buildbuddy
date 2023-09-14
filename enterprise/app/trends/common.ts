import { stats } from "../../../proto/stats_ts_proto";
import { timeHour, timeDay, timeMinute } from "d3-time";

export enum TrendsTab {
  OVERVIEW,
  BUILDS,
  CACHE,
  EXECUTIONS,
  DRILLDOWN,
}

export function computeTimeKeys(
  interval: stats.StatsInterval | null | undefined,
  domain: [Date, Date]
): { timeKeys: number[]; ticks: number[] } {
  // XXX: Read step from RPC response instead.
  if (!interval) {
    // Just let recharts pick the days to render.
    return { timeKeys: timeDay.range(timeDay.floor(domain[0]), domain[1]).map((v) => v.getTime()), ticks: [] };
  } else if (interval.type == stats.IntervalType.INTERVAL_TYPE_HOUR) {
    let hourMultiple = Math.floor(domain[0].getHours() / +interval.count);
    domain[0].setHours(hourMultiple * +interval.count);
    domain[0].setMinutes(0);
    const keyDates = timeHour.range(domain[0], domain[1], +interval.count);
    const hourMods = [24, 12, 6, 3, 1];
    let ticks: number[] = [];
    for (let i = 0; i < hourMods.length; i++) {
      ticks = keyDates.filter((d) => d.getHours() % hourMods[i] === 0 && d.getMinutes() == 0).map((v) => v.getTime());
      if (ticks.length >= 4) {
        break;
      }
    }
    return { timeKeys: keyDates.map((v) => v.getTime()), ticks };
  } else if (interval.type == stats.IntervalType.INTERVAL_TYPE_MINUTE) {
    let minuteMultiple = Math.floor(domain[0].getMinutes() / +interval.count);
    domain[0].setMinutes(minuteMultiple * +interval.count);

    const keyDates = timeMinute.range(domain[0], domain[1], +interval.count);
    const minuteMods = [720, 360, 180, 60, 30];
    let ticks: number[] = [];
    for (let i = 0; i < minuteMods.length; i++) {
      ticks = keyDates
        .filter((d) => (d.getHours() * 60 + d.getMinutes()) % minuteMods[i] === 0)
        .map((v) => v.getTime());
      if (ticks.length >= 4) {
        break;
      }
    }
    return { timeKeys: keyDates.map((v) => v.getTime()), ticks };
  }
  return { timeKeys: timeDay.range(timeDay.floor(domain[0]), domain[1]).map((v) => v.getTime()), ticks: [] };
}
