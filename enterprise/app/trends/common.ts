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
  if (!interval) {
    // Just let recharts pick the days to render.
    return { timeKeys: timeDay.range(timeDay.floor(domain[0]), domain[1]).map((v) => v.getTime()), ticks: [] };
  } else if (interval.type == stats.IntervalType.INTERVAL_TYPE_HOUR) {
    // First, round down to the nearest interval in the local time.
    // For example, for a 2-hour interval, this will round 3:30 to 2:00.
    const hourMultiple = Math.floor(domain[0].getHours() / +interval.count);
    domain[0].setHours(hourMultiple * +interval.count);
    domain[0].setMinutes(0);

    // These are the keys that we expect to have data for in the graph.
    const keyDates = timeHour.range(domain[0], domain[1], +interval.count);

    // We can't show too many ticks on the graph, and recharts does a bad
    // job with selecting ticks (for example, it might always pick noon
    // instead of midnight).  So we progressively search through "good"
    // intervals (midnight, midnight+noon, 0-6-12-18, etc.) until we find
    // a tick gap that gives us at least 4 ticks in the graph.
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
    // First, round down to the nearest interval in the local time.
    // For example, for a 15-minute interval, this will round 3:53 to 3:45.
    const minuteMultiple = Math.floor(domain[0].getMinutes() / +interval.count);
    domain[0].setMinutes(minuteMultiple * +interval.count);

    // These are the keys that we expect to have data for in the graph.
    const keyDates = timeMinute.range(domain[0], domain[1], +interval.count);

    // We can't show too many ticks on the graph, and recharts does a bad
    // job with selecting ticks (for example, it might always pick 12:30
    // instead of midnight).  So we progressively search through "good"
    // intervals (12 hours, 6h, 3h, 1h, 30 minutes) until we find
    // a tick gap that gives us at least 4 ticks in the graph.
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
