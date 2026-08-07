import { getHiddenSeriesAfterLegendClick } from "./chart_series";

describe("getHiddenSeriesAfterLegendClick", () => {
  function expectHiddenSeries(actual: ReadonlySet<number>, expected: number[]) {
    expect([...actual].sort((a, b) => a - b)).toEqual(expected);
  }

  it("isolates a series on regular click when all series are visible", () => {
    const hiddenSeries = getHiddenSeriesAfterLegendClick(new Set(), 1, 3, false);

    expectHiddenSeries(hiddenSeries, [0, 2]);
  });

  it("isolates a series on regular click when another series is hidden", () => {
    const hiddenSeries = getHiddenSeriesAfterLegendClick(new Set([0]), 1, 3, false);

    expectHiddenSeries(hiddenSeries, [0, 2]);
  });

  it("isolates a hidden series on regular click", () => {
    const hiddenSeries = getHiddenSeriesAfterLegendClick(new Set([1]), 1, 3, false);

    expectHiddenSeries(hiddenSeries, [0, 2]);
  });

  it("shows all series on regular click when the clicked series is the only visible series", () => {
    const hiddenSeries = getHiddenSeriesAfterLegendClick(new Set([0, 2]), 1, 3, false);

    expectHiddenSeries(hiddenSeries, []);
  });

  it("isolates a series on regular click when all series are hidden", () => {
    const hiddenSeries = getHiddenSeriesAfterLegendClick(new Set([0, 1, 2]), 1, 3, false);

    expectHiddenSeries(hiddenSeries, [0, 2]);
  });

  it("hides a visible series on modifier click", () => {
    const hiddenSeries = getHiddenSeriesAfterLegendClick(new Set([0]), 1, 3, true);

    expectHiddenSeries(hiddenSeries, [0, 1]);
  });

  it("shows a hidden series on modifier click", () => {
    const hiddenSeries = getHiddenSeriesAfterLegendClick(new Set([0, 1]), 1, 3, true);

    expectHiddenSeries(hiddenSeries, [0]);
  });
});
