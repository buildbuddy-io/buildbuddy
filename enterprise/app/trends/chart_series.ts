/**
 * Returns the hidden series after a legend click.
 *
 * Modifier-clicking toggles the clicked series independently. A regular click
 * isolates the clicked series unless it is already the only visible series, in
 * which case every series becomes visible.
 */
export function getHiddenSeriesAfterLegendClick(
  hiddenSeries: ReadonlySet<number>,
  clickedSeries: number,
  seriesCount: number,
  modifierKey: boolean
): Set<number> {
  if (modifierKey) {
    const nextHiddenSeries = new Set(hiddenSeries);
    if (nextHiddenSeries.has(clickedSeries)) {
      nextHiddenSeries.delete(clickedSeries);
    } else {
      nextHiddenSeries.add(clickedSeries);
    }
    return nextHiddenSeries;
  }

  let clickedSeriesIsOnlyVisible = !hiddenSeries.has(clickedSeries);
  for (let series = 0; series < seriesCount && clickedSeriesIsOnlyVisible; series++) {
    if (series !== clickedSeries && !hiddenSeries.has(series)) {
      clickedSeriesIsOnlyVisible = false;
    }
  }
  if (clickedSeriesIsOnlyVisible) {
    return new Set();
  }

  const nextHiddenSeries = new Set<number>();
  for (let series = 0; series < seriesCount; series++) {
    if (series !== clickedSeries) {
      nextHiddenSeries.add(series);
    }
  }
  return nextHiddenSeries;
}
