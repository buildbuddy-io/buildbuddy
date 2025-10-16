import Long from "long";
import React from "react";
import uPlot from "uplot";
import { io } from "../proto/databuddy_ts_proto";
import Plot from "./components/Plot";
import { ColumnModel } from "./model";

export type ChartProps = {
  chart: io.buildbuddy.databuddy.IChart;
  columns: ColumnModel[];
};

// TODO: more colors
const COLORS = ["green", "red", "blue", "orange", "purple"];

type ChartGridItemModel = {
  repeatColumns: ColumnModel[];
  repeatColumnValues: any[];
  columns: ColumnModel[];
};

/**
 * Chart grid component.
 *
 * The chart is repeated for each unique tuple value of the configured `repeat`
 * columns.
 */
export default React.memo(function ChartGrid({ chart, columns }: ChartProps) {
  // Computing chart data is expensive; memoize based on result.
  // TODO: maybe do this on the server.
  const plots: PlotData[] = React.useMemo(() => {
    const gridItemsByKey: Record<string, ChartGridItemModel> = {};
    const numRows = columns?.[0]?.data.length ?? 0;
    const repeatColumnIndexes = (chart.repeat ?? [])
      .map((repeatColumn) => columns.findIndex((col) => col.name === repeatColumn))
      .filter((index) => index >= 0);
    const repeatColumns = repeatColumnIndexes.map((index) => columns[index]);
    for (let r = 0; r < numRows; r++) {
      const repeatKey = repeatKeyForRow(columns, repeatColumnIndexes, r);
      let gridItem = gridItemsByKey[repeatKey];
      if (gridItem === undefined) {
        gridItem = {
          repeatColumns,
          repeatColumnValues: repeatColumnIndexes.map((c) => columns[c].data[r]),
          columns: columns.map((column) => ({ ...column, data: [] })),
        };
        gridItemsByKey[repeatKey] = gridItem;
      }
      for (let c = 0; c < columns.length; c++) {
        gridItem.columns[c].data.push(columns[c].data[r]);
      }
    }

    // TODO: use lazy rendering to support > 100 plots
    let plots = Object.values(gridItemsByKey).map((gridItem) => getChartModel(chart, gridItem));
    if (plots.length > 100) {
      plots = plots.slice(0, 100);
    }

    return plots;
  }, [chart, columns]);

  return (
    <>
      {plots.map((plot, i) => {
        if (plot.error !== undefined) {
          return (
            <div key={i} className="error">
              {plot.error}
            </div>
          );
        }

        const opts: uPlot.Options = {
          title: plot.repeatColumnValues.map((value, i) => `${plot.repeatColumns[i].name} "${value}"`).join(", "),
          width: 640,
          height: 320,
          axes: [{}, ...plot.yAxes],
          series: [{}, ...plot.ySeries],
        };

        return <Plot key={i} opts={opts} data={plot.data} className="plot" />;
      })}
    </>
  );
});

type PlotData = ReturnType<typeof getChartModel>;

function getChartModel(chart: io.buildbuddy.databuddy.IChart, gridItem: ChartGridItemModel) {
  // Compute unix timestamps
  const xColumn = gridItem.columns?.find((col) => col.name === chart.x?.column);
  if (!xColumn) {
    return {
      error: 'Missing or invalid "x" column',
    };
  }
  const toUnixTimestamp = getTimestampConverter(xColumn);
  if (!toUnixTimestamp) {
    return {
      error: `Missing timestamp converter for ${xColumn.name} (${xColumn.databaseTypeName}). Try aliasing column name with units, like "SELECT my_timestamp as my_timestamp_usec"`,
    };
  }

  const yColumns = (chart.y ?? []).flatMap((y) => {
    // Handle special "..." syntax - expands to "all columns not referenced in x or repeat"
    if (y.column === "...") {
      return gridItem.columns?.filter(
        (col) =>
          col.name !== chart.x?.column &&
          !(chart.repeat || []).includes(col.name) &&
          (col.clientType === "Long" || col.clientType === "Number")
      );
    } else {
      const column = gridItem.columns?.find((col) => col.name === y.column);
      return column ? [column] : [];
    }
  });

  const ySeries: uPlot.Series[] = yColumns.map((column: ColumnModel, i) => ({
    label: column.name,
    stroke: COLORS[i % COLORS.length],
  }));

  const ySeriesData = yColumns.map((column) => getYValues(column)).filter((data) => !!data);

  const yAxes: uPlot.Axis[] = yColumns.map((_, i) => ({
    size: 100,
    show: i === 0,
  }));

  const xTimestamps = xColumn.data.map(toUnixTimestamp);
  const data: uPlot.AlignedData = [xTimestamps, ...ySeriesData];

  return {
    ...gridItem,
    ySeries,
    ySeriesData,
    yAxes,
    data,
  };
}

function repeatKeyForRow(columns: ColumnModel[], repeatColumnIndexes: number[], rowIndex: number): string {
  const values = [];
  for (const c of repeatColumnIndexes) {
    const value = columns[c].data[rowIndex];
    values.push(String(value));
  }
  return values.join(";");
}

function getTimestampConverter(column: ColumnModel): ((value: any) => number) | null {
  // TODO: support more formats
  // TODO: distinguish types by database engine
  if (column.databaseTypeName === "DateTime('UTC')") {
    return (value: string) => {
      return Date.parse(value) / 1000;
    };
  }
  if (column.name.endsWith("_usec") && column.clientType === "Long") {
    const microsPerSecond = Long.fromNumber(1_000_000);
    return (value: string) => {
      return Long.fromString(value).div(microsPerSecond).toNumber();
    };
  }
  return null;
}

function getYValues(column: ColumnModel): number[] | undefined {
  if (column.clientType === "Number") {
    return column.data as number[];
  }
  return column.data.map((value) => +value);
}
