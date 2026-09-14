import React from "react";
import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { getChartColor } from "../../util/color";
import format from "../../format/format";

export interface NamedValue {
  name: string;
  value: number;
}

interface Props {
  title?: string;
  subtitle?: string;
  data: NamedValue[] | undefined;

  // returns a valid css color string (e.g., #ffff00) given a key from data.
  colorPicker?: (k: string) => string;

  // returns a human-readable string for a given value in the data.
  valueFormatter?: (v: number) => string;
}

const OTHER_LABEL = "Other";
const MAX_LEGEND_ENTRIES = 5;

// A little function that converts an ordered list of name keys into a color
// mapping.  This is useful for staying consistent when showing multiple donuts
// with the same keys in close proximity to each other.
export function makeColorPicker(values: string[]): (name: string) => string {
  const deduped: Set<string> = new Set(values);
  const dedupedArray = [...deduped];
  if (deduped.size > MAX_LEGEND_ENTRIES && !deduped.has(OTHER_LABEL)) {
    dedupedArray.splice(MAX_LEGEND_ENTRIES, 0, OTHER_LABEL);
  }

  const mapped = new Map(dedupedArray.map((v, i) => [v, getChartColor(i)]));
  return (n: string) => mapped.get(n) ?? "#eee";
}
export default class DonutChart extends React.Component<Props> {
  render() {
    const valueFormatter = this.props.valueFormatter ?? format.formatWithCommas;
    const colorPicker = (nv: NamedValue, i: number) =>
      this.props.colorPicker ? this.props.colorPicker(nv.name) : getChartColor(i);

    let data = this.props.data?.filter((d) => d.value > 0).sort((a, b) => b.value - a.value) ?? [];
    const sum = data.reduce(
      (prev, current) => {
        return { name: "Sum", value: prev.value + current.value };
      },
      { name: "Sum", value: 0 }
    );

    let other = 0;
    let otherLabels: string[] = [];
    if (data && data?.length > MAX_LEGEND_ENTRIES) {
      for (let i = MAX_LEGEND_ENTRIES; i < data.length; i++) {
        other += data[i].value;
        otherLabels.push(
          `${valueFormatter(data[i].value)} ${data[i].name} (${format.percent(data[i].value / sum.value)}%)`
        );
      }
    }

    data = data?.splice(0, MAX_LEGEND_ENTRIES);
    if (other > 0) {
      data?.push({ name: OTHER_LABEL, value: other });
    }

    return (
      <div>
        {Boolean(this.props.title) && (
          <>
            <div className="donut-chart-title">{this.props.title}</div>
            {Boolean(this.props.subtitle) && <div className="donut-chart-subtitle">{this.props.subtitle}</div>}
          </>
        )}
        <div className="donut-chart">
          <ResponsiveContainer width={80} height={80}>
            <PieChart accessibilityLayer={false}>
              <Pie data={data} dataKey="value" outerRadius={40} innerRadius={20}>
                {data?.map((entry, index) => <Cell key={`cell-${index}`} fill={colorPicker(entry, index)} />)}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
          <div>
            {data?.map((entry, index) => (
              <div className="donut-chart-label">
                <span className="donut-chart-swatch" style={{ backgroundColor: colorPicker(entry, index) }}></span>
                <span>
                  <span className="donut-chart-legend-value">{valueFormatter(entry.value)}</span>{" "}
                  <span
                    className="donut-chart-legend-desc"
                    title={
                      other > 0 && index == MAX_LEGEND_ENTRIES
                        ? otherLabels.join(", ")
                        : `${entry.name} (${format.percent(entry.value / sum.value)}%)`
                    }>
                    {entry.name} ({format.percent(entry.value / sum.value)}%)
                  </span>
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
