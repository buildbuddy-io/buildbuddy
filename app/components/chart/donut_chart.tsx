import React from "react";
import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { getChartColor } from "../../util/color";
import format from "../../format/format";

interface Props {
  data: any[] | undefined;
}

const MAX_LEGEND_ENTRIES = 5;

export default class DonutChart extends React.Component<Props> {
  render() {
    let data = this.props.data?.filter((d) => d.value > 0).sort((a, b) => b.value - a.value);
    const sum = data?.reduce(
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
          `${format.formatWithCommas(data[i].value)} ${data[i].name} (${format.percent(data[i].value / sum.value)}%)`
        );
      }
    }

    data = data?.splice(0, MAX_LEGEND_ENTRIES);
    if (other > 0) {
      data?.push({ name: "Other", value: other });
    }

    return (
      <div className="donut-chart">
        <ResponsiveContainer width={80} height={80}>
          <PieChart>
            <Pie data={data} dataKey="value" outerRadius={40} innerRadius={20}>
              {data?.map((_, index) => <Cell key={`cell-${index}`} fill={getChartColor(index)} />)}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
        <div>
          {data?.map((entry, index) => (
            <div className="donut-chart-label">
              <span className="donut-chart-swatch" style={{ backgroundColor: getChartColor(index) }}></span>
              <span>
                <span className="donut-chart-legend-value">{format.formatWithCommas(entry.value)}</span>{" "}
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
    );
  }
}
