import React from "react";

import {
  ResponsiveContainer,
  ComposedChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Bar,
  Line,
  Legend,
  Tooltip,
  Cell,
  YAxisProps,
} from "recharts";

interface Props {
  title: string;
}

export default class TrendsSummaryComponent extends React.Component<Props> {
  render() {
    return <div>{this.props.title}</div>;
  }
}
