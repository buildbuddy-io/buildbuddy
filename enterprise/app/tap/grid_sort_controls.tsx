import React from "react";
import Select, { Option } from "../../../app/components/select/select";
import router from "../../../app/router/router";
import {
  COLOR_MODE_PARAM,
  ColorMode,
  SORT_DIRECTION_PARAM,
  SORT_MODE_PARAM,
  SortDirection,
  SortMode,
} from "./grid_common";

interface Props {
  search: URLSearchParams;
}

export default class GridSortControlsComponent extends React.Component<Props> {
  handleSortChange(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam(SORT_MODE_PARAM, event.target.value);
  }

  handleDirectionChange(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam(SORT_DIRECTION_PARAM, event.target.value);
  }

  handleColorChange(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam(COLOR_MODE_PARAM, event.target.value);
  }

  getSortMode(): SortMode {
    return (this.props.search.get(SORT_MODE_PARAM) as SortMode) || "pass";
  }

  getSortDirection(): SortDirection {
    return (this.props.search.get(SORT_DIRECTION_PARAM) as SortDirection) || "asc";
  }

  getColorMode(): ColorMode {
    return (this.props.search.get(COLOR_MODE_PARAM) as ColorMode) || "status";
  }

  render() {
    return (
      <div className="tap-sort-controls">
        <div className="tap-sort-control">
          <span className="tap-sort-title">Sort by</span>
          <Select onChange={this.handleSortChange.bind(this)} value={this.getSortMode()}>
            <Option value="target">Target name</Option>
            <Option value="count">Invocation count</Option>
            <Option value="pass">Pass percentage</Option>
            <Option value="avgDuration">Average duration</Option>
            <Option value="maxDuration">Max duration</Option>
            <Option value="flake">Flake percentage</Option>
          </Select>
        </div>
        <div className="tap-sort-control">
          <span className="tap-sort-title">Direction</span>
          <Select onChange={this.handleDirectionChange.bind(this)} value={this.getSortDirection()}>
            <Option value="asc">Asc</Option>
            <Option value="desc">Desc</Option>
          </Select>
        </div>
        <div className="tap-sort-control">
          <span className="tap-sort-title">Color</span>
          <Select onChange={this.handleColorChange.bind(this)} value={this.getColorMode()}>
            <Option value="status">Status</Option>
            <Option value="timing">Duration</Option>
          </Select>
        </div>
      </div>
    );
  }
}
