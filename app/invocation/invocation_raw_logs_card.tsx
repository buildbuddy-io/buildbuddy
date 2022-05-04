import React from "react";
import { PauseCircle } from "lucide-react";
import InvocationModel from "./invocation_model";
import { invocation } from "../../proto/invocation_ts_proto";
import { FilterInput } from "../components/filter_input/filter_input";

interface Props {
  model: InvocationModel;
  pageSize: number;
}

interface State {
  expandedMap: Map<Long, boolean>;
  numPages: number;
  filterString: string;
}

export default class RawLogsCardComponent extends React.Component<Props, State> {
  state: State = {
    expandedMap: new Map<Long, boolean>(),
    numPages: 1,
    filterString: "",
  };

  handleEventClicked(event: invocation.InvocationEvent) {
    this.state.expandedMap.set(event.sequenceNumber, !this.state.expandedMap.get(event.sequenceNumber));
    this.setState(this.state);
  }

  handleMoreClicked() {
    this.setState({ numPages: this.state.numPages + 1 });
  }

  handleAllClicked() {
    this.setState({ numPages: Number.MAX_SAFE_INTEGER });
  }

  handleFilterChange(event: any) {
    this.setState({ ...this.state, filterString: event.target.value });
  }

  render() {
    let filteredEvents = this.props.model.invocations.flatMap((invocation) =>
      invocation.event
        .map((event) => {
          let json = JSON.stringify((event.buildEvent as any).toJSON(), null, 4);
          return {
            event: event,
            json: json,
          };
        })
        .filter((event) =>
          this.state.filterString ? event.json.toLowerCase().includes(this.state.filterString.toLowerCase()) : true
        )
    );
    return (
      <>
        <FilterInput
          className="raw-logs-filter"
          value={this.state.filterString || ""}
          placeholder="Filter..."
          onChange={this.handleFilterChange.bind(this)}
        />
        <div className="card">
          <PauseCircle className="icon rotate-90" />
          <div className="content">
            <div className="title">Raw logs</div>
            <div className="details code">
              <div>
                {filteredEvents
                  .slice(0, (this.props.pageSize && this.state.numPages * this.props.pageSize) || undefined)
                  .map((event) => {
                    var expanded = this.state.expandedMap.get(event.event.sequenceNumber) || this.state.filterString;
                    return (
                      <div className="raw-event">
                        <div className="raw-event-title" onClick={this.handleEventClicked.bind(this, event.event)}>
                          [{expanded ? "-" : "+"}] Build event {event.event.sequenceNumber} -{" "}
                          {Object.keys(event.event.buildEvent)
                            .filter((key) => key != "id" && key != "children")
                            .join(", ")}
                        </div>
                        {expanded && <div>{event.json}</div>}
                      </div>
                    );
                  })}
              </div>
            </div>
            {this.props.pageSize &&
              filteredEvents.length > this.props.pageSize * this.state.numPages &&
              !!this.state.numPages && (
                <>
                  <div className="more" onClick={this.handleMoreClicked.bind(this)}>
                    See more events
                  </div>
                  <div className="more" onClick={this.handleAllClicked.bind(this)}>
                    See all events
                  </div>
                </>
              )}
          </div>
        </div>
      </>
    );
  }
}
