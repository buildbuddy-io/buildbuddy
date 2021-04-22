import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  inProgress: boolean;
}

interface State {
  loading: boolean;
}

export default class FetchCardComponent extends React.Component {
  props: Props;

  state: State = {
    loading: false,
  };

  render() {
    return (
      <div className="card">
        <img className="icon" src="/image/log-circle.svg" />
        <div className="content">
          <div className="title">Fetches</div>
          {this.props.model.getFetches().length > 0 && (
            <div className="executor-cards">
              {this.props.model.getFetches().map((fetch) => (
                <div>{fetch}</div>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  }
}
