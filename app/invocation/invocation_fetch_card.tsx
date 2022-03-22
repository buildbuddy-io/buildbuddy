import React from "react";
import { DownloadCloud } from "lucide-react";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  inProgress: boolean;
}

interface State {
  loading: boolean;
}

export default class FetchCardComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
  };

  render() {
    return (
      <div className="card">
        <DownloadCloud className="icon" />
        <div className="content">
          <div className="title">Fetches</div>
          {this.props.model.getFetchURLs().length > 0 && (
            <div className="fetch-list">
              {this.props.model.getFetchURLs().map((fetchURL) => (
                <div className="fetch-url">{fetchURL}</div>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  }
}
