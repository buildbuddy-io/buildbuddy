import pako from "pako";
import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import rpcService from "../service/rpc_service";

interface Props {
  model: InvocationModel;
  inProgress: boolean;
  hash: string;
  size: Long;
}

interface State {
  loading: boolean;
  contents: string;
}

export default class ActionCardComponent extends React.Component {
  props: Props;

  state: State = {
    loading: false,
    contents: ""
  };

  fetchAction() {
    let actionFile = "bytestream://localhost:1987/blobs/" + this.props.hash + "/" + String(this.props.size);
    rpcService
      .fetchBytestreamFile(actionFile, this.props.model.getId())
      .then((contents: any) => {
        this.setState({
            contents: contents,
            loading: true,
        })
      })
      .catch(() => {
        console.error("Error loading bytestream timing profile!");
        this.setState({
          ...this.state,
          timingLoaded: false,
          error: "Error loading timing profile. Make sure your cache is correctly configured.",
        });
      });
    
  
  }

  render() {
    return (
      <div className="card">
        <img className="icon" src="/image/link.svg" />
        <div className="content">
          <div className="title" onClick={this.fetchAction.bind(this)}>{this.props.hash}/{this.props.size}</div>
          {this.state.loading && (
            <div>
                {this.state.contents}
            </div>
          )}
        </div>
      </div>
    );
  }
}
