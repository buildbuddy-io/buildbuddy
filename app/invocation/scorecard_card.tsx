import React from "react";
import InvocationModel from "./invocation_model";
import rpcService from "../service/rpc_service";

interface Props {
  model: InvocationModel;
}

export default class ScorecardCardComponent extends React.Component {
  props: Props;

  getCacheAddress() {
    let address = this.props.model.optionsMap.get("remote_cache").replace("grpc://", "");
    address = address.replace("grpcs://", "");

    if (this.props.model.optionsMap.get("remote_instance_name")) {
      address = address + "/" + this.props.model.optionsMap.get("remote_instance_name");
    }
    return address;
  }

  getActionDownloadURL(actionId: string) {
    let actionResultURI = "actioncache://" + this.getCacheAddress() + "/blobs/ac/" + actionId + "/141";
    return rpcService.getBytestreamFileUrl(actionId, actionResultURI, this.props.model.getId());
  }

  render() {
    if (!this.props.model.scoreCard) return null;
    return (
      <div className="card scorecard">
        <img className="icon" src="/image/x-circle-regular.svg" />
        <div className="content">
          <div className="title">Cache Misses</div>
          <div className="details">
            {this.props.model.scoreCard.misses.map((result) => (
              <div>
                <div className="scorecard-target-name">{result.targetId}</div>
                <div className="scorecard-action-id-list">
                  <a href={this.getActionDownloadURL(result.actionId)} className="scorecard-action-id">
                    {result.actionId}
                  </a>
                </div>
              </div>
            ))}
          </div>
          {this.props.model.scoreCard.misses.length == 0 && <span>No Misses</span>}
        </div>
      </div>
    );
  }
}
