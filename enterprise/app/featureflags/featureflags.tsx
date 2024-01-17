import React from "react";
import { FilledButton } from "../../../app/components/button/button";
import TextInput from "../../../app/components/input/input";
import rpcService from "../../../app/service/rpc_service";
import {featureflag} from "../../../proto/featureflag_ts_proto";
import errorService from "../../../app/errors/error_service";

export interface Props {
}

interface State {

}

export default class FeatureflagsComponent extends React.Component<Props, State> {
  render() {
    return (
      <div className="ip-rules">
        <div className="title">Visibility metadata:</div>
        <TextInput
            placeholder={"e.g. PUBLIC (optional)"}
        />
        <FilledButton
          onClick={this.createFF.bind(this)}>
          Create
        </FilledButton>
      </div>
    );
  }

  private createFF() {
      rpcService.service
          .createFeatureFlag(
              new featureflag.CreateFeatureFlagRequest({
                  name: "test-hardcoded",
              })
          )
          .then((response) => {
              console.log(response);
          })
          .catch((e) => errorService.handleError(e));
  }
}
