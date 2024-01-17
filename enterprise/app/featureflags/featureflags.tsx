import React from "react";
import TextInput from "../../../app/components/input/input";

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
      </div>
    );
  }
}
