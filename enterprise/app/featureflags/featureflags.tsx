import React from "react";
import { ChevronRight } from "lucide-react";
import { FilledButton } from "../../../app/components/button/button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import TextInput from "../../../app/components/input/input";
import rpcService from "../../../app/service/rpc_service";
import {featureflag} from "../../../proto/featureflag_ts_proto";
import errorService from "../../../app/errors/error_service";

export interface Props {
}

type State = {
    createFlagName: string;
    flags: Map<string, featureflag.FeatureFlag>;
    groups: featureflag.Group[] | null;
}

export default class FeatureflagsComponent extends React.Component<Props, State> {
    state: State = {
        createFlagName: "",
      flags: new Map<string, featureflag.FeatureFlag>(),
      groups: null,
    };

  componentDidMount() {
    this.fetchFlags();
    this.getGroups();
  }

  render() {
    return (
    <div>
      <div className="ip-rules">
        <div className="title">Name:</div>
        <TextInput
            onChange={(e) => this.setState({ createFlagName: e.target.value })}
        />
          <FilledButton
          onClick={this.createFF.bind(this)}>
          Create
        </FilledButton>
      </div>
        {this.state.flags && (
            <div>
                {this.renderFlags()}
            </div>
        )}
    </div>
    );
  }

  renderFlags() {
      if (this.state.flags.size == 0) return;
      const flags = new Array<JSX.Element>();
      this.state.flags.forEach((flag) => {
          const experimentOn = flag.experimentGroupIds.length > 0;
        flags.push(
            <div className="featureflag">
                <div className="flag-details">
                    <div>{flag.name}</div>
                    <div className="toggle">
                        <label className="switch">
                            <input type="checkbox"
                                   checked={flag.enabled}
                                   onChange={this.onToggleFlag.bind(this, flag)}
                            />
                            <span className="slider round"></span>
                        </label>
                    </div>
                </div>
                <div className="card card-neutral experiment-groups">
                    <div className="group-toggle">
                        <ChevronRight className="icon" />
                        <div>
                            {experimentOn ? "Configured groups" : "Enabled for all groups"}
                        </div>
                    </div>
                    {this.state.groups && this.renderExperimentGroups(flag.name)}
                </div>
            </div>
        )
      })
      return flags;
  }

  // TODO: Should I pass the name, so we grab a fresh flag from state?
  private renderExperimentGroups(flagName: string) {
      if (!this.state.groups) {
          return;
      }


      const flag = this.state.flags.get(flagName)!;
     return this.state.groups.map((eg) => {
         return (
             <div className="experiment-group">
                 <Checkbox
                     checked={flag.experimentGroupIds.includes(eg.groupId)}
                     onChange={this.onToggleGroup.bind(this, flag, eg.groupId)}
                 />
                 <div> {eg.name} </div>
             </div>
         )
     })
  }

  onToggleGroup(flag: featureflag.FeatureFlag, groupID: string) {
      const idx = flag.experimentGroupIds.indexOf(groupID);
     if (idx == -1) {
         flag.experimentGroupIds.push(groupID);
     } else {
        flag.experimentGroupIds.splice(idx, 1);
     }

     const mapClone = this.state.flags;
     mapClone.set(flag.name, flag);
     this.setState( {flags: mapClone });
     this.onClickUpdate(flag);
  }

    private createFF() {
      if (this.state.createFlagName == "") {
          errorService.handleError("flag name required")
          return
      }
      rpcService.service
          .createFeatureFlag(
              new featureflag.CreateFeatureFlagRequest({
                  name: this.state.createFlagName,
              })
          )
          .then((response) => {
              this.fetchFlags();
          })
          .catch((e) => errorService.handleError(e));
  }

  private fetchFlags() {
      rpcService.service
          .getAllFeatureFlags(
              new featureflag.GetAllFeatureFlagsRequest({}))
          .then((response) => {
              const m = new Map(this.state.flags);
              response.flags.forEach((flag) => {
                  m.set(flag.name, flag);
              })
              this.setState({ flags: m });
          })
          .catch((e) => errorService.handleError(e));

      this.renderFlags()
  }

  private onClickUpdate(flag: featureflag.FeatureFlag) {
      rpcService.service
          .updateFeatureFlag(
              new featureflag.UpdateFeatureFlagRequest({
                  name: flag.name,
                  enabled: flag.enabled,
                  configuredGroupIds: flag.experimentGroupIds,
              }))
          .then((response) => {

          })
          .catch((e) => errorService.handleError(e));

  }
    onToggleFlag(flag: featureflag.FeatureFlag) {
        const mapClone = this.state.flags;
        flag.enabled = !flag.enabled;
        mapClone.set(flag.name, flag);
        this.setState({flags: mapClone});
        this.onClickUpdate(flag);
    }

    private getGroups(){
        rpcService.service
            .getGroups(
                new featureflag.GetGroupsRequest({}))
            .then((response) => {
                this.setState({groups: response.groups})
            })
            .catch((e) => errorService.handleError(e));
    }
}
