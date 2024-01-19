import React from "react";
import { ChevronRight, ChevronDown } from "lucide-react";
import {
    FilledButton,
    OutlinedButton
} from "../../../app/components/button/button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import TextInput from "../../../app/components/input/input";
import rpcService from "../../../app/service/rpc_service";
import {featureflag} from "../../../proto/featureflag_ts_proto";
import errorService from "../../../app/errors/error_service";
import Modal from "../../../app/components/modal/modal";
import Dialog, {
    DialogBody, DialogFooter, DialogFooterButtons,
    DialogHeader,
    DialogTitle
} from "../../../app/components/dialog/dialog";
import capabilities from "../../../app/capabilities/capabilities";
import Spinner from "../../../app/components/spinner/spinner";

export interface Props {
}

type State = {
    showCreateForm: boolean;
    createFlagName: string;
    flags: Map<string, featureflag.FeatureFlag>;
    showFlagGroups: Map<string, boolean>;
    groups: featureflag.Group[] | null;
}

export default class FeatureflagsComponent extends React.Component<Props, State> {
    state: State = {
        showCreateForm: false,
        createFlagName: "",
        flags: new Map<string, featureflag.FeatureFlag>(),
        showFlagGroups: new Map<string, boolean>(),
        groups: null,
    };

  componentDidMount() {
    this.fetchFlags();
    this.getGroups();
  }

  render() {
    return (
    <div>
      <div>
        <div className="create-button">
          <FilledButton
              className="big-button"
              onClick={this.onClickCreateNew.bind(this)}>
              Create new flag
          </FilledButton>
            {this.renderCreateForm()}
        </div>
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
                <div style={{padding: "20px", margin: "20px"}} className={`experiment-groups card ${experimentOn ? "card-success" : "card-neutral"}`}>
                    <div className="group-toggle" onClick={this.onShowFlagGroups.bind(this, flag.name)}>
                        {this.state.showFlagGroups.get(flag.name) ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />}
                        <div>
                            {experimentOn ? "Configured groups" : "Enabled for all groups"}
                        </div>
                    </div>
                    {this.state.showFlagGroups.get(flag.name) && this.renderExperimentGroups(flag.name)}
                </div>
            </div>
        )
      })
      return flags;
  }

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

    renderCreateForm() {
        return (
            <Modal isOpen={this.state.showCreateForm}>
                <Dialog>
                    <DialogHeader>
                        <DialogTitle>Create a Flag</DialogTitle>
                    </DialogHeader>
                    <form className="api-keys-form" onSubmit={this.createFF.bind(this)}>
                        <DialogBody>
                            <div className="field-container">
                                <label className="title" htmlFor="label">
                                    Name
                                </label>
                                <TextInput
                                    onChange={(e) => this.setState({ createFlagName: e.target.value })}
                                />
                            </div>
                        </DialogBody>
                        <DialogFooter>
                            <DialogFooterButtons>
                                <OutlinedButton type="button"
                                    onClick={this.onCloseCreateForm.bind(this)}
                                >
                                    Cancel
                                </OutlinedButton>
                                <FilledButton type="submit"
                                >
                                    Create
                                </FilledButton>
                            </DialogFooterButtons>
                        </DialogFooter>
                    </form>
                </Dialog>
            </Modal>
        )
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
              const showGroupsMap = this.state.showFlagGroups;
              response.flags.forEach((flag) => {
                  m.set(flag.name, flag);
                  showGroupsMap.set(flag.name, false);
              })
              this.setState({ flags: m, showFlagGroups: showGroupsMap });
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

    onShowFlagGroups(flagName: string) {
      const m = this.state.showFlagGroups;
      const prev = m.get(flagName);
      m.set(flagName, !prev);
      this.setState({showFlagGroups: m});
    }

   onClickCreateNew() {
     this.setState({showCreateForm: true});
   }

   onCloseCreateForm() {
       this.setState({showCreateForm: false});
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
