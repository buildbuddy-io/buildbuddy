import React from "react";
import { ChevronRight, ChevronDown } from "lucide-react";
import Button, {
    FilledButton,
    OutlinedButton
} from "../../../app/components/button/button";
import Slider from "../../../app/components/slider/slider";
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
import arrayContaining = jasmine.arrayContaining;

export interface Props {
}

type FlagComponent = {
    flag: featureflag.FeatureFlag;
    groupsToDisplay: number;
    groupAssignmentPercentage: number;
}

type State = {
    showCreateForm: boolean;
    createFlagName: string;
    flags: Map<string, FlagComponent>;
    showFlagGroups: Map<string, boolean>;
    groups: featureflag.Group[] | null;
}

export default class FeatureflagsComponent extends React.Component<Props, State> {
    state: State = {
        showCreateForm: false,
        createFlagName: "",
        flags: new Map<string, FlagComponent>(),
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
                    <div className="create-button" style={{display:"none"}}>
                        <FilledButton
                            className="big-button"
                            onClick={this.onClickCreateGroups.bind(this)}>
                            Create groups
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

        var sortedFlagNames = new Array<string>();
        this.state.flags.forEach((fc) => {
            sortedFlagNames.push(fc.flag.name);
        })
        sortedFlagNames = sortedFlagNames.sort();

        const flags = new Array<JSX.Element>();
        sortedFlagNames.forEach((flagName) => {
            const fc = this.state.flags.get(flagName)!;

            const groupsInExperiment = fc.flag.experimentGroupIds.length > 0;
            const experimentOn = groupsInExperiment && fc.flag.enabled;
            flags.push(
                <div className="featureflag">
                    <div className="flag-details">
                        <div>{fc.flag.name}</div>
                        <div className="toggle">
                            <label className="switch">
                                <input type="checkbox"
                                       checked={fc.flag.enabled}
                                       onChange={this.onToggleFlag.bind(this, fc)}
                                />
                                <span className="slider round"></span>
                            </label>
                        </div>
                    </div>
                    <div style={{padding: "20px", margin: "20px"}} className={`experiment-groups card ${experimentOn ? "card-success" : "card-neutral"}`}>
                        <div className="group-toggle" onClick={this.onShowFlagGroups.bind(this, fc.flag.name)}>
                            {this.state.showFlagGroups.get(fc.flag.name) ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />}
                            <div>
                                {groupsInExperiment ? "Configured groups" : "Enabled for all groups"}
                            </div>
                        </div>
                        {this.state.showFlagGroups.get(fc.flag.name) && this.renderExperimentGroups(fc.flag.name)}
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

        const fc = this.state.flags.get(flagName)!;
        const groupElements = new Array<JSX.Element>()

        groupElements.push(
            <div className="assign-groups">
                <div className="text-button">
                    Groups to assign:
                </div>
                <Slider
                    className="assign-groups-slider"
                    value={[
                        fc.groupAssignmentPercentage,
                    ]}
                    min={0}
                    max={100}
                    renderThumb={(props, state) => (
                        <div {...props}>
                            <div className="slider-thumb-circle"></div>
                            <div className="slider-thumb-value">
                                {fc.groupAssignmentPercentage}%
                            </div>
                        </div>
                    )}
                    pearling
                    minDistance={10}
                    onChange={(e) => {
                        fc.groupAssignmentPercentage = e;
                        const mapClone = this.state.flags;
                        mapClone.set(fc.flag.name, fc);
                    }}
                />
                <div className="text-button" onClick={this.onAssignGroupPercentage.bind(this, fc)}>
                    Apply
                </div>
            </div>
        )
        for (let i = 0; i < fc.groupsToDisplay; i++) {
            const group = this.state.groups[i];
            groupElements.push(
                <div className="experiment-group">
                    <Checkbox
                        checked={fc.flag.experimentGroupIds.includes(group.groupId)}
                        onChange={this.onToggleGroup.bind(this, fc, group.groupId)}
                    />
                    <div> {group.name} </div>
                </div>
            )
        }

        const totalGroups = this.state.groups.length;
        if (fc.groupsToDisplay < totalGroups) {
            groupElements.push(
                <div className="text-button load-more-groups" onClick={() => this.onClickLoadMoreGroups(fc)}>
                    Load more groups
                </div>
            );
        }

        return groupElements
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
    onToggleGroup(fc: FlagComponent, groupID: string) {
        const idx = fc.flag.experimentGroupIds.indexOf(groupID);
        if (idx == -1) {
            fc.flag.experimentGroupIds.push(groupID);
        } else {
            fc.flag.experimentGroupIds.splice(idx, 1);
        }

        const mapClone = this.state.flags;
        mapClone.set(fc.flag.name, fc);
        this.setState( {flags: mapClone });
        this.updateExperimentAssignments(fc.flag);
    }

    onAssignGroupPercentage(fc: FlagComponent) {
        const numGroups = this.state.groups?.length!;
        let groupsToSelect = Math.floor(numGroups * (fc.groupAssignmentPercentage / 100));

        const assignedGroups = new Array<string>();
        const groupCopy = this.state.groups?.slice()!;
        while (groupsToSelect > 0) {
            const groupIdx = Math.floor(Math.random() * (groupCopy.length - 1));
            const group = groupCopy.splice(groupIdx, 1)[0];
            assignedGroups.push(group.groupId);
            groupsToSelect--;
        }

        fc.flag.experimentGroupIds = assignedGroups;

        const mapClone = this.state.flags;
        mapClone.set(fc.flag.name, fc);
        this.setState( {flags: mapClone });

        this.updateExperimentAssignments(fc.flag);
    }

    onClickLoadMoreGroups(fc: FlagComponent) {
        let numGroups = fc.groupsToDisplay + 10;
        let totalGroups = this.state.groups?.length!;
        if (numGroups > totalGroups) {
            numGroups = totalGroups;
        }
        fc.groupsToDisplay = numGroups;

        const mapClone = this.state.flags;
        mapClone.set(fc.flag.name, fc);
        this.setState( {flags: mapClone });
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
                    const fc: FlagComponent = {
                        flag: flag,
                        groupsToDisplay: 10,
                        groupAssignmentPercentage: 0,
                    }
                    m.set(flag.name, fc);
                    showGroupsMap.set(flag.name, false);
                })
                this.setState({ flags: m, showFlagGroups: showGroupsMap });
            })
            .catch((e) => errorService.handleError(e));

        this.renderFlags()
    }

    private updateFlag(flag: featureflag.FeatureFlag) {
        rpcService.service
            .updateFeatureFlag(
                new featureflag.UpdateFeatureFlagRequest({
                    name: flag.name,
                    enabled: flag.enabled,
                }))
            .then((response) => {

            })
            .catch((e) => errorService.handleError(e));

    }

    private updateExperimentAssignments(flag: featureflag.FeatureFlag) {
        rpcService.service
            .updateExperimentAssignments(
                new featureflag.UpdateExperimentAssignmentsRequest({
                    name: flag.name,
                    configuredGroupIds: flag.experimentGroupIds,
                }))
            .then((response) => {

            })
            .catch((e) => errorService.handleError(e));
    }

    onToggleFlag(fc: FlagComponent) {
        const mapClone = this.state.flags;
        fc.flag.enabled = !fc.flag.enabled;
        mapClone.set(fc.flag.name, fc);
        this.setState({flags: mapClone});
        this.updateFlag(fc.flag);
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

    onClickCreateGroups() {
        rpcService.service
            .createGroups(
                new featureflag.CreateGroupsRequest({}))
            .then((response) => {
            })
            .catch((e) => errorService.handleError(e));
    }
}
