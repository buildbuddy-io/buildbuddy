import React from "react";
import { User } from "../../../app/auth/auth_service";
import WorkflowsAdminComponent from "./workflows_admin";
import WorkflowsUserComponent from "./workflows_user";

export type WorkflowsProps = {
  path: string;
  user: User;
};

export default class WorkflowsComponent extends React.Component<WorkflowsProps> {
  render() {
    if (this.props.user.isGroupAdmin()) {
      return <WorkflowsAdminComponent path={this.props.path} user={this.props.user}></WorkflowsAdminComponent>;
    } else {
      return <WorkflowsUserComponent user={this.props.user}></WorkflowsUserComponent>;
    }
  }
}
