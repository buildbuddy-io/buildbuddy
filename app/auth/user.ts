import { grp } from "../../proto/group_ts_proto";
import { user_id } from "../../proto/user_id_ts_proto";
import { BuildBuddyServiceRpcName } from "../service/rpc_service";

export class User {
  displayUser: user_id.DisplayUser;
  groups: grp.Group[];
  selectedGroup: grp.Group;
  allowedRpcs: Set<BuildBuddyServiceRpcName>;
  githubToken: string;

  selectedGroupName() {
    if (this.selectedGroup?.name == "DEFAULT_GROUP") return "Organization";
    return this.selectedGroup?.name?.trim();
  }

  isInDefaultGroup() {
    return this.selectedGroup?.id == "GR0000000000000000000";
  }

  canCall(rpc: BuildBuddyServiceRpcName) {
    return this.allowedRpcs.has(rpc);
  }
}
