import { grp } from "../../proto/group_ts_proto";
import { user_id } from "../../proto/user_id_ts_proto";
import { BuildBuddyServiceRpcName } from "../service/rpc_service";

export class User {
  displayUser: user_id.DisplayUser;
  groups: grp.Group[];
  selectedGroup: grp.Group;
  allowedRpcs: Set<BuildBuddyServiceRpcName>;
  githubToken: string;
  /** Whether the user is temporarily acting as a member of the selected group. */
  isImpersonating: boolean;

  selectedGroupName() {
    if (this.selectedGroup?.name == "DEFAULT_GROUP") return "Organization";
    return this.selectedGroup?.name?.trim();
  }

  canCall(rpc: BuildBuddyServiceRpcName) {
    return this.allowedRpcs.has(rpc);
  }

  canImpersonate() {
    return this.allowedRpcs.has("getInvocationOwner");
  }
}
