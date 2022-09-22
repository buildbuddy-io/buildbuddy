import { Subject } from "rxjs";
import { grp } from "../../proto/group_ts_proto";
import { user_id } from "../../proto/user_id_ts_proto";
import { user } from "../../proto/user_ts_proto";
import capabilities from "../capabilities/capabilities";
import rpcService, { BuildBuddyServiceRpcName } from "../service/rpc_service";
import errorService from "../errors/error_service";
import { BuildBuddyError } from "../../app/util/errors";
import router from "../router/router";
import { User } from "./user";

export { User };

const SELECTED_GROUP_ID_LOCAL_STORAGE_KEY = "selected_group_id";
const IMPERSONATING_GROUP_ID_SESSION_STORAGE_KEY = "impersonating_group_id";
const AUTO_LOGIN_ATTEMPTED_STORAGE_KEY = "auto_login_attempted";
const TOKEN_REFRESH_INTERVAL_SECONDS = 30 * 60; // 30 minutes

export class AuthService {
  user: User = null;
  userStream = new Subject<User>();

  static userEventName = "user";

  register() {
    if (!capabilities.auth) return;
    // Set initially preferred group ID from local storage.
    rpcService.requestContext.groupId = window.localStorage.getItem(SELECTED_GROUP_ID_LOCAL_STORAGE_KEY);
    // Set impersonating group ID from session storage, so impersonation doesn't
    // persist across different sessions.
    rpcService.requestContext.impersonatingGroupId = window.sessionStorage.getItem(
      IMPERSONATING_GROUP_ID_SESSION_STORAGE_KEY
    );

    let request = new user.GetUserRequest();
    this.getUser(request)
      .then((response: user.GetUserResponse) => {
        this.handleLoggedIn(response);
      })
      .catch((error: any) => {
        if (BuildBuddyError.parse(error).code == "PermissionDenied" && String(error).includes("logged out")) {
          this.emitUser(null);
        } else if (
          BuildBuddyError.parse(error).code == "PermissionDenied" &&
          String(error).includes("session expired")
        ) {
          this.refreshToken();
        } else if (BuildBuddyError.parse(error).code == "Unauthenticated" || String(error).includes("not found")) {
          this.createUser();
        } else {
          this.onUserRpcError(error);
        }
      })
      .finally(() => {
        setTimeout(() => {
          this.startRefreshTimer();
        }, 1000); // Wait a second before starting the refresh timer so we can grab the session duration.
      });
  }

  getCookie(name: string) {
    let match = document.cookie.match(new RegExp("(^| )" + name + "=([^;]+)"));
    if (match) return match[2];
  }

  startRefreshTimer() {
    let refreshFrequencySeconds =
      parseInt(this.getCookie("Session-Duration-Seconds")) / 2 || TOKEN_REFRESH_INTERVAL_SECONDS;
    console.info(`Refreshing access token every ${refreshFrequencySeconds} seconds.`);
    setInterval(() => {
      if (this.user) this.refreshToken();
    }, refreshFrequencySeconds * 1000);
  }

  refreshToken() {
    return this.getUser(new user.GetUserRequest()).catch((error: any) => {
      let parsedError = BuildBuddyError.parse(error);
      console.warn(parsedError);
      if (parsedError?.code == "Unauthenticated" || parsedError?.code == "PermissionDenied") {
        this.handleTokenRefreshError();
      }
    });
  }

  handleLoggedIn(response: user.GetUserResponse) {
    localStorage.removeItem(AUTO_LOGIN_ATTEMPTED_STORAGE_KEY);
    this.emitUser(this.userFromResponse(response));
  }

  handleTokenRefreshError() {
    // If we've already tried to auto-relogin and it didn't work, just log the user out.
    if (localStorage.getItem(AUTO_LOGIN_ATTEMPTED_STORAGE_KEY)) {
      this.emitUser(null);
      return;
    }
    // If we haven't tried to auto-relogin already, try it.
    localStorage.setItem(AUTO_LOGIN_ATTEMPTED_STORAGE_KEY, "true");
    window.location.href = `/login/?${new URLSearchParams({
      redirect_url: window.location.href,
    })}`;
  }

  refreshUser() {
    return this.getUser(new user.GetUserRequest())
      .then((response: user.GetUserResponse) => {
        this.handleLoggedIn(response);
      })
      .catch((error: any) => {
        this.onUserRpcError(error);
      });
  }

  private getUser(request: user.IGetUserRequest) {
    if (rpcService.requestContext.impersonatingGroupId) {
      return rpcService.service.getImpersonatedUser(request);
    }
    return rpcService.service.getUser(request);
  }

  createUser() {
    let request = new user.CreateUserRequest();
    rpcService.service
      .createUser(request)
      .then((response: user.CreateUserResponse) => {
        this.refreshUser();
      })
      .catch((error: any) => {
        // TODO(siggisim): Remove "No user token" string matching after the next release.
        if (BuildBuddyError.parse(error).code == "Unauthenticated" || String(error).includes("No user token")) {
          console.log("User was not created because no auth cookie was set, this is normal.");
          this.emitUser(null);
        } else {
          this.onUserRpcError(error);
        }
      });
  }

  onUserRpcError(error: any) {
    errorService.handleError(error);
    this.emitUser(null);
  }

  userFromResponse(response: user.GetUserResponse) {
    let user = new User();
    user.displayUser = response.displayUser as user_id.DisplayUser;
    user.groups = response.userGroup as grp.Group[];
    user.selectedGroup = response.userGroup.find((group) => group.id === response.selectedGroupId) as grp.Group;
    user.githubToken = response.githubToken;
    user.allowedRpcs = new Set(
      response.allowedRpc.map(
        // Ensure RPC names are lowerCamelCase so that they match the RPC names
        // generated by protobufjs.
        (name) => (name[0].toLowerCase() + name.substring(1)) as BuildBuddyServiceRpcName
      )
    );
    user.isImpersonating = Boolean(rpcService.requestContext.impersonatingGroupId);
    return user;
  }

  emitUser(user: User | null) {
    console.log("User", user);
    this.user = user;
    this.updateRequestContext();
    // Ensure that the user we are about to emit will see a route they are
    // authorized to view.
    router.rerouteIfNecessary(user);
    this.userStream.next(user);
  }

  updateRequestContext() {
    let cookieName = "userId";
    let match = document.cookie.match("(^|[^;]+)\\s*" + cookieName + "\\s*=\\s*([^;]+)");
    let userIdFromCookie = match ? match.pop() : "";
    rpcService.requestContext.userId = new user_id.UserId();
    rpcService.requestContext.userId.id = userIdFromCookie;
    rpcService.requestContext.groupId = this.user?.selectedGroup?.id || "";
  }

  async setSelectedGroupId(groupId: string, { reload = false }: { reload?: boolean } = {}) {
    window.localStorage.setItem(SELECTED_GROUP_ID_LOCAL_STORAGE_KEY, groupId);
    if (reload) {
      // Don't publish a new user to avoid UI flickering.
      window.location.reload();
      return;
    }
    const selectedGroup = this.user.groups.find((group) => group.id === groupId);
    if (!selectedGroup) {
      await this.refreshUser();
    } else {
      this.emitUser(Object.assign(new User(), this.user, { selectedGroup }));
    }
  }

  async enterImpersonationMode(groupId: string) {
    window.sessionStorage.setItem(IMPERSONATING_GROUP_ID_SESSION_STORAGE_KEY, groupId);
    window.location.reload();
  }

  async exitImpersonationMode() {
    window.sessionStorage.removeItem(IMPERSONATING_GROUP_ID_SESSION_STORAGE_KEY);
    window.location.reload();
  }

  login(slug?: string) {
    const search = new URLSearchParams(window.location.search);
    if (slug) {
      window.location.href = `/login/?${new URLSearchParams({
        redirect_url: search.get("redirect_url") || window.location.href,
        slug,
      })}`;
      return;
    }

    window.location.href = `/login/?${new URLSearchParams({
      redirect_url: search.get("redirect_url") || window.location.href,
      issuer_url: capabilities.auth,
    })}`;
  }

  logout() {
    window.location.href = `/logout/`;
  }
}

export default new AuthService();
