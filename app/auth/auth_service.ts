import { Subject } from "rxjs";
import { grp } from "../../proto/group_ts_proto";
import { user_id } from "../../proto/user_id_ts_proto";
import { user } from "../../proto/user_ts_proto";
import capabilities from "../capabilities/capabilities";
import rpcService, { BuildBuddyServiceRpcName } from "../service/rpc_service";
import errorService from "../errors/error_service";
import { BuildBuddyError } from "../../app/util/errors";
import popup from "../../app/util/popup";
import router from "../router/router";
import { User } from "./user";
import rpc_service from "../service/rpc_service";

export { User };

const SELECTED_GROUP_ID_COOKIE = "Selected-Group-ID";
// Group ID used to be stored in local storage, but has been transitioned to being stored in a cookie.
const SELECTED_GROUP_ID_LOCAL_STORAGE_KEY = "selected_group_id";
const IMPERSONATING_GROUP_ID_COOKIE = "Impersonating-Group-ID";
const AUTO_LOGIN_ATTEMPTED_STORAGE_KEY = "auto_login_attempted";
const TOKEN_REFRESH_INTERVAL_SECONDS = 30 * 60; // 30 minutes

export class AuthService {
  user?: User;
  userStream = new Subject<User | undefined>();

  static userEventName = "user";

  register() {
    if (!capabilities.auth) return;
    // Set initially preferred group ID from cookie.
    const preferredGroupId =
      this.getCookie(SELECTED_GROUP_ID_COOKIE) ||
      window.localStorage.getItem(SELECTED_GROUP_ID_LOCAL_STORAGE_KEY) ||
      "";
    rpcService.requestContext.groupId = preferredGroupId;
    // Store the group ID in a cookie in case it was loaded from the old
    // local storage location.
    this.setCookie(SELECTED_GROUP_ID_COOKIE, preferredGroupId);

    rpcService.requestContext.impersonatingGroupId = this.getCookie(IMPERSONATING_GROUP_ID_COOKIE) || "";

    let request = new user.GetUserRequest();
    this.getUser(request)
      .then((response: user.GetUserResponse) => {
        this.handleLoggedIn(response);
      })
      .catch((error: any) => {
        if (BuildBuddyError.parse(error).code == "PermissionDenied" && String(error).includes("logged out")) {
          this.emitUser(undefined);
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

  // sets a cookie with a default age of 1 year.
  setCookie(name: string, value: string, { maxAge = 31536000 } = {}) {
    let cookie = `${name}=${value}; path=/;`;
    if (maxAge > 0) {
      cookie += ` max-age=${maxAge};`;
    }
    if (capabilities.config.subdomainsEnabled) {
      cookie += ` domain=${capabilities.config.domain};`;
    }
    document.cookie = cookie;
  }

  startRefreshTimer() {
    const sessionDuration = Number(this.getCookie("Session-Duration-Seconds") || 0);
    const refreshFrequencySeconds = sessionDuration ? sessionDuration / 2 : TOKEN_REFRESH_INTERVAL_SECONDS;
    console.info(`Refreshing access token every ${refreshFrequencySeconds} seconds.`);
    setInterval(() => {
      if (this.user) this.refreshToken();
      // Calling setInterval with a number larger than a 32 bit int causes refresh spamming
    }, Math.min(refreshFrequencySeconds * 1000, 86400000)); // One day in ms
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
    window.opener?.postMessage({ type: "buildbuddy_message", error: "", success: true }, window.location.origin);
    localStorage.removeItem(AUTO_LOGIN_ATTEMPTED_STORAGE_KEY);
    this.emitUser(this.userFromResponse(response));
  }

  handleTokenRefreshError() {
    // If we've already tried to auto-relogin and it didn't work, just log the user out.
    if (localStorage.getItem(AUTO_LOGIN_ATTEMPTED_STORAGE_KEY)) {
      this.emitUser(undefined);
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

  private getUser(request: user.GetUserRequest) {
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
          this.emitUser(undefined);
        } else {
          this.onUserRpcError(error);
        }
      });
  }

  onUserRpcError(error: any) {
    errorService.handleError(error);
    this.emitUser(undefined);
  }

  userFromResponse(response: user.GetUserResponse) {
    const selectedGroupId = response.selectedGroup?.groupId ? response.selectedGroup.groupId : response.selectedGroupId;

    return new User({
      displayUser: response.displayUser as user_id.DisplayUser,
      groups: response.userGroup as grp.Group[],
      selectedGroup: response.userGroup.find((group) => group.id === selectedGroupId) as grp.Group,
      selectedGroupAccess: response.selectedGroup?.access,
      githubLinked: response.githubLinked,
      allowedRpcs: new Set(
        response.allowedRpc.map(
          // Ensure RPC names are lowerCamelCase so that they match the RPC names
          // generated by protobufjs.
          (name) => (name[0].toLowerCase() + name.substring(1)) as BuildBuddyServiceRpcName
        )
      ),
      isImpersonating: response.isImpersonating,
      subdomainGroupID: response.subdomainGroupId,
    });
  }

  emitUser(user?: User) {
    console.log("User", user);
    this.user = user;
    this.updateRequestContext();
    // Ensure that the user we are about to emit will see a route they are
    // authorized to view.
    router.setUser(user);
    this.userStream.next(user);
  }

  updateRequestContext() {
    let cookieName = "userId";
    let match = document.cookie.match("(^|[^;]+)\\s*" + cookieName + "\\s*=\\s*([^;]+)");
    let userIdFromCookie = match ? match.pop() : "";
    rpcService.requestContext.userId = new user_id.UserId({ id: userIdFromCookie });
    rpcService.requestContext.groupId = this.user?.selectedGroup?.id || "";
  }

  async setSelectedGroupId(groupId: string, groupURL: string, { reload = false }: { reload?: boolean } = {}) {
    if (!this.user) throw new Error("failed to set selected group ID: not logged in");

    this.setCookie(SELECTED_GROUP_ID_COOKIE, groupId);

    // If the new group is on a different subdomain then we have to use a redirect.
    if (capabilities.config.subdomainsEnabled && new URL(groupURL).hostname != window.location.hostname) {
      window.location.href = groupURL;
      return;
    }

    if (reload) {
      // Don't publish a new user to avoid UI flickering.
      window.location.reload();
      return;
    }
    // Refresh the user to re-fetch the user properties associated with their
    // selected group, such as the allowed RPCs list.
    rpcService.requestContext.groupId = groupId;
    await this.refreshUser();
  }

  // Enters impersonation for the given group, which may either be a group ID or a URL identifier.
  async enterImpersonationMode(query: string, { redirectUrl = "" }: { redirectUrl?: string } = {}) {
    const request = grp.GetGroupRequest.create(query.startsWith("GR") ? { groupId: query } : { urlIdentifier: query });
    const response = await rpc_service.service.getGroup(request);
    this.setCookie(IMPERSONATING_GROUP_ID_COOKIE, response.id, { maxAge: 0 });

    // If we have an explicit redirect URL, navigate there directly.
    if (redirectUrl) {
      window.location.href = redirectUrl;
      return;
    }

    // If the new group is on a different subdomain then we have to use a redirect.
    if (capabilities.config.subdomainsEnabled && new URL(response.url).hostname != window.location.hostname) {
      window.location.href = response.url + window.location.pathname + window.location.search;
    } else {
      window.location.reload();
    }
  }

  async exitImpersonationMode() {
    this.setCookie(IMPERSONATING_GROUP_ID_COOKIE, "", { maxAge: 0 });
    window.location.reload();
  }

  login(slug?: string) {
    const search = new URLSearchParams(window.location.search);
    if (slug) {
      let url = `/login/?${new URLSearchParams({
        redirect_url: search.get("redirect_url") || window.location.href,
        show_picker: capabilities.config.popupAuthEnabled ? "true" : "false",
        slug,
      })}`;
      if (capabilities.config.popupAuthEnabled) {
        popup
          .open(url)
          .then(() => this.refreshUser())
          .catch(errorService.handleError);
        return;
      }
      window.location.href = url;
      return;
    }

    let url = `/login/?${new URLSearchParams({
      redirect_url: search.get("redirect_url") || window.location.href,
      show_picker: capabilities.config.popupAuthEnabled ? "true" : "false",
      issuer_url: capabilities.auth,
    })}`;
    if (capabilities.config.popupAuthEnabled) {
      popup
        .open(url)
        .then(() => this.refreshUser())
        .catch(errorService.handleError);
      return;
    }
    window.location.href = url;
  }

  logout() {
    window.location.href = `/logout/`;
  }
}

export default new AuthService();
