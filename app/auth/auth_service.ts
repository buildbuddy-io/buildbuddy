import events from 'fbemitter';
import oidc from 'oidc-client';
import googleAuth from './google_auth';

export class User {
  name: string;
  email: string;
  profilePhotoUrl: string;
}

export class AuthService {
  user: User = null;
  userStream = new events.EventEmitter();

  userManager = googleAuth;

  static userEventName = "user";

  constructor() {
    oidc.Log.logger = console;
    this.handleRedirectStateIfPresent();
    window.addEventListener("storage", (event) => {
      this.userManager.getUser().then((user) => {
        this.emitUser(user);
      });
    });
    this.userManager.getUser().then((user) => {
      this.emitUser(user);
    });
  }

  handleRedirectStateIfPresent() {
    if (window.location.pathname.startsWith("/auth/")) {
      this.userManager.signinRedirectCallback(window.location.href).then((user) => {
        this.emitUser(user);
      });
      var newUrl = window.location.protocol + "//" + window.location.host;
      window.history.pushState({ path: newUrl }, '', newUrl);
    }
  }

  handleUserLoaded() {
    console.log("User loaded", this.userManager.getUser());
  }

  emitUser(oidcUser: oidc.User) {
    console.log("User", oidcUser);
    if (!oidcUser) {
      this.userStream.emit(AuthService.userEventName, null);
      return;
    }
    this.user = new User();
    this.user.email = oidcUser.profile.email;
    this.user.name = oidcUser.profile.name;
    this.user.profilePhotoUrl = oidcUser.profile.picture;
    this.userStream.emit(AuthService.userEventName, this.user);
  }

  login() {
    this.userManager.signinRedirect();
  }

  logout() {
    this.userManager.removeUser().then(() => {
      this.emitUser(null);
    });
  }
}

export default new AuthService();
