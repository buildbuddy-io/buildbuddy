import oidc from 'oidc-client';

// TODO(siggisim): Github auth requires some server side support. It doesn't support implicit auth
// https://github.com/isaacs/github/issues/330
export default new oidc.UserManager({
  authority: 'https://github.com/',
  metadata: {
    authorization_endpoint: 'https://github.com/login/oauth/authorize',
    token_endpoint: 'https://github.com/login/oauth/access_token',
    userinfo_endpoint: 'https://api.github.com/user'
  },
  redirect_uri: window.location.protocol + "//" + window.location.host + "/auth/",
  client_id: 'Iv1.21e835daa9e06fb6',
  automaticSilentRenew: true,
  scope: 'user:email',
  response_type: 'code',
  userStore: new oidc.WebStorageStateStore({ store: window.localStorage })
});
