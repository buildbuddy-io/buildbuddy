import oidc from 'oidc-client';

export default new oidc.UserManager({
  authority: 'https://accounts.google.com/',
  // Replace this with your Google Oauth client id. You can create one here - select Web app as your app type:
  // https://developers.google.com/adwords/api/docs/guides/authentication#create_a_client_id_and_client_secret
  client_id: '20395457557-du0phjnhbac4op05m32etnia21keec93.apps.googleusercontent.com',
  redirect_uri: window.location.protocol + "//" + window.location.host + "/auth/",
  automaticSilentRenew: true,
  scope: 'openid email profile',
  userStore: new oidc.WebStorageStateStore({ store: window.localStorage })
});