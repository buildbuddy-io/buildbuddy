---
id: config-auth
title: Auth Configuration
sidebar_label: Auth
---

Auth is only configurable in the [Enterprise version](enterprise.md) of BuildBuddy.

## OIDC

### Section

`auth:` The Auth section enables BuildBuddy authentication using an OpenID Connect provider that you specify. **Optional**

### Options

- `oauth_providers:` A list of configured OAuth Providers.
  - `issuer_url: ` The issuer URL of this OIDC Provider.
  - `client_id: ` The oauth client ID.
  - `client_secret: ` The oauth client secret.
- `enable_anonymous_usage:` If true, unauthenticated build uploads will still be allowed but won't be associated with your organization.

### Redirect URL

If during your OpenID provider configuration you're asked to enter a **Redirect URL**, you should enter `https://YOUR_BUILDBUDDY_URL/auth/`. For example if your BuildBuddy instance was hosted on `https://buildbuddy.acme.com`, you'd enter `https://buildbuddy.acme.com/auth/` as your redirect url.

### OIDC Examples

#### Google auth provider

If you'd like to use Google as an auth provider, you can easily obtain your client id and client secret [here](https://console.developers.google.com/apis/credentials).

**Example**:

```
auth:
  oauth_providers:
    - issuer_url: "https://accounts.google.com"
      client_id: "12345678911-f1r0phjnhbabcdefm32etnia21keeg31.apps.googleusercontent.com"
      client_secret: "sEcRetKeYgOeShErE"
```

#### Gitlab auth provider

You can use Gitlab as an OIDC identity provider for BuildBuddy.
This feature is available for both Gitlab On-Prem Deployment and Gitlab SaaS offering.

For more details, please refer to [Gitlab's latest Official Documentation](https://docs.gitlab.com/ee/integration/openid_connect_provider.html)

**Note**: Because [Gitlab has yet to support refresh tokens](https://gitlab.com/gitlab-org/gitlab/-/issues/16620), you need to configure BuildBuddy to not request the `offline_access` scope from Gitlab:

```
auth:
  disable_refresh_token: true
```

**Configuration**:

- First, register a Gitlab Application:
  - For Gitlab SaaS, follow [Group-Owned Application Documentation](https://docs.gitlab.com/ee/integration/oauth_provider.html#create-a-group-owned-application) instructions.
  - For Gitlab On-Prem, follow [Instance-Wide Application Documentation](https://docs.gitlab.com/ee/integration/oauth_provider.html#create-a-group-owned-application) instructions.
- The Redirect URL should be `https://YOUR_BUILDBUDDY_URL/auth/`, pointing to your existing BuildBuddy deployment.
- The scopes needed are `openid`, `profile` and `email`.

Once the Gitlab application is created, you can configure it as a BuildBuddy auth provider like so:

```
auth:
  oauth_providers:
    - issuer_url: "https://gitlab.com"
      client_id: "<GITLAB APPLICATION ID>"
      client_secret: "<GITLAB APPLICATION SECRET>"
```

#### Azure AD provider

1. Navigate to the [Azure Portal](https://portal.azure.com/).
   If there are multiple Azure AD tenants available, select the tenant that will be using BuildBuddy.

2. Register a BuildBuddy application:

   - Search for and select Azure Active Directory
   - Under `Manage`, select `App registration` -> `New registration`
   - Enter `BuildBuddy` for application name.
   - Select the account types in Azure AD that should have access to BuildBuddy.
     Usually `Accounts in this organizational directory only` is correct for the single-tenant use case,
     "Accounts in any organizational directory" is correct for the multi-tenant use case.
   - Redirect URI should be `https://YOUR_BUILDBUDDY_URL/auth/` with `Web` platform.
   - Click `Register`
   - Take note of `Application (client) ID` and `Directory (tenant) ID`.

3. Configure Application Secret

   - Click on `Certificates & secrets` -> `Client secrets` -> `New client secret`
     We recommend set the expiry date of the secret to 12 months.
   - Click `Add` -> Take note of the `Value` of the newly created secret.

4. Configure Application API scope

   - Navigate to `API permissions`
   - Select `Add a permission` -> `Microsoft Graph` -> `Delegated permission`
   - In `OpenId permissions`, select `email`, `offline_access`, `openid`, `profile`.
   - In `User`, select `User.Read`
   - Click `Add permissions`

5. After that, your BuildBuddy config should be like this
   ```
   auth:
     oauth_providers:
       - issuer_url: "https://login.microsoftonline.com/<DIRECTORY_ID>/v2.0"
         client_id: "<CLIENT_ID>"
         client_secret: "<CLIENT_SECRET>"
   ```

## SAML 2.0

SAML 2.0 authentication is avaliable for BuildBuddy Cloud (SaaS).

### Okta SAML provider

1. Find your organization's short name (slug) in your [BuildBuddy Organization Settings](https://app.buildbuddy.io/settings/) and replace instances of `<org-slug>` below with this value.

2. On the Okta Applications page, click `Create App Integration`, select `SAML 2.0`.

3. Enter `BuildBuddy` and hit `Next`.

4. Enter the following fields under **General**, making sure to replace `<org-slug>` with the value froms step 1:

a. For Single sign on URL enter: `https://app.buildbuddy.io/auth/saml/acs?slug=<org-slug>`

b. For Audience URI (SP Entity ID) enter: `https://app.buildbuddy.io/saml/metadata?slug=<org-slug>`

6. Add an `Attribute Statement` to map `email` and value `user.email`.

7. Click `Finish`.

8. Under `Metadata URL` click copy and share this URL (which should have the format `https://xxxx.okta.com/app/XXXX/sso/saml/metadata`) with BuildBuddy support.

### Azure AD / Entra SAML provider

1. Find your organization's short name (slug) in your [BuildBuddy Organization Settings](https://app.buildbuddy.io/settings/) and replace instances of `<org-slug>` below with this value.

2. Visit the [Entra portal page](https://entra.microsoft.com/), navigate to `Applications` -> `Enterprise applications`.

3. Click `New application`.

4. Search for `Azure AD SAML Toolkit` and select it. Change the name to `BuildBuddy` and hit `Create`.

5. In the newly created appliction view, navigate to `Single sign-on` and select `SAML`.

6. Click on `Edit` in the first section `Basic SAML Configuration`.

   a. `Identified (Entity ID)` should be `https://app.buildbuddy.io/saml/metadata?slug=<org-slug>`.

   b. `Reply URL (Assertion Consumer Service URL)` should be `https://app.buildbuddy.io/auth/saml/acs?slug=<org-slug>`.

   c. `Sign on URL` should be `https://app.buildbuddy.io/login?slug=<org-slug>`.

   d. Hit `Save` button.

7. Click on `Edit` in the second section `Attributes & Claims`.

   a. Select `Add new claim`.

   b. For `Name`, fill in `email`.

   c. For `Source` select `Attribute` and for `Source attribute`, search and select `user.mail`.

   d. Hit the `Save` button.

8. In the 3rd section `SAML Certificates`, copy the `App Federation Metadata Url` and share it with BuildBuddy support.

### Other providers

- Find the short name (slug) for your organization in your [BuildBuddy Organization Settings](https://app.buildbuddy.io/settings/).

- **Assertion Consumer Service (ACS) URL**: `https://app.buildbuddy.io/auth/saml/acs?slug=<org-slug>`

- **Audience URL (SP Entity ID)**: `https://app.buildbuddy.io/saml/metadata?slug=<org-slug>`

- Make sure the `email` attribute is mapped to `user.email` (or equivalent).

Once the app is created, share the **Identity Provider Metadata** URL with BuildBuddy support.
