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

```yaml title="config.yaml"
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

```yaml title="config.yaml"
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

```yaml title="config.yaml"
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

5. Add an `Attribute Statement` to map `email` and value `user.email`.

6. Click `Finish`.

7. Under `Metadata URL` click copy and share this URL (which should have the format `https://xxxx.okta.com/app/XXXX/sso/saml/metadata`) with BuildBuddy support.

### Azure AD / Entra SAML provider

1. Find your organization's short name (slug) in your [BuildBuddy Organization Settings](https://app.buildbuddy.io/settings/) and replace instances of `<org-slug>` below with this value.

1. Visit the [Entra portal page](https://entra.microsoft.com/), navigate to `Applications` -> `Enterprise applications`.

1. Click `New application`.

1. Click `Create your own application`.

1. Enter `BuildBuddy` for the name and hit `Create`.

1. In the newly created appliction view, navigate to `Single sign-on` and select `SAML`.

1. Click on `Edit` in the first section `Basic SAML Configuration`.

   a. `Identified (Entity ID)` should be `https://app.buildbuddy.io/saml/metadata?slug=<org-slug>`.

   b. `Reply URL (Assertion Consumer Service URL)` should be `https://app.buildbuddy.io/auth/saml/acs?slug=<org-slug>`.

   c. `Sign on URL` should be `https://app.buildbuddy.io/login?slug=<org-slug>`.

   d. Hit `Save` button.

1. Click on `Edit` in the second section `Attributes & Claims`.

   a. Select `Add new claim`.

   b. For `Name`, fill in `email`.

   c. For `Source` select `Attribute` and for `Source attribute`, search and select `user.mail`.

   d. Hit the `Save` button.

1. In the 3rd section `SAML Certificates`, copy the `App Federation Metadata Url` and share it with BuildBuddy support.

### Other providers

- Find the short name (slug) for your organization in your [BuildBuddy Organization Settings](https://app.buildbuddy.io/settings/).

- **Assertion Consumer Service (ACS) URL**: `https://app.buildbuddy.io/auth/saml/acs?slug=<org-slug>`

- **Audience URL (SP Entity ID)**: `https://app.buildbuddy.io/saml/metadata?slug=<org-slug>`

- Make sure the `email` attribute is mapped to `user.email` (or equivalent).

Once the app is created, share the **Identity Provider Metadata** URL with BuildBuddy support.

## User management via SCIM

Users can be provisioned and deprovisioned within BuildBuddy by external auth providers using the SCIM API.

First, create an API key that will be used for managing users on the organization settings page. Select "Org admin key"
as the key type.

### Okta

1. First, we will add a custom User attribute that will determine the role of the synced user.

   1. Open the `Profile Editor` page in Okta.

   1. Select the `User (default)` profile.

   1. Add a new attribute with the following settings:

      Display name: `BuildBuddy role`

      Variable name: `buildBuddyRole`

1. Within Okta, open the BuildBuddy application that was created for SAML integration.

1. Edit the `App Settings` on the `General Page`, enable `SCIM provisioning` and Save.

1. Go to the `Provisioning` tab, click `Edit` and make the following changes:

   1. For `SCIM connector base URL`, enter `https://app.buildbuddy.io/scim`

   1. For `Unique identifier field for users `, enter `userName`.

   1. Under `Supported provisioning actions`, enable `Import New Users and Profile Updates`, `Push New Users` and `Push Profile Updates`.

   1. For `Authentication Mode`, select `HTTP Header` and enter the previously created API key as the `Token`.

   1. Click `Save`

1. On the `Provisioning` tab, click `Edit` next to `Provisioning to App`.

   1. Enable `Create Users`, `Update User Attributes` and `Deactivate Users`.

   1. Click `Save`.

1. Under `Attribute Mappings` perform the following changes:

   1. Delete all mappings except `userName`, `givenName` and `familyName`.

   1. Click `Go to Profile Editor`, click `Add Attribute` and create an attribute with the following details:

      Display name: `Role`

      Variable name: `role`

      External name: `role`

      External namespace: `urn:ietf:params:scim:schemas:core:2.0:User`

1. Navigate back to the `Provisioning` page.

1. Under `Attribute Mappings` do the following:

   1. Click `Show Unmapped Attributes`

   1. Find the `Role` attribute and click the edit icon.

   1. Under `Attribute value` select `Map from Okta Profile` and choose `buildBuddyRole` as the source attribute.

   1. For `Apply on`, select `Create and update`

   1. Click `Save` to finish adding the attribute.

By default, users that do not have the attribute field set will be created with the `developer` role.

You can modify the attribute value in Okta if you wish to grant them a different role.

### Azure AD / Entra

1. Within Entra, open the BuildBuddy application that was created for SAML integration.

1. Go to the `Provisioning` page.

1. Under the `Manage` section of side-bar, select `Provisioning`.

1. Change `Provisioning Mode` to `Automatic`.

1. Under `Admin Credentials`, enter the following information:

   1. Tenant URL: `https://app.buildbuddy.io/scim`

   1. Secret Token: Enter the value of the `Org admin key` that was created earlier.

   1. Press `Save`

1. After pressing `Save` in the previous step, you should see a new `Mappings` section. Under that section do the following:

   1. Open `Provision Microsoft Entra ID Groups` and set `Enabled` to No as BuildBuddy does not support syncing groups. Save and return to the previous page.

   1. Open `Provision Microsoft Entra ID Users` and make the following changes:

      1. Delete all mappings except `userName`, `active`, `name.givenName` and `name.familyName`

      1. Ensure the `userName` mapping matches the attribute that was configured for SAML login.

         e.g. If SAML claims were configured to use `user.mail` then the `userName` mapping should also be set to `user.mail`.

      1. Add an attribute for the application role:

         Type: `expression`

         Expression: `SingleAppRoleAssignment([appRoleAssignments])`

         Target attribute: `roles[primary eq "True"].value`

1. The last step is to configure the BuildBuddy specific roles:

   1. From the main Entra page, open the `App registrations` page.

   1. Click on the `BuildBuddy` application (you may need to select `All applications` to see it)

   1. Go to the `App roles` page.

   1. Delete the `Users` role.

   1. Create a role for each BuildBuddy role to use.

      The available roles are "admin", "developer", "writer", "reader".

      The display name should exactly match one of the values listed above and the value can be anything.

      When sending role information downstream, Entra only sends the role display name, ignoring the role value.
