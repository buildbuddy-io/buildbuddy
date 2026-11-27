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

- `jwt_key:` A secret key used to sign authentication JWTs. Required when authentication is configured.
- `oauth_providers:` A list of configured OAuth Providers.
  - `issuer_url: ` The issuer URL of this OIDC Provider.
  - `client_id: ` The oauth client ID.
  - `client_secret: ` The oauth client secret.
- `enable_anonymous_usage:` If true, unauthenticated build uploads will still be allowed but won't be associated with your organization.

### JWT signing key

Set `auth.jwt_key` to a securely generated random value. Keep this value secret and use the same value for every BuildBuddy server replica. You can generate a suitable key with:

```bash
openssl rand -hex 32
```

Then add it to your BuildBuddy config:

```yaml title="config.yaml"
auth:
  jwt_key: "<randomly-generated-secret>"
```

### JWT key rotation

JWT signing keys can be rotated without interrupting authenticated requests. Complete each phase on every BuildBuddy server replica before proceeding to the next phase.

1. Generate a new key and configure it as `new_jwt_key`. Continue signing JWTs with the current key:

   ```yaml title="config.yaml"
   auth:
     jwt_key: "<current-secret>"
     new_jwt_key: "<new-secret>"
     sign_using_new_jwt_key: false
   ```

2. After every replica is configured to accept both keys, begin signing new JWTs with the new key:

   ```yaml title="config.yaml"
   auth:
     jwt_key: "<current-secret>"
     new_jwt_key: "<new-secret>"
     sign_using_new_jwt_key: true
   ```

3. Wait for all JWTs signed with the old key to expire. The default `auth.jwt_duration` is 6 hours. Then promote the new key and remove the rotation settings:

   ```yaml title="config.yaml"
   auth:
     jwt_key: "<new-secret>"
   ```

### Redirect URL

If during your OpenID provider configuration you're asked to enter a **Redirect URL**, you should enter `https://YOUR_BUILDBUDDY_URL/auth/`. For example if your BuildBuddy instance was hosted on `https://buildbuddy.acme.com`, you'd enter `https://buildbuddy.acme.com/auth/` as your redirect url.

### OIDC Examples

#### Google auth provider

If you'd like to use Google as an auth provider, you can easily obtain your client id and client secret [here](https://console.developers.google.com/apis/credentials).

**Example**:

```yaml title="config.yaml"
auth:
  jwt_key: "<randomly-generated-secret>"
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
  jwt_key: "<randomly-generated-secret>"
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
     jwt_key: "<randomly-generated-secret>"
     oauth_providers:
       - issuer_url: "https://login.microsoftonline.com/<DIRECTORY_ID>/v2.0"
         client_id: "<CLIENT_ID>"
         client_secret: "<CLIENT_SECRET>"
   ```

## SAML 2.0

SAML 2.0 authentication is available for BuildBuddy Cloud (SaaS).

### Okta SAML provider

1. Find your organization's short name (slug) in your [BuildBuddy Organization Settings](https://app.buildbuddy.io/settings/) and replace instances of `<org-slug>` below with this value.

2. On the Okta Applications page, click `Create App Integration`, select `SAML 2.0`.

3. Enter `BuildBuddy` for the application name and hit `Next`.

4. Enter the following fields under **General**, making sure to replace `<org-slug>` with the value from step 1:

   a. For Single sign on URL enter: `https://app.buildbuddy.io/auth/saml/acs?slug=<org-slug>`

   b. For Audience URI (SP Entity ID) enter: `https://app.buildbuddy.io/saml/metadata?slug=<org-slug>`

   Press `Next` to continue, followed by `Finish` to create the integration.

5. On the Sign On page, under the Attribute statements, press `Add expression` and create a mapping with name `email` and value `user.profile.email`.

6. Still on the Sign On page, copy and share the `Metadata URL` (which should have the format `https://xxxx.okta.com/app/XXXX/sso/saml/metadata`) with BuildBuddy support.

### Azure AD / Entra SAML provider

1. Find your organization's short name (slug) in your [BuildBuddy Organization Settings](https://app.buildbuddy.io/settings/) and replace instances of `<org-slug>` below with this value.

1. Visit the [Entra portal page](https://entra.microsoft.com/), navigate to `Applications` -> `Enterprise applications`.

1. Click `New application`.

1. Click `Create your own application`.

1. Enter `BuildBuddy` for the name and hit `Create`.

1. In the newly created application view, navigate to `Single sign-on` and select `SAML`.

1. Click on `Edit` in the first section `Basic SAML Configuration`.

   a. `Identifier (Entity ID)` should be `https://app.buildbuddy.io/saml/metadata?slug=<org-slug>`.

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

Users and groups can be provisioned and deprovisioned within BuildBuddy by external auth providers using the SCIM API.

First, create an API key that will be used for managing users on the organization settings page. Select "Org admin key"
as the key type.

### Okta

1. Within Okta, open the BuildBuddy application that was created for SAML integration.

1. Edit the `App Settings` on the `General Page`, enable `SCIM provisioning` and Save.

1. Go to the `Provisioning` tab, click `Edit` and make the following changes:

   1. For `SCIM connector base URL`, enter `https://app.buildbuddy.io/scim/v2`

   1. For `Unique identifier field for users `, enter `userName`.

   1. Enable all options under `Supported provisioning actions`.

   1. For `Authentication Mode`, select `HTTP Header` and enter the previously created API key as the `Token`.
   1. Press `Test Connector Configuration` to validate that the endpoint and the API key are configured properly.

   1. Click `Save`

1. On the `Provisioning` tab, click `Edit` next to `Provisioning to App`.

   1. Enable `Create Users`, `Update User Attributes` and `Deactivate Users`.

   1. Click `Save`.

1. In the `Attribute Mappings` section, delete all mappings except `userName`, `givenName` and `familyName`.

1. On the `Push Groups` tab, add each group you want synced to BuildBuddy.

   Once you have at least one group synced you can go to the BuildBuddy UI and assign a role to the group using the
   [IAM Groups](https://app.buildbuddy.io/settings/org/user-lists) page.

   Users that are synced to BuildBuddy but are not assigned a role will have the minimal (Reader) role.

### Azure AD / Entra

Entra can sync users, groups, and group memberships with BuildBuddy through SCIM. Choose where to manage roles:

- **Group-based roles (SCIM v2):** Entra syncs groups and their members. Assign roles to those groups in BuildBuddy's
  **IAM Groups** settings.
- **Legacy per-user roles:** Entra sends an application role on each user. Group objects are not synced to BuildBuddy.

#### Group-based roles (SCIM v2)

Before using this workflow, confirm that [**IAM Groups**](https://app.buildbuddy.io/settings/org/user-lists) is available in
your BuildBuddy organization. If it is not, contact BuildBuddy support. Group-based application assignment requires
[Microsoft Entra ID P1 or P2](https://learn.microsoft.com/en-us/entra/fundamentals/licensing).

**Group-member removals:** Do not enable Entra's
[`aadOptscim062020` compatibility flag](https://learn.microsoft.com/en-us/entra/identity/app-provisioning/application-provisioning-config-problem-scim-compatibility#flags-to-alter-the-scim-behavior)
with this workflow. The flag uses filtered removal paths such as `members[value eq "user-id"]`, which BuildBuddy does not
currently support. BuildBuddy accepts removal requests with `path: "members"` and a `value` array of member IDs. Verify
removals through scheduled provisioning before rolling out group-based roles. On-demand provisioning alone is not
sufficient because Microsoft
[notes that the flag does not apply to it](https://learn.microsoft.com/en-us/entra/identity/app-provisioning/application-provisioning-config-problem-scim-compatibility#flags-to-alter-the-scim-behavior).

1. In Entra, open the enterprise application used for BuildBuddy SAML login and navigate to **Provisioning**.

1. Configure automatic provisioning with the following credentials:

   - **Tenant URL:** `https://app.buildbuddy.io/scim/v2`
   - **Secret Token:** The BuildBuddy **Org admin key** created above.

   Use **Test Connection**, then save the configuration. In the new Entra provisioning experience, credentials are
   configured under **Connectivity**.

1. Under **Attribute mapping > Users**, retain only the following target attributes (the BuildBuddy side of each mapping):

   - `userName`: Map from `mail` if the SAML `email` claim uses `user.mail`. These values must match; the provisioning UI
     displays the source as `mail`, without the `user.` prefix.
   - `active`: Retain the active-state expression.
   - `name.givenName`: Map from `givenName`.
   - `name.familyName`: Map from `surname`.

   Remove any existing `SingleAppRoleAssignment([appRoleAssignments])` mapping. You will assign roles to the synced
   groups in BuildBuddy instead.

1. Enable the **Groups** object mapping and keep only these target mappings:

   - `displayName`: Keep the default mapping and use this as the matching attribute.
   - `members`: Keep the default mapping. Entra handles group membership synchronization automatically.

   Save the mappings. Before starting provisioning, check that each Entra group you plan to sync has a unique name
   and that none of those names are already used by an existing BuildBuddy IAM group.

1. Set the provisioning scope to **Sync only assigned users and groups**. Assign the intended groups to the enterprise
   application. To restrict sign-in to assigned users, also set **Properties > Assignment required?** to **Yes**.
   Provisioning scope alone does not restrict sign-in. These assignments do not set the groups' roles in BuildBuddy.
   Entra provisions direct group members, not members of nested groups. See
   [Microsoft's assignment guidance](https://learn.microsoft.com/en-us/troubleshoot/entra/entra-id/app-integration/error-code-aadsts50105-user-not-assigned-role)
   for details, including the Global Administrator exception to the sign-in assignment requirement.

1. Start provisioning and check the provisioning logs for successful user, group, and membership synchronization.
   Once the groups appear in BuildBuddy, open **Settings > IAM Groups** and use **Edit role** to assign each synced group
   its intended role. Do not manually create same-named groups in place of syncing them.

   New users provisioned through `/scim/v2` without an explicit role receive the Reader role. Assign the synced admin
   group the Admin role and other groups the appropriate Developer, Writer, or Reader role in BuildBuddy.

Before broad rollout, verify that a pilot user can sign in with the expected permissions. Add and remove the user from
a group through scheduled provisioning. Keep the user in another assigned group during the removal test so you verify
the role change without deprovisioning the user.

**Migrating existing users:** Changing the Tenant URL or removing the per-user role mapping does not automatically
remove roles assigned directly to users. BuildBuddy combines those permissions with IAM group permissions. A direct
Developer or Admin role can therefore preserve access after removal from an IAM group. After confirming the synced
group roles, review and reduce direct user roles to the intended baseline. Keep an administrator's access throughout
the migration.

For Entra's provisioning scope and group requirements, see
[Microsoft's provisioning documentation](https://learn.microsoft.com/en-us/entra/identity/app-provisioning/use-scim-to-provision-users-and-groups)
and [group-based scoping limitations](https://learn.microsoft.com/en-us/entra/identity/app-provisioning/how-provisioning-works#assignment-based-scoping).

<details>
<summary>Legacy per-user roles</summary>

Use this workflow to keep role assignment in Entra rather than BuildBuddy IAM groups. It uses the legacy `/scim`
endpoint, not `/scim/v2`.

1. Within Entra, open the BuildBuddy application that was created for SAML integration.

1. Go to the `Provisioning` page.

1. Under the `Manage` section of side-bar, select `Provisioning`.

1. Change `Provisioning Mode` to `Automatic`.

1. Under `Admin Credentials`, enter the following information:

   1. Tenant URL: `https://app.buildbuddy.io/scim`

   1. Secret Token: Enter the value of the `Org admin key` that was created earlier.

   1. Press `Save`

1. After pressing `Save` in the previous step, you should see a new `Mappings` section. Under that section do the following:

   1. Open `Provision Microsoft Entra ID Groups` and set `Enabled` to No. Group sync is not supported by the legacy `/scim`
      endpoint. Save and return to the previous page.

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

</details>

## Organization membership requests

By default, BuildBuddy allows users to request access to an organization
using the organization's invite link or `/join/<org-slug>` route. If the
requesting user's email domain matches the organization's owned domain,
BuildBuddy may add them directly to the organization; otherwise it creates
a pending membership request for an organization admin to review.

Pending membership requests can be viewed and managed by organization
admins from either **Users** or **Settings > Members**.

Enterprise server admins can disable this self-service join flow and
require organization membership to be managed via SCIM:

```yaml title="config.yaml"
app:
  # Disable org membership requests.
  group_membership_requests_enabled: false
```

When disabled:

- The `/join/<org-slug>` route is unavailable.
- The **Invite link** UI is hidden from **Settings > Org details**.
- Pending join requests are not shown in the UI.
