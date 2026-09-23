# Enterprise permissions tests

These tests run the **real enterprise binary**, production authentication and
permission checks, a fresh SQLite database, Redis, and local blob/cache storage.
Only the upstream identity provider is fake. No production impersonation endpoint,
mocked authorization result, or external login account is needed.

## Run

```sh
bazel test //enterprise/server/testutil/testoidc:testoidc_test \
  //enterprise/server/test/integration/permissions:permissions_test \
  //enterprise/server/test/webdriver/permissions:permissions_test \
  //enterprise/server/backends/authdb:authdb_test \
  //enterprise/server/auditlog:auditlog_test \
  //server/backends/invocationdb:invocationdb_test \
  --config=remote --test_output=errors
```

On a VM configured for BuildBuddy remote runs, replace `bazel` with `bb remote`.
The browser suite uses the existing Chromium/WebDriver Bazel infrastructure;
`--config=remote` runs it in the WebDriver image with Chromium's shared libraries.
Running it without remote execution requires those libraries on the test host.
Its explicit target is important: WebDriver suites are tagged `manual` by default.
The tests are destructive to their **disposable fixture only**; they are not
intended to run against a deployed instance or a production database. “Local-only”
means the test starts its own server, including when the whole test runs remotely.
The audit logger regression also uses a disposable real ClickHouse database; its
Bazel target selects a Docker-capable remote runner.

## Fixtures and identities

`enterprise/server/testutil/permissionstest` owns the common fixture. Each test
starts a fresh app and test OIDC provider, seeds two organizations and users, and
then logs in through the normal OIDC callback/session flow. Identities are:

| Identity | Organization A | Organization B |
| --- | --- | --- |
| admin | Admin | — |
| developer | Developer | — |
| writer | Writer | — |
| reader | Reader | — |
| outsider | — | Admin |
| dual | Reader | Admin |
| groupless | — | — |
| anonymous | Not logged in | Not logged in |

Roles describe application policy, not their everyday English meanings. In
particular, Reader is not a universal prohibition on application mutations;
Reader/Developer/Writer differ in cache capabilities.

The test OIDC provider binds each authorization code and refresh token to its
selected identity. It has its own ephemeral signing key and serves discovery and
JWKS endpoints. The app validates real signed ID tokens and creates real sessions.
The browser tests use a new WebDriver session for each identity to isolate both
cookies and local/session storage. HTTP fixture login additionally verifies the
returned user ID and email with `GetUser` in the same session, so a failed login
cannot masquerade as a role-based denial. Browser login selects the exact OIDC
subject rather than relying on native select typeahead.

## Browser coverage

- Admin vs. Developer/Writer/Reader settings navigation and direct URLs.
- Organization API-key labels: admin-only, member-visible, and other-org keys.
- Personal API-key creation controls for each member role.
- Switching the same user between a Reader org and an Admin org, then observing
  an administrator's role downgrade without logging the browser out.
- An authorized organization edit, checked against the real database, with the
  other organization unchanged.
- Real Bazel/BEP uploads using both personal and organization API keys, followed
  by history and direct invocation access checks across all four roles and an
  outsider.
- Cookie-authenticated invocation and build-log RPCs, plus search queries with
  substituted foreign organization IDs, using those real uploaded resources.
- Logout and anonymous browser/HTTP access to private invocations and logs;
  outsider/anonymous access after sharing via `UpdateInvocation`, preserving the
  existing owner and group ACL bits.

## HTTP RPC coverage

The companion integration suite checks backend enforcement independently of
hidden controls:

- Per-organization capabilities and allowed RPCs for every fixture identity,
  including a groupless user and the dual-role user.
- Organization settings and membership administration, including attempts to
  elevate one's own role and substitute another organization's ID.
- Organization key listing, creation, and both direct-read endpoints across
  Admin/Developer/Writer/Reader roles. Hidden keys are denied to non-admin user
  sessions; a key-authenticated caller can retrieve itself, but not sibling keys.
- Personal-key ownership and organization isolation, including authorized admin
  access and rejected attempts to forge another owner.
- Role downgrade and membership removal using an existing session, without
  disabling the production auth caches.
- Invocation updates and deletes by owners and other members of each role,
  including read-only ACLs and cross-org/public resources. Denied mutations leave
  rows and ACLs unchanged; public read access never grants deletion rights.
- No-cookie RPC requests with forged user/group IDs under both anonymous-usage
  settings, including absence of returned data and mutation side effects.
- Persisted database state after rejected writes.

### API-key revocation and audit attribution

A separate real-CAS test uses a personal API key before downgrade, after downgrade,
and after membership removal. It verifies actual persisted blobs: read-only CAS
writes intentionally acknowledge success without storing data, so an OK status
alone is not a sufficient assertion. This test explicitly disables
`auth.api_key_group_cache_ttl` to test enforcement on fresh DB lookups. Production
uses a five-minute cache by default; this test does **not** promise immediate
key revocation under default settings. The existing session test keeps those
production defaults unchanged.

The audit logger regression invokes the real `UpdateInvocation` handler using
hidden organization and impersonation keys, then reads real ClickHouse audit
entries and verifies their key IDs, labels, resource IDs, and mutation payloads.

The hidden-org-key direct-read regression exposed a gap in `AuthDB.GetAPIKey`:
list filtering was enforced, but direct reads previously relied on the key's
broader group-read ACL. Direct reads now enforce the member-visibility flag for
non-admins as well; personal-key owner access is unchanged. The only additional
exception is the authenticated API key's own ID, which the audit logger must be
able to resolve. This exception cannot be selected by forging request-context
identity fields and does not expose other hidden keys.

## Extending coverage

Treat this as an extensible permissions matrix, **not a claim that every app
feature is exhaustively covered**. Add each new resource/action with:

1. Explicit policy expectations independent of the production capability filter.
2. Valid data and an allowed positive control (an empty list or nonexistent ID is
   not evidence of isolation).
3. Same-org, foreign-org, owner/non-owner, and applicable anonymous cases.
4. Direct requests with forged resource IDs and request contexts, not only UI
   visibility checks. Verify status and response contents, not just any error.
5. A database read after a denied mutation to establish lack of side effects.
6. Browser assertions that wait for data loading before checking absence.

Future coverage should include ClickHouse-backed trends/executions, artifact and
bytestream downloads, workflows, remote execution cancellation, usage, broader
audit-log visibility and server-admin impersonation scenarios, SAML-specific
policy, and additional feature-flag combinations.
SQLite exercises real SQL authorization paths; it does not establish equivalence
with MySQL/PostgreSQL or ClickHouse. Add dedicated database variants where query
paths differ rather than substituting a mock database.

Screenshots and browser logs are produced by `webtester`; screenshots are stored
in Bazel's undeclared test outputs, including for named matrix subtests.
