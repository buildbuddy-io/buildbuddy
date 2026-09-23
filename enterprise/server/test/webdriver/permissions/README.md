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
cookies and local/session storage.

## Browser coverage

- Admin vs. Developer/Writer/Reader settings navigation and direct URLs.
- Organization API-key labels: admin-only, member-visible, and other-org keys.
- Personal API-key creation controls for each member role.
- Switching the same user between a Reader org and an Admin org, then observing
  an administrator's role downgrade without logging the browser out.
- An authorized organization edit, checked against the real database, with the
  other organization unchanged.
- Real Bazel/BEP uploads in both organizations, followed by history and direct
  invocation access checks across all four roles and an outsider.
- Cookie-authenticated invocation and build-log RPCs, plus search queries with
  substituted foreign organization IDs, using those real uploaded resources.
- Logout, anonymous access to private invocations, and outsider/anonymous access
  after making the same invocation public.

The companion HTTP RPC suite tests backend enforcement independently of hidden
controls. Its assertions should include both an authorized positive control and
unauthorized requests, and check persisted state after denied mutations.

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
bytestream downloads, workflows, remote execution cancellation, usage, audit logs,
server-admin impersonation, SAML-specific policy, and feature-flag combinations.
SQLite exercises real SQL authorization paths; it does not establish equivalence
with MySQL/PostgreSQL or ClickHouse. Add dedicated database variants where query
paths differ rather than substituting a mock database.

Screenshots and browser logs are produced by `webtester`; screenshots are stored
in Bazel's undeclared test outputs, including for named matrix subtests.
