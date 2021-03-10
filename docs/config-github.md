---
id: config-github
title: GitHub Configuration
sidebar_label: GitHub
---

In order to configure BuildBuddy's GitHub integration, you'll either need to:

- [create an access token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) with the `repo:status` scope. This is the supported method for BuildBuddy open source.
- or [create a GitHub Oauth app](https://docs.github.com/en/developers/apps/creating-an-oauth-app). In the `Authorization callback URL` field - you'll need to enter your BuildBuddy application url, followed by the path `/auth/`. For example: `https://https://app.buildbuddy.io/auth/`. This is the recommended method for BuildBuddy Enterprise.

## Section

`github:` The GitHub section enables the posting of BuildBuddy GitHub commit statuses for CI runs. **Optional**

## Options

**Optional**

- `access_token:` The GitHub access token used to post GitHub commit statuses.

- `client_id:` The client ID of your GitHub Oauth App. [ENTERPRISE ONLY]

- `client_secret:` The client secret of your GitHub Oauth App. [ENTERPRISE ONLY]

## Example section

Open source with access token:

```
github:
  access_token: abc123
```

Enterprise with Oauth app:

```
github:
  client_id: abc123
  client_secret: def456
```
