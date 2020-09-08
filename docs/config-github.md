<!--
{
  "name": "Github",
  "category": "5eed3e2ace045b343fc0a328",
  "priority": 500
}
-->

# Github Configuration
In order to configure BuildBuddy's Github integration, you'll first need to [create a Github Oauth app](https://docs.github.com/en/developers/apps/creating-an-oauth-app). In the `Authorization callback URL` field - you'll need to enter your BuildBuddy application url, followed by the path `/auth/`. For example: `https://https://app.buildbuddy.io/auth/`.

## Section

```github:``` The Github section enables the posting of BuildBuddy Github commit statuses for CI runs. **Optional**

## Options

**Optional**

* ```client_id:``` The client ID of your Github Oauth App.

* ```client_secret:``` The client secret of your Github Oauth App.

## Example section

```
github:
  client_id: abc123
  client_secret: def456
```