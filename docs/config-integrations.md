---
id: config-integrations
title: Integration Configuration
sidebar_label: Integrations
---

## Section

`integrations:` A section configuring optional external services BuildBuddy can integrate with, like Slack. **Optional**

## Options

**Optional**

- `slack:` A section configuring Slack integration.

  - `webhook_url` A webhook url to post build update messages to.

## Getting a webhook url

For more instructions on how to get a Slack webhook url, see the [Slack webhooks documentation](https://api.slack.com/messaging/webhooks#getting_started).

## Example section

```
integrations:
  slack:
    webhook_url: "https://hooks.slack.com/services/AAAAAAAAA/BBBBBBBBB/1D36mNyB5nJFCBiFlIOUsKzkW"
```
