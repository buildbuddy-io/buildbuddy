---
id: config-org
title: Organization Configuration
sidebar_label: Org
---

Your organization is only configurable in the [Enterprise version](enterprise.md) of BuildBuddy.

## Section

`org:` The org section allows you to configure your BuildBuddy organization. **Optional**

## Options

**Optional**

- `name:` The name of your organization, which is displayed on your organization's build history.

- `domain:` Your organization's email domain. If this is set, only users with email addresses in this domain will be able to register for a BuildBuddy account.

## Example section

```
org:
  name: Acme Corp
  domain: acme.com
```
