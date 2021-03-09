---
id: config-misc
title: Miscellaneous Configuration
sidebar_label: Misc
---

## BuildEventProxy Section

`build_event_proxy:` The BuildEventProxy section configures proxy behavior, allowing BuildBuddy to forward build events to other build-event-protocol compatible servers. **Optional**

## Options

**Optional**

- `hosts` A list of host strings that BuildBudy should connect and forward events to.

## Example section

```
build_event_proxy:
  hosts:
    - "grpc://localhost:1985"
    - "grpc://events.buildbuddy.io:1985"
```
