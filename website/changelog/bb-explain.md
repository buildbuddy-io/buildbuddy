---
title: `bb explain`: understand why your build re-ran actions
date: 2026-03-09:11:00:00
author: fabian
---

`bb explain` shows a structural diff of two compact execution logs. It helps answer the common questions: "Why did Bazel re-execute that action?" and "What changed between these builds?"

![bb explain](../static/img/changelog/bb-explain.png)

It can highlight non-hermetic outputs and differences in inputs, environment variables, and arguments.

It works with your last two builds automatically, or you can point it at any invocation IDs:

```
bb explain
bb explain --old <OLD_INVOCATION_ID> --new <NEW_INVOCATION_ID>
```

You can download the `bb` CLI [here](https://www.buildbuddy.io/cli/).
More details in [Fabian's BazelCon talk](https://www.youtube.com/watch?v=jsIzSkaUcx8).
