---
title: Fixed ANSI terminal line clearing issues
description: Resolved display problems where terminal output lines weren't clearing properly in ANSI terminals
date: 2024-12-19
tags: ['platform']
author: Zoey
---

We've addressed an issue where lines in ANSI terminals weren't clearing properly, which could lead to garbled or overlapping output in terminal displays. This fix improves the reliability of terminal output formatting, ensuring that build logs and command output render correctly across different terminal environments. The change enhances the overall user experience when viewing build results and debugging output in terminal-based workflows.