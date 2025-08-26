---
title: Ansi terminal line clearing improvements
description: Fixed terminal line clearing issues in ANSI terminals for better display output
date: 2025-01-27
tags: ['platform']
author: Grace
---

We've resolved an issue where lines in ANSI terminals were not clearing properly, which was causing display artifacts and poor formatting in terminal output. This fix ensures that terminal clearing sequences work correctly across different terminal environments, providing cleaner and more reliable output formatting for build logs and status updates. The improvement enhances the overall developer experience when working with command-line tools by ensuring consistent terminal behavior.