---
id: config-telemetry
title: Telemetry
sidebar_label: Telemetry
---

At BuildBuddy, we collect telemetry for the purpose of helping us build a better BuildBuddy. Data about how BuildBuddy is used is collected to better understand what parts of BuildBuddy needs improvement and what features to build next. Telemetry also helps our team better understand the reasons why people use BuildBuddy and with this knowledge we are able to make better product decisions.

The telemetry data we collect is reported once per day and contains only aggregate stats like invocation counts and feature usage information. Our telemetry infrastructure is also used to report when important security updates are available. For a complete picture of the telemetry data we collect, you can see our [telemetry proto](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/telemetry.proto).

We also use Google Analytics to collect pseudonymized usage data about how users are using the BuildBuddy product and how well it is performing. This data is analyzed and used to improve the BuildBuddy product.

While we encourage users to enable telemetry collection and strive to be fully transparent about the data we collect to improve BuildBuddy, telemetry can be disabled at any time using the flag `--disable_telemetry=true`. Google Analytics can also optionally be disabled using the flag `--disable_ga=true`.
