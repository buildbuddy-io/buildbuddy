<!--
{
  "name": "Telemetry",
  "category": "5eed3e2ace045b343fc0a328",
  "priority": 150
}
-->

# Telemetry

At BuildBuddy, we collect telemetry for the purpose of helping us build a better BuildBuddy. Data about how BuildBuddy is used is collected to better understand what parts of BuildBuddy needs improvement and what features to build next. Telemetry also helps our team better understand the reasons why people use BuildBuddy and with this knowledge we are able to make better product decisions.

The telemetry data we collect is reported once per day and contains only aggregate stats like invocation counts and feature usage information. Our telemetry infrastructure is also used to report when important security updates are available. For a complete picture of the telemetry data we collect, you can see our [telemetry proto](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/telemetry.proto).

While we encourage users to enable telemetry collection and strive to be fully transparent about the data we collect to improve BuildBuddy, telemetry can be disabled at any time using the flag `--disable_telemetry=true`.
