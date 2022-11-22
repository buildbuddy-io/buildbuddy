---
slug: clickhouse-build-trends
title: "How We Use ClickHouse to Analyze Trends Across Millions of Builds"
author: Lulu Zhang
author_title: Engineer @ BuildBuddy
date: 2022-11-22:12:00:00
author_url: https://www.linkedin.com/in/luluzhang66
author_image_url: https://avatars.githubusercontent.com/u/3977083?v=4
image: /img/blog/clickhouse.png
tags: [product, engineering]
---

When you use Buildbuddy with Bazel to build and test software, Buildbuddy
captures information about each Bazel invocation, such as number of builds, build
duration, remote cache performance, and more. Buildbuddy has
a [Trends](https://app.buildbuddy.io/trends/) page to visualize trends in this
data over time.

![](/img/blog/trends-2.png)

The trends page allows you to see how improvements you are making to your builds
affects your average build duration and other stats. It also exposes areas that
might need improving. For example, if you see the cache hit rate go down over
time, your build might have some non-deterministic build actions that could be
improved, or some newly introduced dependencies that result in more frequent
cache invalidations.

When we first created the Trends page, we used MySQL queries to aggregate
build stats and generate the data we wanted to display. For a time this worked
well, but we quickly ran into performance issues for customers that had very
large numbers of builds. We were able to temporarily improve performance by
adding various indices, and though this helped to reduce the number of rows
read, it was not sufficient. Some customers do millions of builds monthly, and the
Trends page (which can look back up to a year) for these customers was taking
more than 20 _minutes_ to load.

The queries behind the trends page require aggregation of multiple columns, such
as cache hits and cache misses. A traditional row-based database like MySQL is
not always ideal for such a use case. In row-based databases, data is stored row
by row. When aggregating columns, more I/O seeks are required than
a column-based database, which stores the data of each column in contiguous
blocks. Moreover, column-based databases have a higher compression rate because
consecutive values of the same column are of the same type and may repeat.

![](/img/blog/row-column-datastore.png)

With a row-based store, we can see from this diagram that computing a sum of
cache hit count would require us to load both block 1 and block 2. With
a column-based store, all the cache hits data are stored in the same block.

Therefore, we felt that using ClickHouse, a column-based database, would improve
the performance of required queries for the trends page. We validated
ClickHouse’s performance against our use case: it took ClickHouse 0.317 seconds
to process 1.5 million rows and calculate the stats. The same query took MySQL
about 24 minutes.

One of our goals for data migration is to make sure the data is accurate. We
added monitoring and compared data between MySQL and ClickHouse after we enabled
double writing in production. One source of inconsistency was that data was
inserted into ClickHouse both by the backfill script and production servers.
Different to a traditional database, ClickHouse’s
[ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree/)
engine only deduplicates data in the background at an indeterminate time. As
a result, we needed to manually run the
[OPTIMIZE](https://clickhouse.com/docs/en/sql-reference/statements/optimize/)
operation to force ClickHouse to deduplicate data after the backfill was done.
After we were confident in the data consistency, we finally enabled the Trends
page to read from ClickHouse.

## What's next

We are excited how ClickHouse unlocks more possibilities for us to provide
analytical insights into builds, targets, tests and remote execution. For
example, we want to add graphs that show how remote actions are spending most of
their time. These insights can be used to guide remote execution performance
optimizations.

We would love to hear your feedback about what stats and graphs you are interested in seeing.
Join our [Slack channel](https://slack.buildbuddy.io) or email us at
<hello@buildbuddy.io> with any questions, comments, or thoughts.
