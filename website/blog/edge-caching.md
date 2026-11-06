---
slug: edge-caching
title: Edge Caching
description: How BuildBuddy edge caches save customers time and money by moving their data closer to them.
authors: iain
date: 2026-01-05:08:00:00
image: /img/edge-caching.png
tags: [engineering, bazel, cache, cloud]
---

# BuildBuddy Cloud

Cloud providers have made it easier than ever to deploy, manage, and scale software that runs on machines all over the world. One downside is that these providers like to charge for data leaving their cloud, or "egress." Typically they charge a few (5-8) cents per gigabyte sent to the internet, plus another cent for data going through a load balancer, and another fraction of a cent for the processing done on the load balancer, oh and there's another cent or two for replicating data across availability zones, and those availability zones aren't _always_ "available" so you really do need replication. This all adds up, and it incentivizes customers to colocate their data storage and processing in one provider's cloud. So, what do you do when you run a giant cache storing build and test artifacts and you want to serve customers using every cloud provider all over the world? You edge cache.

<!-- truncate -->

# Edge Caching

Edge Caching is the practice of running small cache deployments close to users to reduce slow and expensive traffic that might otherwise, for example, cross an ocean. There are some companies whose entire business is edge caching for their customers. Fortunately for us, Bazel actually provides a pretty natural edge caching API in the `ActionCache` (AC) and `ContentAddressableStorage` (CAS). Both of these RPC services have read and write operations, and the CAS has a contains operation (`FindMissingBlobs`). The BuildBuddy edge cache is a small binary containing read-through versions of both of these RPC services with some fanciness sprinkled on top to handle all the things that go wrong if some Bazel requests never reach the remote cache.

Our edge caches serve a few main purposes. First, they serve our remote execution clusters with lower latency, reduce the load on our app servers from these clusters, and help us avoid "thundering herd" problems when auto-scaling executor pools. Second, they reduce network egress and lantecy serving customer requests that originate from other cloud providers and regions. Finally, they let our customers avoid paying network egress costs they would incur sending bytes (for writes) to us. We've run these edge caches for almost a year now and they actually do a pretty good job of addressing these issues! But as usual, there's some cool engineering going on behind the scenes.

# Challenges

## Auth

All of our authentication and authorization logic runs in the BuildBuddy app servers and depends on data stored in an SQL database that's colocated with the app servers. We don't really want to be in the business of replicating that database anywhere and everywhere we want to run edge caches, so the edge caches authenticate by forwarding incoming HTTP/RPC headers (including the API key) to the apps in a special `Auth` RPC. The `Auth` RPC returns a JWT containing information about the authenticated user and group, if successful, which the edge caches validate and then use locally for all of the remaining authentication and authorization checks. We keep these locally in the edge caches to avoid too many expensive roundtrip requests to the apps for incoming requests with identical auth headers.

## Cache Consistency

We all know the old saying, cache consistency is one of the hard problems in computer science. For edge caching, the risk is that an artifact exists in the edge cache but not in the authoritative cache, leading to an inconsistent view of the cache across clusters. We solve this problem by 1. always serving `ContentAddressableStorage.FindMissingBlobs` out of the authoritative cache and 2. asynchronously refreshing the authoritative cache's blob access times for requests served out of the edge caches. This design ensures that clients have a consistent view of the cache via `FindMissingBlobs` and that recently accessed blobs are always refreshed in the authoritative cache, to prevent eviction. The Bazel client doesn't do reads without first doing an existence check, so we don't need to check the authoritative cache before serving CAS or AC reads.

TODO(iain): is the above thing about blind reads true?

## Action Cache Consistency

If you really know your stuff, you may have noticed a problematic corner case above: CAS artifacts are... well, content-addressable. The hash of a CAS artifact uniquely identifies the artifact, and if the underlying data changes, the hash changes. The only cache-consistency issue with CAS artifacts is whether they're present or not. AC artifacts are not content-addressable. The same AC hash key could point to action result `foo` for a while and then be updated to point to action result `bar` and that update needs to be reflected in the global view of the cache.

At first, we solved this problem by always serving AC requests out of the authoritative cache. That worked fine, but it means that customers with very high AC hit rates did a lot of network traffic between the edge and authoritative caches. To fix this, we added an expected digest in the `GetActionResult` request that the edge cache populates with the digest it has for the AC result. This lets the app server only respond with full AC results that have changed, saving a bit of network bandwidth between clusters.

## Hit-Tracking

BuildBuddy's cache scorecard and usage tracking rely on tallying up cache requests served for an invocation before a finalization event is sent. This is all done in the apps, because that's where all the scorecard and usage data is ultimately stored. But that means these features don't work for reads served out of the edge caches. To fix this, we asynchronously report all digests read locally from edge caches back to the app. Unlike the cache consistency RPCs, which need to work most of the time, this tracking really needs to be correct, so we take additional steps to ensure these hit-tracking RPCs are always sent, like flushing the hit-tracking queue much more aggressively.

## Tree Cache

We use Bazel's `GetTree` API for fetching directories. Not all remote cache implementations do because Bazel represents directories using Merkle Trees which are notoriously difficult to cache. We already play a bunch of tricks to improve the cacheability of these Merkle Trees that probably warrant an entire other blog post, and fortunately these all operate on the CAS, so they're easily re-used in our edge caches.

# Conclusion

Dropbox guy
