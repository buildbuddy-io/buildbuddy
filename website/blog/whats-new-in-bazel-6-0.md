---
slug: whats-new-in-bazel-6-0
title: "What's New in Bazel 6.0"
description: We reviewed over 3,100 commits and summarized them, again 😅, so you don't have to!
author: Brentley Jones
author_title: Developer Evangelist @ BuildBuddy
date: 2022-12-19:9:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/bazel_6_0.png
tags: [bazel]
---

[Bazel 6.0][bazel-6-0] includes [over 3,100 changes][diff] since 5.4.
It's the latest major release,
following the release of 5.0 in January of this year,
and it's Bazel's [third LTS release][lts-releases].
Since there were so many changes,
many of them quite impactful,
I felt I needed to review them all and provide a nice summary for y'all.

[bazel-6-0]: https://blog.bazel.build/2022/12/19/bazel-6.0.html
[diff]: https://github.com/bazelbuild/bazel/compare/5.4.0...6.0.0
[lts-releases]: https://bazel.build/versions/6.0.0/release/versioning#lts-releases

<!-- truncate -->

Similar to [Bazel 5.0's changes][whats-new-in-bazel-5],
the end result was quite big,
so I've included a table of contents to allow easy navigation to the changes that interest you the most:

<nav class="toc">

- [Command-line flag changes](#command-line-flag-changes)
  - [Renamed](#renamed)
  - [Default values changed](#default-values-changed)
  - [Deprecated](#deprecated)
  - [No-op](#no-op)
  - [Removed](#removed)
- [Remote](#remote)
  - [Remote caching (RBC)](#remote-caching-rbc)
  - [Remote execution (RBE)](#remote-execution-rbe)
  - [Dynamic execution](#dynamic-execution)
  - [Local execution](#local-execution)
  - [Build Event Service (BES)](#build-event-service-bes)
- [Logging](#logging)
  - [Build Event Protocol (BEP)](#build-event-protocol-bep)
  - [Timing profile](#timing-profile)
  - [Execution log](#execution-log)
- [Query](#query)
  - [cquery](#cquery)
  - [aquery](#aquery)
- [Dependency management](#dependency-management)
- [Platforms and toolchains](#platforms-and-toolchains)
- [Execution platforms](#execution-platforms)
  - [Linux](#linux)
  - [macOS](#macos)
- [Target platforms](#target-platforms)
  - [Android](#android)
  - [Apple](#apple)
- [Languages](#languages)
  - [C and C++](#c-and-c%2B%2B)
  - [Java](#java)
  - [Objective-C](#objective-c)
  - [Python](#python)
- [Rules authoring](#rules-authoring)
  - [Aspects](#aspects)
  - [Persistent workers](#persistent-workers)
  - [Starlark](#starlark)
- [Misc](#misc)

</nav>

[whats-new-in-bazel-5]: whats-new-in-bazel-5-0.md

## Command-Line Flag Changes

Bazel's [LTS strategy][lts-releases] allows for breaking changes between major versions.
In particular,
it allows for command-line flags to be removed,
renamed,
made to do nothing,
or have their default values changed.
In the following sections I collected all such flag changes I could find.

### Renamed

- [`--experimental_build_transitive_python_runfiles` is now `--incompatible_build_transitive_python_runfiles`.](https://github.com/bazelbuild/bazel/commit/36afffa04151d9243051f83897c88257ab4d1026)
- [`--experimental_debug_spawn_scheduler` is now `--debug_spawn_scheduler`.](https://github.com/bazelbuild/bazel/commit/e38c73f8ecc327d54e0409892468ad1bec6e4a49)
- [`--experimental_desugar_java8_libs` is now `--desugar_java8_libs`.](https://github.com/bazelbuild/bazel/commit/e38c73f8ecc327d54e0409892468ad1bec6e4a49)
- [`--experimental_enable_bzlmod` is now `--enable_bzlmod`.](https://github.com/bazelbuild/bazel/commit/f106d5c1dc4d84e119537dda3d68bc2dd83e2077)
- [`--experimental_local_execution_delay` is now `--dynamic_local_execution_delay`.](https://github.com/bazelbuild/bazel/commit/e38c73f8ecc327d54e0409892468ad1bec6e4a49)
- [`--experimental_worker_max_multiplex_instances` is now `--worker_max_multiplex_instances`.](https://github.com/bazelbuild/bazel/commit/e38c73f8ecc327d54e0409892468ad1bec6e4a49)

### Default Values Changed

- [`--analysis_testing_deps_limit=1000`](https://github.com/bazelbuild/bazel/commit/21dfe4cdc35ed0b3536accdc91be042aa5c550aa)
- [`--experimental_allow_unresolved_symlinks=true`](https://github.com/bazelbuild/bazel/commit/3d5c5d746b286c840ba5cfd437d93d8d11995e02)
- [`--experimental_keep_config_nodes_on_analysis_discard=true`](https://github.com/bazelbuild/bazel/commit/60523c7fecd4e72490c2dde547e1e36eab5a79ef)
- [`--experimental_collect_local_sandbox_action_metrics=true`](https://github.com/bazelbuild/bazel/commit/60523c7fecd4e72490c2dde547e1e36eab5a79ef)
- [`--experimental_incremental_dexing_after_proguard=50`](https://github.com/bazelbuild/bazel/commit/ce55639c3ef2b9bd703d64026c40df0b7485b6a5)
- [`--experimental_incremental_dexing_after_proguard_by_default=true`](https://github.com/bazelbuild/bazel/commit/ce55639c3ef2b9bd703d64026c40df0b7485b6a5)
- [`--experimental_inmemory_dotd_files=true`](https://github.com/bazelbuild/bazel/commit/d44f11be11f3ec12d644eb5f5245bf70c6a65bee)
- [`--experimental_inmemory_jdeps_files=true`](https://github.com/bazelbuild/bazel/commit/822e049d2881a1c7bedc9182116f772a72e00227)
- [`--experimental_keep_config_nodes_on_analysis_discard=false`](https://github.com/bazelbuild/bazel/commit/2f9f8429be9651d4bb94b425b6ae5f11e95bea16)
- [`--experimental_use_dex_splitter_for_incremental_dexing=true`](https://github.com/bazelbuild/bazel/commit/ce55639c3ef2b9bd703d64026c40df0b7485b6a5)
- [`--incompatible_always_include_files_in_data=true`](https://github.com/bazelbuild/bazel/commit/0caf488a7492740425af88b32c622fdc33bc1593)
- [`--incompatible_enforce_config_setting_visibility=true`](https://github.com/bazelbuild/bazel/commit/aad2db20b5)
- [`--incompatible_existing_rules_immutable_view=true`](https://github.com/bazelbuild/bazel/commit/74b7dd55325d6588f1a8827dd3bdb30deea073a0)
- [`--incompatible_remote_results_ignore_disk=true`](https://github.com/bazelbuild/bazel/commit/4c56431c271850f7536aae0a0719f811e3c35b5b)
- [`--incompatible_use_platforms_repo_for_constraints=true`](https://github.com/bazelbuild/bazel/commit/f137e640303486b52e39b4d4edee088f895b6b00)
- [`--use_top_level_targets_for_symlinks=true`](https://github.com/bazelbuild/bazel/commit/6452024a0106ab901f38027db65e8ab831201288)
- [`--use_workers_with_dexbuilder=true`](https://github.com/bazelbuild/bazel/commit/0c3f5280e0746b9cbf222b426d3885658e525b0b)

### Deprecated

- [`--allowed_local_actions_regex`](https://github.com/bazelbuild/bazel/commit/16f2eecc3c2406d9de0cfc11f8187cb68df12b51)
- [`--experimental_spawn_scheduler`](https://github.com/bazelbuild/bazel/commit/da6f8026967e2338973306c9d85ae6cf23244ecb)

### No-op

These flags now do nothing, but still exist to allow for migration off of them:

- [`--distinct_host_configuration`](https://github.com/bazelbuild/bazel/commit/78d0fc9ff1d2f22005ddfce43384e35fbac338cb)
- [`--dynamic_worker_strategy`](https://github.com/bazelbuild/bazel/commit/db64e7e54b7f17e86a3206b4834f0fd11c065155)
- [`--experimental_allow_top_level_aspects_parameters`](https://github.com/bazelbuild/bazel/commit/bccbcbf9767ebd081df464db78c582d25173115b)
- [`--experimental_dynamic_skip_first_build`](https://github.com/bazelbuild/bazel/commit/db64e7e54b7f17e86a3206b4834f0fd11c065155)
- [`--experimental_keep_config_nodes_on_analysis_discard`](https://github.com/bazelbuild/bazel/commit/75bb463ab73f5f7cc60e7cc445ba24b377f4963f)
- [`--experimental_multi_cpu`](https://github.com/bazelbuild/bazel/commit/85bfefedeef20063fb6d17a73d9ee18a028414f7)
- [`--experimental_skyframe_prepare_analysis`](https://github.com/bazelbuild/bazel/commit/f22e63deef43141f08b63cc0210fc9b9e2fded1d)
- [`--incompatible_disable_managed_directories`](https://github.com/bazelbuild/bazel/commit/cbf8159ba6190ab447ad54df63cb4db25763d755)
- [`--incompatible_disable_third_party_license_checking`](https://github.com/bazelbuild/bazel/commit/0aa750b7252ef8c71e11ae2f6cf6849b6ff0e715)
- [`--incompatible_disallow_legacy_py_provider`](https://github.com/bazelbuild/bazel/commit/f068b31c94eef1ea10477a755d9173b4fadf6485)
- [`--incompatible_override_toolchain_transition`](https://github.com/bazelbuild/bazel/commit/2d5375c27a4ca43635183bbe0d12849c64a6176e)

### Removed

- [`--all_incompatible_changes`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--analysis_warnings_as_errors`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--default_android_platform`](https://github.com/bazelbuild/bazel/commit/ed7a0565f02b496bc4fc613157111638e8dd997f)
- [`--experimental_delay_virtual_input_materialization`](https://github.com/bazelbuild/bazel/commit/c887c2a4fe4395f9663ce2f1cde9b656d216415c)
- [`--experimental_enable_cc_toolchain_config_info`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--experimental_local_memory_estimate`](https://github.com/bazelbuild/bazel/commit/361ce673ad2b959b73e859a121ff2e996feb561b)
- [`--experimental_persistent_test_runner`](https://github.com/bazelbuild/bazel/commit/1f33504af3ebe3a43aa91e607e94edc07b1807f8)
- [`--experimental_profile_cpu_usage`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--experimental_required_aspects`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--experimental_shadowed_action`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--experimental_skyframe_eval_with_ordered_list`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--experimental_worker_allow_json_protocol`](https://github.com/bazelbuild/bazel/commit/09df7c0a14b9bf13d4aa18f5a02b4651e626d5f4)
- [`--extra_proguard_specs`](https://github.com/bazelbuild/bazel/commit/ab51d2e45378d6ec23bce8b5b40d632364d77dbb)
- [`--forceJumbo`](https://github.com/bazelbuild/bazel/commit/9cb551c0e540090679448460ee19b04b0f281f1d)
- [`--incompatible_applicable_licenses`](https://github.com/bazelbuild/bazel/commit/29e4aee112e8649c93577ec225c7ed9fdedd76a2)
- [`--incompatible_disable_depset_items`](https://github.com/bazelbuild/bazel/commit/bf30d81caf907e0fa58e71b63924d81a1271c40a)
- [`--incompatible_disable_late_bound_option_defaults`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--incompatible_disable_legacy_proto_provider`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--incompatible_disable_proto_source_root`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--incompatible_do_not_emit_buggy_external_repo_import`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--incompatible_enable_exports_provider`](https://github.com/bazelbuild/bazel/commit/01a46f05dc79db2313c6c8e174a5d6eab474aefc)
- [`--incompatible_linkopts_to_linklibs`](https://github.com/bazelbuild/bazel/commit/34ce6a23f5a2be58bb59661dd5fc9ab586ea1703)
- [`--incompatible_proto_output_v2`](https://github.com/bazelbuild/bazel/commit/6b6c63ed9fb5d51a90b91dca4bed5a68955a6859)
- [`--incompatible_use_native_patch`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--ios_cpu`](https://github.com/bazelbuild/bazel/commit/684fb0a576827d4f5c7311f3ac13b59d0786ea04)
- [`--json_trace_compression`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)
- [`--master_bazelrc`](https://github.com/bazelbuild/bazel/commit/96c8a9073807c9e97635ddafe2ed0365a9318d6f)
- [`--max-bytes-wasted-per-file`](https://github.com/bazelbuild/bazel/commit/e846207dc2495e39fd178218b29bab3b4a8cd1d1)
- [`--minimal-main-dex`](https://github.com/bazelbuild/bazel/commit/63ddfc468d5b8a5c5f4ead28997dd4412fbfbb76)
- [`--remote_allow_symlink_upload`](https://github.com/bazelbuild/bazel/commit/4f557e8af473d26a55265ada7c4c950b7bef9b35)
- [`--set-max-idx-number`](https://github.com/bazelbuild/bazel/commit/e846207dc2495e39fd178218b29bab3b4a8cd1d1)
- [`--show_task_finish`](https://github.com/bazelbuild/bazel/commit/2e48994ab8202796324df3c93ff9441a44b5ba4d)
- [`--ui`](https://github.com/bazelbuild/bazel/commit/8e6c469106261c765e929532ee7d71fee71c7abc)

## Remote

One of Bazel's most powerful features is its ability to use [remote caching and remote execution][remote-explained].
Numerous improvements and fixes to Bazel's remote capabilities are included in Bazel 6.0.

[remote-explained]: bazels-remote-caching-and-remote-execution-explained.md

### Remote caching (RBC)

Using a remote cache is one of the most popular ways of speeding up a Bazel build.
Thankfully these changes make using a remote cache both more performant and more reliable:

- [Added the `--experimental_remote_downloader_local_fallback` flag, which causes the remote downloader to fallback to normal fetching.](https://github.com/bazelbuild/bazel/commit/7b141c1edf243acae5859f56cd0afff2d3eaba32)
- [Added the `--incompatible_remote_downloader_send_all_headers` flag, which causes all values of a multi-value header to be sent with Remote Downloader.](https://github.com/bazelbuild/bazel/commit/b750f8c0242d7fcb581d368d8b75e59c51c13a61)
- [The `--experimental_action_cache_store_output_metadata` flag is now enabled by default when using the `--remote_download_*` flags.](https://github.com/bazelbuild/bazel/commit/e5f92a40338ffe0f50f817ee1b2b6e3557d380e2)
- [Outputs downloaded with `--experimental_remote_download_regex` no longer block spawn execution.](https://github.com/bazelbuild/bazel/commit/e01e7f51dd19f39ce3bc0718cec20ed6474de733)
- [`--experimental_remote_download_regex` now matches on execution root relative paths.](https://github.com/bazelbuild/bazel/commit/bb8e6eca7a8698b2bb8216160038a36e55e69da7)
- [Top-level outputs are now always downloaded when using `--remote_download_toplevel`.](https://github.com/bazelbuild/bazel/commit/b6f3111d8e460de35a5e5570bd784919fc30c5f8)
- [`bazel run` now works with `--remote_download_minimal`.](https://github.com/bazelbuild/bazel/commit/845077fff18432e6b2a1a089964e1afc9b9fec7f)
- [When using Build without the Bytes, AC results in the disk cache are now validated to be complete.](https://github.com/bazelbuild/bazel/commit/21b0992eb26e91ad3e6d3046fa90f1f7ad8b9f08)
- [Outputs and stdErr are now always downloaded.](https://github.com/bazelbuild/bazel/commit/b303cd128d9f1d913749927f4a1cd942b10b1ae2)
- [Symlinked targets are no longer unnecessarily downloaded.](https://github.com/bazelbuild/bazel/commit/32b0f5a258ee83b040d6975a6bd0795670f5cd47)
- [Downloads now use a priority system to improve critical path performance.](https://github.com/bazelbuild/bazel/commit/05b97393ff7604f237d9d31bd63714f27fbe83f2)
- [HTTP remote caches now support client TLS authentication.](https://github.com/bazelbuild/bazel/commit/aaf65b97b7cd75160b4b4dcc9149fb9fc60e1759)
- [Headers are no longer included in `FetchBlobRequest`.](https://github.com/bazelbuild/bazel/commit/7aa69a968b9063cb080417426a11bb32954f7f6c)
- [AC uploads will no longer error if `--remote_upload_local_results=true` and `GetCapabilities` returns `update_enabled=false`.](https://github.com/bazelbuild/bazel/commit/f589512846c5762e89757b96835ee54da4bb2281)
- [Fixed hanging when failing to upload action inputs.](https://github.com/bazelbuild/bazel/commit/8b61746cf18d3d8413b569e0b5fca639d28914ea)

### Remote execution (RBE)

For some projects,
using remote execution is the ultimate performance unlock for their Bazel builds.
In addition to the remote caching changes covered above,
which also apply to remote execution,
the following changes improve the remote execution experience:

- [Added the `--experimental_remote_mark_tool_inputs` flag, which marks tool inputs for remote executors, which in turn allows them to implement remote persistent workers.](https://github.com/bazelbuild/bazel/commit/72b481a8e9c19de1acf323c69d2d822c954a6dbb)
- [Added the `--incompatible_remote_dangling_symlinks` flag, which allows symlinks in action outputs to dangle.](https://github.com/bazelbuild/bazel/commit/5b46a48db8b380ba9afe2df2cb7d564b0c927913)
- [Added the `--remote_print_execution_messages` flag, which allows control over when to print remote execution messages.](https://github.com/bazelbuild/bazel/commit/8b57c5810f2cc58f71995be83604d47226cf9244)
- [Added the `exclusive-if-local` tag, for disabling parallel local test execution but allowing remote execution if requested.](https://github.com/bazelbuild/bazel/commit/8936828610db8106864e41860ad86c5c415fa0ed)
- [Added support for remotely generating unresolved symlinks.](https://github.com/bazelbuild/bazel/commit/ca95fecde07a28736ea815ec64bcd639a234d79c)
- [Fixed formatting of non-ASCII text.](https://github.com/bazelbuild/bazel/commit/c55b01e3e4c535738f9aebbb4d1ba0623235aee0)

### Dynamic execution

[Dynamic execution][dynamic-execution] allows for Bazel to race remote and local execution of actions,
potentially allowing you to get the benefit of both modes,
without the drawbacks of either.
Bazel 6.0 included a number of changes to dynamic execution,
almost all of them behind new experimental flags:

- [Added the `--experimental_dynamic_exclude_tools` flag, which prevents dynamic execution for tools.](https://github.com/bazelbuild/bazel/commit/9a52a270132250b11523db055e13e69791aecba1)
- [Added the `--experimental_dynamic_local_load_factor` flag, which controls how much load from dynamic execution to put on the local machine.](https://github.com/bazelbuild/bazel/commit/d5c4f551fa9f5981b36ab67c69386e3787d06f0c)
- [Added the `--experimental_dynamic_slow_remote_time` flag, which starts locally scheduling actions above a certain age in dynamic scheduling.](https://github.com/bazelbuild/bazel/commit/88f605c689881f50e8ec310f43878e411936f23c)
- [Added the `--experimental_prioritize_local_actions` flag, which prioritizes local-only actions over dynamically executed actions for local resources.](https://github.com/bazelbuild/bazel/commit/8fa3ccbb072236a8d0a4937e9e4a0ced58268c25)

[dynamic-execution]: https://bazel.build/versions/6.0.0/remote/dynamic

### Local execution

In `buck2`,
[local execution is a specialization of remote execution][buck2-local-execution].
So I'm going to use that as the reason that this section is listed under the "Remote" section,
and not because I don't want to reorganize this post just to fit in a couple neat local execution changes 😉:

- [Announced at BazelCon][skymeld-bazelcon], SkyMeld allows merging the analysis and execution phases with the the [`--experimental_merged_skyframe_analysis_execution` and `--experimental_skymeld_ui` flags](https://github.com/bazelbuild/bazel/issues/14057).
- [Local actions now fetch inputs before acquiring the resource lock.](https://github.com/bazelbuild/bazel/commit/17276d4fade793955a6f2491e3527c2be184867a)

[buck2-local-execution]: https://github.com/facebookincubator/buck2/blob/2116ae1d48d63109d72cb7100cb99ad8d20bc873/docs/why.md#why-might-it-be-interesting
[skymeld-bazelcon]: https://youtu.be/IEJLHNNRP9U

### Build Event Service (BES)

Using a build event service can give you unparalleled insight into your Bazel builds at scale.
There were some nice changes to BES support,
though I think the improvements to how it interacts with the remote cache are especially noteworthy.

- [Added the `--bep_maximum_open_remote_upload_files` flag, which allows control over the maximum number of open files allowed during BEP artifact upload.](https://github.com/bazelbuild/bazel/commit/46104c6948dc4ca66797e413c62b58bd21981c51)
- [Added the `--bes_check_preceding_lifecycle_events` flag, which tells BES to check whether it previously received `InvocationAttemptStarted` and `BuildEnqueued` events matching the current tool event.](https://github.com/bazelbuild/bazel/commit/14b5c41c29423866cd3f2ee3f7b69ff48241bd34)
- [Added the `--experimental_build_event_upload_max_retries` and `--experimental_build_event_upload_retry_minimum_delay` flags, which allow for configuring the behavior of BES uploader retires.](https://github.com/bazelbuild/bazel/commit/e7218d556a2a265183a10a19fcaa21c0277820ad)
- [Added the `--experimental_remote_build_event_upload` flag, which controls the way Bazel uploads files referenced in BEP to remote caches.](https://github.com/bazelbuild/bazel/commit/6b5277294bfcf032d1d2d009c102147af12ec896)
- [BES RPC calls now include request metadata.](https://github.com/bazelbuild/bazel/commit/dbcf260e0318ef553d606b0510832cfe8a5b14eb)

## Logging

Bazel offers various methods to gain insight into your build.
I cover some of the notable changes to those methods below.

### Build Event Protocol (BEP)

The build event protocol is used by [build event services](<#build-event-service-(bes)>),
so all of these changes could have also been listed in that section as well.
The BEP can also be collected locally with [`--build_event_json_file`][build_event_json_file] and [`--build_event_binary_file`][build_event_binary_file].

The vast majority of changes added additional information to the BEP,
though some are fixes and improvements:

- [Added the `digest` and `length` fields to `File`.](https://github.com/bazelbuild/bazel/commit/da6d9499d61ed4fb14a0bbbf456d5e9381c328ee)
- [Added the `BuildMetrics.network_metrics` field.](https://github.com/bazelbuild/bazel/commit/9bc9096241fb414303cafecd36a993f45117f3e1)
- [Added the `MemoryMetrics.peak_post_gc_tenured_space_heap_size` field.](https://github.com/bazelbuild/bazel/commit/a9ac2b6162234c4c1d0db37a6d7b783763d0f79f)
- [Added the `WorkerMetrics.last_action_start_time_in_ms` field.](https://github.com/bazelbuild/bazel/commit/d233c896fd6eff93901f7dd8641936a76b544ccb)
- [Changed the semantics of `build_event_stream.BuildMetrics.PackageMetrics.packages_loaded` to be the number of packages successfully loaded.](https://github.com/bazelbuild/bazel/commit/d8c25fcced761cb139e560076d24d9f179055548)
- [Deprecated `AnomalyReport`.](https://github.com/bazelbuild/bazel/commit/286fb80081db0af43b1f86292ce417c6541d4ad4)

[build_event_binary_file]: https://bazel.build/versions/6.0.0/reference/command-line-reference#flag--build_event_binary_file
[build_event_json_file]: https://bazel.build/versions/6.0.0/reference/command-line-reference#flag--build_event_json_file

### Timing profile

The action timing profile,
which is enabled by default with [`--profile`][profile],
is viewable both [locally in Chrome][performance-profiling] and on [build event services](<#build-event-service-(bes)>).
These changes add more detail and clarity to the profile:

- [Added action mnemonics.](https://github.com/bazelbuild/bazel/commit/e78fd2e5f4cfc4029d4725d764eca0b1abff164b)
- [Added the `--experimental_collect_load_average_in_profiler` flag, which adds the system's overall load average.](https://github.com/bazelbuild/bazel/commit/b4dbed0c599862cc3886fc644cda95d0a38b4f70)
- [Added the `--experimental_collect_system_network_usage` flag, which adds system network usage.](https://github.com/bazelbuild/bazel/commit/e382cb2fd7b02e557a513f93a58eafd42ab234b0)
- [Added the `--experimental_collect_worker_data_in_profiler` flag, which adds worker memory usage.](https://github.com/bazelbuild/bazel/commit/be5354b4b4a27d34badbce0c6540b879c84fd865)
- [Improved reporting of critical path components.](https://github.com/bazelbuild/bazel/commit/3d2bb2a400c48bc51e43db11ac154d999a97881b)
- [Made the `sort_index` value always a string.](https://github.com/bazelbuild/bazel/commit/081f831e3c86b4a4c6d6107b29c4883581b94aee)

[performance-profiling]: https://bazel.build/versions/6.0.0/rules/performance#performance-profiling
[profile]: https://bazel.build/versions/6.0.0/reference/command-line-reference#flag--profile

### Execution log

Bazel logs all of the [spawns][spawns] it executes in the execution log,
which is enabled with the [`--execution_log_json_file`][execution_log_json_file] or [`--execution_log_binary_file`][execution_log_binary_file] flags.
This feature is relatively stable,
with just a few noticeable additions:

- [Added the `--experimental_execution_log_spawn_metrics` flag, which causes spawn metrics in be included in the execution log.](https://github.com/bazelbuild/bazel/commit/b4b8b2614ae854651075506666f109d0fc508ad1)
- [Added the `SpawnExec.digest` field.](https://github.com/bazelbuild/bazel/commit/b2cbc9a17c90f9ddeb88eca4756cb2fc764abebe)
- [Added the `SpawnExec.target_label` field.](https://github.com/bazelbuild/bazel/commit/9f908cada13c9015f267f368f263c361df812983)

[execution_log_binary_file]: https://bazel.build/versions/6.0.0reference/command-line-reference#flag--execution_log_binary_file
[execution_log_json_file]: https://bazel.build/versions/6.0.0/reference/command-line-reference#flag--execution_log_json_file
[spawns]: bazels-remote-caching-and-remote-execution-explained.md#spawns

## Query

`bazel build` wasn't the only command to get improvements in this release.
Here are some changes that were made to the `query` family of commands:

- [Added the `--incompatible_package_group_includes_double_slash` flag, which removes the leading `//` from `package_group.package` output.](https://github.com/bazelbuild/bazel/commit/1473988aa1e9b92c42fcbad4e155f247f1983d13)
- [Added an optional second argument to the `some` operator, specifying number of nodes returned.](https://github.com/bazelbuild/bazel/commit/cc71db2a465716ba175d3defaa6032107bf1dd90)
- [Labels are decanonicalized if possible.](https://github.com/bazelbuild/bazel/commit/47b1cad5a6026c220002d56ae3c25e5c20d0cef7)

### `cquery`

- [Added support for queries over incompatible targets.](https://github.com/bazelbuild/bazel/commit/73b22b6485de339794bf623e592f2595692c10af)
- [Added `struct`, `json`, `proto`, and `depset` to `--output=starlark`.](https://github.com/bazelbuild/bazel/commit/d69346575a7a2f791e61d181ca59aae6354236f9)
- [Added `ConfiguredRuleInput` when using `--transitions`, which reports the configuration dependencies are configured in.](https://github.com/bazelbuild/bazel/commit/9994c3277607a3b7bd452512e1ff6e5ba73bbc4a)
- [Added more information about configurations to proto output.](https://github.com/bazelbuild/bazel/commit/29d46eb41a413430498ab033f84a8d960edfb6fb)
- [Added the `Configuration.is_tool` attribute to proto output.](https://github.com/bazelbuild/bazel/commit/fb92e2da3c75db0ac3c119d137119d38c4858009)
- [Complete configurations are now included in proto output.](https://github.com/bazelbuild/bazel/commit/ac48e65f702d3e135bb0b6729392f9cb485da100)
- [`--output=files` now also outputs source files.](https://github.com/bazelbuild/bazel/commit/ca8674cc4879ed1846bf015c33fe7d920a3f66ab)
- [Starlark transitions now report source code location.](https://github.com/bazelbuild/bazel/commit/5de9888f1f28837d0e801f7a4bbbf6f5d6481012)
- [Updated `AnalysisProtosV2`.](https://github.com/bazelbuild/bazel/commit/46a36d683801604e92b8b38f74a1388a973fd543)

### `aquery`

- [Added the `--include_file_write_contents` flag, which includes file contents for the `FileWrite` action.](https://github.com/bazelbuild/bazel/commit/6d73b9619026c536bb9e64f69b515d015b18bf67)
- [Added the `Configuration.is_tool` attribute to the proto output.](https://github.com/bazelbuild/bazel/commit/fb92e2da3c75db0ac3c119d137119d38c4858009)
- [Fixed formatting of non-ASCII text.](https://github.com/bazelbuild/bazel/commit/c55b01e3e4c535738f9aebbb4d1ba0623235aee0)
- [Fixed non-deterministic sorting of execution requirements.](https://github.com/bazelbuild/bazel/commit/ec1ac2f272716a25f9909d61bddb5cfe82756a7b)

## Dependency management

A new dependency system named [Bzlmod][bzlmod] was added in Bazel 5.0,
and made non-experimental in Bazel 6.0.
Besides all of the changes needed to support Bzlmod,
these were some other notable dependency management related changes:

- [Added the `--experimental_check_external_repository_files` flag, which allows disabling checking for modifications to files in external repositories.](https://github.com/bazelbuild/bazel/commit/123da96ef501457193f6ff52f42522249f5e3737)
- [Added the `build_file` and `build_file_content` attributes to `git_repository`.](https://github.com/bazelbuild/bazel/commit/f5a899fbbeb34cbd13ec6293ef722344f20d6714)
- [Added the `add_prefix` attribute to `http_*` rules.](https://github.com/bazelbuild/bazel/commit/87c8b09061eb4d51271630353b1718c39dfd1ebe)
- [Added the `integrity` attribute to `http_file` and `http_jar`.](https://github.com/bazelbuild/bazel/commit/e51a15f4395d4223c9665e5cc8ae2c8dd29e8f20)
- [Added the `workspace_root` attribute to `repository_ctx`.](https://github.com/bazelbuild/bazel/commit/8edf6abec40c848a5df93647f948e31f32452ae6)
- [Added the `success` parameter to `repository_ctx.download`.](https://github.com/bazelbuild/bazel/commit/5af794bb9bb8f9a7f3667d53f31b132a3f51314e)
- [Added the `rename_files` parameter to `repository_ctx.extract`.](https://github.com/bazelbuild/bazel/commit/2b02416b6175bf0ac82cb5c8ecc9a80a2f397e88)
- [Added host arch to repository rule markers.](https://github.com/bazelbuild/bazel/commit/16c89c12af1b703c605b35ec06ede881e77237de)
- [`--override_repository` now accepts tildes.](https://github.com/bazelbuild/bazel/commit/cc55400e614e4e301244cc7c338ee3ea89424ce0)
- [`http_*` rules now honour the `NETRC` environment variable.](https://github.com/bazelbuild/bazel/commit/a15f342931c0ef6529c8f53cd7a8b8823de8979e)
- [Download progress now displays human readable bytes.](https://github.com/bazelbuild/bazel/commit/801e01c30005a562e4d056f2640e6f56cc096413)
- [Removed support for managed directories.](https://github.com/bazelbuild/bazel/commit/cbf8159ba6190ab447ad54df63cb4db25763d755)
- [When Bzlmod is enabled, canonical label literals are used.](https://docs.google.com/document/d/1N81qfCa8oskCk5LqTW-LNthy6EBrDot7bdUsjz6JFC4)

[bzlmod]: https://bazel.build/versions/6.0.0/build/bzlmod

## Platforms and toolchains

The C++, Android, and Apple rules are being migrated to support [building with Platforms][building-with-platforms].
While progress has been made,
they don't fully support it yet in Bazel 6.0.
For C++ projects,
it's recommended that the `--incompatible_enable_cc_toolchain_resolution` flag is used,
to help the Bazel team discover any issues in the wide variety of projects that exist.

Here are some of the platforms and toolchains related changes which weren't tied to any of those migrations:

- The `host` configuration is deprecated, and [a](https://github.com/bazelbuild/bazel/commit/6e6c4cf1bd6edaa15ecc4417cd0be70581181def) [lot](https://github.com/bazelbuild/bazel/commit/32fc7cac3c62dcf1721e9093a449561eb854f241) [of](https://github.com/bazelbuild/bazel/commit/790d7a75b34eb963c18f9165bd9608d7a8eb4f3d) [changes](https://github.com/bazelbuild/bazel/commit/48e88685105c906ae52865422da01c39c580bf21) [were](https://github.com/bazelbuild/bazel/commit/715c9faabba573501c9cb7604192759950633205) [made](https://github.com/bazelbuild/bazel/commit/d988e8b3bace4b2bb2ec4adc17d9bf7b4f49a907) to migrate away from it and to the `exec` configuration.
- [Added the `--incompatible_disable_starlark_host_transitions` flag, which prevents rules from using the `host` configuration.](https://github.com/bazelbuild/bazel/commit/6464f1cbdf14f0b8e8f29f7b57990a40ea584062)
- [Added the `--experimental_exec_configuration_distinguisher` flag, which changes how the `platform_suffix` is calculated.](https://github.com/bazelbuild/bazel/commit/51c90c7d7f749af99d66cbd21bbdda09b68f79ac)
- [Added the `exec_group` argument to `testing.ExecutionInfo()`.](https://github.com/bazelbuild/bazel/commit/423fb20c2bc7773f8c9567be8056566ce4633e5f)
- [Toolchain dependencies can now be optional.](https://github.com/bazelbuild/bazel/issues/14726)
- [`sh` path is now selected based on execution platform instead of host platform, making it possible to execute `sh` actions in multi-platform builds.](https://github.com/bazelbuild/bazel/commit/eeb2e04e52cfad165a1bde33ce2d83a392f53d00)
- [The `exec` transition no longer resets any `TestConfiguration.TestOptions` options.](https://github.com/bazelbuild/bazel/commit/2adf0033bb4e71171e96f95bbd54ab783c6bb6d1)
- [`platform.exec_properties` now become execution requirements.](https://github.com/bazelbuild/bazel/commit/e4c1c434d49062449c7a83dd753fe01923766b1d)

[building-with-platforms]: https://bazel.build/versions/6.0.0/concepts/platforms

## Execution platforms

Execution platforms are [platforms][platforms] which build tools execute on.
These include the host platform on which Bazel runs.

In the following sections I collected notable changes for Linux and macOS.
I'm sure there were some for Windows as well,
but since I don't use Bazel on Windows,
none of the changes stood out to me as pertaining only to it.

[platforms]: https://bazel.build/versions/6.0.0/extending/platforms

### Linux

- [Added the `--incompatible_sandbox_hermetic_tmp` flag, which causes the sandbox to have its own dedicated empty directory mounted as `/tmp` rather than sharing `/tmp` with the host filesystem.](https://github.com/bazelbuild/bazel/commit/ae6a90a143b1ef25ff5fd620c662ab5f81d0043d)
- [Added the `--sandbox-explicit-pseudoterminal` flag, which allow processes in the sandbox to open pseudoterminals.](https://github.com/bazelbuild/bazel/commit/9a13051fb73d68221dfd9849b52e0f3cd046c524)
- [Fixed handling of large UIDs.](https://github.com/bazelbuild/bazel/commit/467f32d8087fed11bd480ec5112bf1228b63053d)

### macOS

- [Remote Xcode version is now matched more granuarly.](https://github.com/bazelbuild/bazel/commit/2ff4124004355b0bf86e1228b4bb1d19bd55ec3d)
- [`clean --async` is now available on macOS.](https://github.com/bazelbuild/bazel/commit/b8d0e26357065740d4007223cf20f488a120290e)
- [The `@bazel_tools//tools/cpp:compiler` flag now has the value `clang` for the auto-configured Xcode toolchain rather than the generic value `compiler`.](https://github.com/bazelbuild/bazel/commit/f99319f14fb1c92c98217bb9b2a85c75bd963368)
- [Removed some aborts from `wrapped_clang`.](https://github.com/bazelbuild/bazel/commit/3451774a8a76b8ef9211e852ec3173498bbac1a7)
- [Fixed `lld` detection on macOS.](https://github.com/bazelbuild/bazel/commit/d9f20dcf099d245a90c2d2965706076126ea574b)

## Target platforms

Target platforms are [platforms][platforms] which you are ultimately building for.
I cover the Android and Apple platforms in the following sections,
as they still have some functionality provided by Bazel core,
instead of being fully supported by standalone Starlark rules.

### Android

- [D8 is now the default desugarer.](https://github.com/bazelbuild/bazel/commit/ff311f618a602c15f5848a317561934b0154132b)
- [D8 is now the default dexer.](https://github.com/bazelbuild/bazel/commit/66d07f096cb697bdadb6f9d9fbc1c4fe33be6d59)
- [The D8 jar from Maven is used instead of Android Studio's.](https://github.com/bazelbuild/bazel/commit/ae247145b897d0f41cdf2b317f5c7b856845e303)
- [Added the `android_binary.min_sdk_version` attribute.](https://github.com/bazelbuild/bazel/commit/4c3219ec5fc2f74b45f7b29b029caf56ed8a1b4d)
- [Added the `ApkInfo.signing_min_v3_rotation_api_version` attribute.](https://github.com/bazelbuild/bazel/commit/f6dbd1e4a127b95276127afe64a095345678019b)
- [Added the `--mobile_install_run_deployer` flag to the `mobile-install` command, which allows skipping the deployer after building.](https://github.com/bazelbuild/bazel/commit/021665748680b415704d70698d13b4efb9e0e9dc)
- [Added the `--bytecode_optimization_pass_actions` flag, which allows splitting the optimization pass into N parts.](https://github.com/bazelbuild/bazel/commit/2b44482eb7645f738fddf48622ccc1cf05a179d7)
- [Added the `--persistent_multiplex_android_tools`, `--persistent_multiplex_android_resource_processor`, `--persistent_android_dex_desugar`, and `--persistent_multiplex_android_dex_desugar` expansion flags, which allow for easy enabling of persistent and multiplexed Android tools (dexing, desugaring, resource, and processing)](https://github.com/bazelbuild/bazel/commit/63aace26c0e624c146840625e69051bace300ff2)
- [Added the `--incompatible_android_platforms_transition_updates_affected` flag, which causes `AndroidPlatformsTransition` to also update `affected by Starlark transition` with the changed options.](https://github.com/bazelbuild/bazel/commit/563664e0e0a9083be554b4931654743d84b9cba4)
- [Added the `--experimental_persistent_multiplex_busybox_tools` flag, which enables multiplex worker support in `ResourceProcessorBusyBox`.](https://github.com/bazelbuild/bazel/commit/4cfd32d419ca322894dea5cbd6e8ce61676ec476)
- [Added worker support to `AndroidCompiledResourceMergingAction`.](https://github.com/bazelbuild/bazel/commit/3df19e8e900c0f435923754fee36ca5d6eb8724d)
- [Added worker support to `CompatDexBuilder`.](https://github.com/bazelbuild/bazel/commit/7ce1c578b1d9a28709b3949ddfaaebe5e32c989f)
- [Added worker support to the D8 desugarer.](https://github.com/bazelbuild/bazel/commit/f32b99f452c08e4276c9c5efd564a246383f50bb)
- [Added the merged manifest to the `android_deploy_info` output group.](https://github.com/bazelbuild/bazel/commit/c60eb5d324da4d81f6be93f442ac6d7576741e8e)
- [Added `application_resources`'s output groups to `android_binary`.](https://github.com/bazelbuild/bazel/commit/bb97b02fb8fd7c4a60d0e85f86acec12bd1af58c)
- [Removed support for `android_binary.multidex=off`.](https://github.com/bazelbuild/bazel/commit/7de4fab0d8b507f3790533e1ae18e94dca916d04)
- [Reduced `AndroidAssetMerger` intermediate outputs.](https://github.com/bazelbuild/bazel/commit/b76cc3a2ec4991bb5772ce309fae288e147bcebe)
- [Resources are now sorted to produce a consistent zip (which helps cache hit rates).](https://github.com/bazelbuild/bazel/commit/2c25f73880ed72dddb4bfa843388b15f02011158)
- [Incremental APKs are now zipaligned before they are installed.](https://github.com/bazelbuild/bazel/commit/1b2cf8d596a98abe3f02f711903435e060f76720)
- [Split APKs are now zipaligned before they are installed.](https://github.com/bazelbuild/bazel/commit/6613f6fbbcf85aac0ee5865d7d5273aad2a5813a)
- [`minsdk` is now added to dexing and desugaring artifacts paths.](https://github.com/bazelbuild/bazel/commit/4829960c1626357f514ac16eb1a4005a360ee96d)
- [Certain Android actions now have their output paths stripped of config information.](https://github.com/bazelbuild/bazel/commit/2907d15985907f17fe53aa8c4e7953b5fa69977c)
- [Fixed Android's `armeabi-v7a` constraint.](https://github.com/bazelbuild/bazel/commit/46e0be4ef67901cab7896348c89de06396f737b4)

### Apple

- [Added the `watchos_device_arm64` `cpu`.](https://github.com/bazelbuild/bazel/commit/ce611646969cfe72ae5e22083708684eae11e478)
- [Added the `watchos_device_arm64e` `cpu`.](https://github.com/bazelbuild/bazel/commit/531df65129f632a19ce0fd27422d55696af42511)
- [Added the `apple_common.link_multi_arch_static_library` function.](https://github.com/bazelbuild/bazel/commit/877845584186301e53ed01eaecf3166db4ddcec7)
- [Added `-no_deduplicate` when linking with Darwin `dbg`/`fastbuild`.](https://github.com/bazelbuild/bazel/commit/96084140804f552c0402de6223776757a66d0e5a)
- [Added support for the `static_link_cpp_runtimes` feature in Apple linking.](https://github.com/bazelbuild/bazel/commit/2cc3dc4130e3a2b11fbfb980609ca3f2532489b7)
- [`cpu`s for tvOS and watchOS are now correctly inferred when running on an Apple Silicon host.](https://github.com/bazelbuild/bazel/commit/da8a327802187363636908f6c3bc16d9f5a18604)
- [32-bit watchOS architectures are no longer included in mulit-arch builds when targeting watchOS 9.0 or greater.](https://github.com/bazelbuild/bazel/commit/01d46bbd54b1f3e75eda5876da4467b5bda33d01)
- [Moved `-framework` flags to be after the `-filelist` flag.](https://github.com/bazelbuild/bazel/commit/7866fd90d216d6bec7af3631ded5b2db56c5cde2)
- [Static frameworks are now linked into fully linked static libraries.](https://github.com/bazelbuild/bazel/commit/ccb2cc05ff72f9f10c741b0e1d44f9a1b1265ddb)
- [macOS dynamic libraries now have `.dylib` extensions instead of `.so`.](https://github.com/bazelbuild/bazel/commit/6e1b440a94214fdef0f6dd489c0aa3c4fdc6ef87)
- [Linking `cc_info` is now exposed from `AppleExecutableBinary` and `AppleDynamicFramework`.](https://github.com/bazelbuild/bazel/commit/9feeb1d928899e3b390241da6d6cfbc8dca1e4a6)
- [Removed the `AppleDylibBinary` and `AppleLoadableBundleBinary` providers.](https://github.com/bazelbuild/bazel/commit/092884b0118f1b8b14ba2277851baa6dcce5cac2)
- [Removed the `should_lipo` argument from `apple_common.link_multi_arch_binary`.](https://github.com/bazelbuild/bazel/commit/3073f1b72fd6a9df758d059391b1f6791ea5eb1b)
- [Removed the native `apple_binary` rule.](https://github.com/bazelbuild/bazel/commit/0535477eabfbafb9665cc5a191b677077496751c)
- [Removed the native `apple_static_library` rule.](https://github.com/bazelbuild/bazel/commit/589354cdcc274edc01e5634c081739737c593f5e)

## Languages

While there are lots of programming languages that are supported through standalone Starlark rules,
some are still written as "native" rules in Bazel core,
or are bundled Starlark rules while [Starlarkification](#starlark) is in progress.
In the following sections I summarize the notable changes in support of these languages.

### C and C++

- [Added the `--experimental_unsupported_and_brittle_include_scanning` flag, which enables C/C++ include scanning.](https://github.com/bazelbuild/bazel/commit/6522472dc8a7efaa278b04fd27b3ebb3d467d4d3)
- [Added the `--host_per_file_copt` flag.](https://github.com/bazelbuild/bazel/commit/4919d4a61d8506d175b25a035500842b8bfe3d0d)
- [Added the `archive_param_file` feature, which allows turning off param file for archives.](https://github.com/bazelbuild/bazel/commit/bff973031a65c139a4878c87e5f335a895964ab9)
- [Added the `default_link_libs` feature, which can be disabled to support pure C.](https://github.com/bazelbuild/bazel/commit/5ebb0d6884c62150afaa8e1a402881fd5c4f6a37)
- [Added the `gcc_quoting_for_param_files` feature, which enables gcc quoting for linker param files.](https://github.com/bazelbuild/bazel/commit/a9e5a32b9c0de2ade15be67bd1b80c3ec8e6b472)
- [Added the `treat_warnings_as_errors` feature, which treats warnings as errors.](https://github.com/bazelbuild/bazel/commit/f802525ad37e4f4567103682244d65f6cc55ff57)
- [Added the `separate_module_headers` parameter to `cc_common.compile`.](https://github.com/bazelbuild/bazel/commit/94f83f4c8c5feb0f879eb83d122808fa328837c6)
- [Added the `language` parameter to `cc_common.configure_features`.](https://github.com/bazelbuild/bazel/commit/d308c175aa53757746c65ab65c17f9dbd6267755)
- [Added the `main_output` parameter to `cc_common.link`.](https://github.com/bazelbuild/bazel/commit/8ac29d796722cc7f8ad951e64fa18b4b4cd467a4)
- [Added the `BAZEL_CURRENT_REPOSITORY` local define to `cc_*` rules.](https://github.com/bazelbuild/bazel/commit/eb181661ff7e72781f46b7994a5e5c9bab45d5dd)
- [Added `Action.argv` support to `CppCompileAction`.](https://github.com/bazelbuild/bazel/commit/aaba5be3d50b642ad3d9bccada8971a06c68c123)
- [Added support for vendor'ed `clang`](https://github.com/bazelbuild/bazel/commit/17ed57ac5ad9b52bfc43e52ca40512f7114c58aa)
- [Reverted `cc_library.interface_deps` back to `implementation_deps`.](https://github.com/bazelbuild/bazel/commit/abe6667469363d543d8d0fe9108eacb9f96028ed)
- [Default flags features can now be disabled.](https://github.com/bazelbuild/bazel/commit/25d17f53bed60464ce74d1c1a1769787ab259cf2)
- [The `per_object_debug_info` feature is now enabled by default.](https://github.com/bazelbuild/bazel/commit/5f51d21e918a4c7286c5024c3c50f2d931662af9)
- [The `@bazel_tools//tools/cpp:compiler` flag now has the value `gcc` if the configured compiler is detected to be `gcc`.](https://github.com/bazelbuild/bazel/commit/ef3f05852b3bf542903f091184da96b96354a13d)
- [The `malloc` attribute of `cc_*` rules now accepts any `CcInfo` providing target.](https://github.com/bazelbuild/bazel/commit/1746a7958d8c332cae563333d9028b066373e547)
- [The C++ archive action now has the `CppArchive` mnemonic.](https://github.com/bazelbuild/bazel/commit/d519fec25245fcc5806360c671b2d242469515f6)
- [Coverage can now be collected for external targets.](https://github.com/bazelbuild/bazel/commit/32e61b3235cf49b8764dd6b10622197fec6056ce)
- [Make variable substitution now accepts `data` dependencies.](https://github.com/bazelbuild/bazel/commit/46a8e09334a03eeb8518a4913dcd98f19ddc9c62)
- [`cc_common.link` no longer stamps actions for tool dependencies.](https://github.com/bazelbuild/bazel/commit/17998427da1438174fb41708d3afcaff7eedc4f4)
- [Windows interface libraries can now use the `.lib` extension.](https://github.com/bazelbuild/bazel/commit/af4a1506d8b5f68a3208e401ee5a9a7c1e0a10ad)
- [Fixed `cc_test` to apply all compilation flags.](https://github.com/bazelbuild/bazel/commit/06f9202e813d649b4ea48aa48cb0668fecb9cefa)
- [Fixed dynamic library lookup with remotely executed tools.](https://github.com/bazelbuild/bazel/commit/e3dcfa54baec45a6b247143106f7ab689df424cd)

### Java

- [Upgraded JDK11.](https://github.com/bazelbuild/bazel/commit/8f3d99aa2e7c0742f7f89ddc3921350537feef77)
- [Upgraded JDK17.](https://github.com/bazelbuild/bazel/commit/fbb0958f281e523db085849e87f56bd70f038b34)
- [Added JDK18.](https://github.com/bazelbuild/bazel/commit/a7f1c7133a58ac721d176487ff8e82bccc55699b)
- [Added the `--incompatible_disallow_java_import_empty_jars` flag, which disallows empty `java_import.jars` attributes.](https://github.com/bazelbuild/bazel/commit/1acda6be4ffc197f0a19a54556a4a3f8eb4b4906)
- [Added the `--multi_release_deploy_jars` flag, which causes `_deploy.jar` outputs of `java_binary` to be Multi-Release jar files.](https://github.com/bazelbuild/bazel/commit/7f75df2d299e383f58e7a0f82fd0822c88776b5e)
- [Added the `com.google.devtools.build.buildjar.javac.enable_boot_classpath_cache` property, which disables the bootstrap classpath cache.](https://github.com/bazelbuild/bazel/commit/fbb68e9bcbe8401690a513f086afeadc57adfdd6)
- [Added the `@AutoBazelRepository` annotation processor.](https://github.com/bazelbuild/bazel/commit/0f95c8a8a74d18823c3117d8f3370bc7301081b4)
- [Added the `add_exports` and `add_opens` attributes to `java_*` rules.](https://github.com/bazelbuild/bazel/commit/2217b13cae4110b0e2b8fe6a283a9b6dfbf150e8)
- [Added the `hermetic_srcs` and `lib_modules` attributes to `java_runtime`.](https://github.com/bazelbuild/bazel/commit/79badc0bf7937e8443bbe9171d92aefcf1e90e6f)
- [Added the `classpath_resources` argument to `java_common.compile`.](https://github.com/bazelbuild/bazel/commit/37d08ed18b6830e0eea81d7cdb7866c3535f9e47)
- [Added the `resource_jars` argument to `java_common.compile`.](https://github.com/bazelbuild/bazel/commit/80ca10bddc68af318efeef37975de0bfec204a68)
- [Added the `--add_exports` and `--add_opens` options to `singlejar`.](https://github.com/bazelbuild/bazel/commit/4ff441b13db6b6f5d5d317881c6383f510709b19)
- [Added the `--hermetic_java_home` option to `singlejar`.](https://github.com/bazelbuild/bazel/commit/341d7f3366ac15b375eb3f9a750382e2a034c9e1)
- [Added the `--jdk_lib_modules` option to `singlejar`.](https://github.com/bazelbuild/bazel/commit/276fb093b83c37d9c4d2b6df01c04700e7aa3346)
- [Added the `--multi_release` option to `singlejar`.](https://github.com/bazelbuild/bazel/commit/f33ce3d8192953e425f613024ec04541ceb80f1b)
- [Added the `--output_jar_creator` option to `singlejar`.](https://github.com/bazelbuild/bazel/commit/8b5ed8aac2b25180fbfac0ff27ebe3998a61209a)
- [Added a tag-based suppression mechanism for `java_import.deps` checking.](https://github.com/bazelbuild/bazel/commit/2930dd3ac20810730f2263fa9e319379f5d22720)
- [Coverage can now be collected for external targets.](https://github.com/bazelbuild/bazel/commit/acbb9e1de6c9f2cc99be4ff849e456d316da9db1)
- [The Java runtimes now have `target_compatible_with` set instead of `exec_compatible_with`.](https://github.com/bazelbuild/bazel/commit/d5559c16ac008b86345fbbade5d600181a2fce6f)
- [`ijar`/`java_import` now preserve classes with `@kotlin.Metadata` annotations.](https://github.com/bazelbuild/bazel/commit/a32a0fd0d6bf75c2c8c6af6281875e90908b82f6)
- [Hermetic packaged JDK modules now record file size in deploy JAR manifest `JDK-Lib-Modules-Size` attribute.](https://github.com/bazelbuild/bazel/commit/756be22f6445d6052a0a9cce66fdb7f7b3a8f300)
- [`TurbineTransitiveJar` attributes are now recognized in `ijar`.](https://github.com/bazelbuild/bazel/commit/b64f734ffea7fa5d53cb1f0e62247a3ff0b74e0d)
- [The stub template now defaults to a UTF-8 locale.](https://github.com/bazelbuild/bazel/commit/17cfa015d73195f6fda7705a57702e36609b3175)

### Objective-C

- [Added support for Objective-C specific features to `cc_common.configure_features`.](https://github.com/bazelbuild/bazel/commit/68f29c67098cedbfaa3ef011eef4df661860ac36)
- [`objc_library` now requires `CcInfo` in its deps.](https://github.com/bazelbuild/bazel/commit/540892d788727f414cc0cd3ea2ae4d4445914366)
- [Removed the `ObjcProvider.direct_headers` attribute.](https://github.com/bazelbuild/bazel/commit/8a2b711a2700740575904682066dbe1e5c9f6d02)

### Python

- [Added the `coverage_tool` attribute to `py_runtime`.](https://github.com/bazelbuild/bazel/commit/9d0163002ca63f4cdbaff9420380d72e2a6e38b3)
- [Added the `requires-darwin` execution requirement to macOS `py_test` targets.](https://github.com/bazelbuild/bazel/commit/32364dc9194b71e5759546a0fd0013c44bf7109e)
- [Added `CurrentRepository()` to Python runfiles library.](https://github.com/bazelbuild/bazel/commit/d60ce2c7c86393638c77698c00c2168a7a936a53)
- [The stub now also considers `RUNFILES_DIR` when no runfiles directory can be found.](https://github.com/bazelbuild/bazel/commit/c3425feeb3bc204923979773ae985dd2f3e24b9f)
- [Reduced the number of imports used in the stub.](https://github.com/bazelbuild/bazel/commit/c33e44c29e6c4fe35331d0c3f7aad1e76d6318da)
- [`py_*.srcs_version="PY2"` is now the the same as `"PY2ONLY"`.](https://github.com/bazelbuild/bazel/commit/ecd4c900b61416609d99c76a4e2190a4d6d7b97f)
- [Removed UNC path prefix on Windows-10.1607 or later.](https://github.com/bazelbuild/bazel/commit/40b95c32317f1739f14899ee5e4605c58e4836b2)

## Rules authoring

Bazel's extensibility is powered by the ability to write custom [rules][rules].
Most rules used in a project will be provided by open source rule sets,
but projects may also define rules themselves.
Bazel 6.0 includes numerous changes that make custom rules more performant,
easier to write,
or even expand what is possible to do with them.

[rules]: https://bazel.build/versions/6.0.0/extending/rules

### Aspects

[Aspects][aspects] allow augmenting build dependency graphs with additional information and actions.
These changes expanded their capabilities:

- [Added the `--aspects_parameters` flag, which allows passing parameters to command-line aspects.](https://github.com/bazelbuild/bazel/commit/37710728712e00e8bd7145662668d72591204146)
- [Added support for using `attr.bool()`.](https://github.com/bazelbuild/bazel/commit/30fd508bf68c8bb9cf32839b71f79a9aa4bd069a)
- [Added support for using `attr.int()`.](https://github.com/bazelbuild/bazel/commit/14292d176cb85d1cf6e20f79fde0249b0fe6ba24)
- [Added support for setting exec_compatible_with and exec_group on aspects.](https://github.com/bazelbuild/bazel/commit/7e3755d3bf1443a0f2a780bc8efff658c464c3a5)
- [Aspects now inherit fragments from required aspects and from targets they are attached to.](https://github.com/bazelbuild/bazel/commit/b3e12bad9852a068ed8687d39e278441b52c910d)

[aspects]: https://bazel.build/versions/6.0.0/extending/aspects

### Persistent workers

[Persistent workers][workers] improve build performance by sending multiple requests to long-running processes.
Here are some notable changes to persistent worker support:

- [Added the `--experimental_total_worker_memory_limit_mb` flag, which sets a limit on total worker memory usage.](https://github.com/bazelbuild/bazel/commit/8e674324d18b715fd09c7fcfedd97be28caa525c)
- [Added the `--experimental_worker_multiplex_sandboxing` flag, which controls whether to sandbox multiplex workers that support it.](https://github.com/bazelbuild/bazel/commit/fb19a28db30c6937f0df4e143b2256e7e4743bd4)
- [Added the `--experimental_worker_strict_flagfiles` flag, which checks if the worker argument list conforms to the spec.](https://github.com/bazelbuild/bazel/commit/cb2cd9fd2b65311da927777c35939701add5b879)

[workers]: https://bazel.build/versions/6.0.0/remote/persistent

### Starlark

[As mentioned at Bazelcon][starlarkification],
progress is being made on migrating natives rules out of Bazel and into standalone Starlark rules.
In the Bazel 6.0 release progress was made on the Android, C++, Java, Objective-C, Protobuf, and Python rules.

In addition to changes directly needed for Starlarkification,
for which there were many and I'm not going to list them here,
the Starlark language itself received performance and feature improvements:

- [Added the `--incompatible_disallow_symlink_file_to_dir` flag, which disallows `ctx.actions.symlink` from symlinking a file into a directory.](https://github.com/bazelbuild/bazel/commit/54f11fe4189edfed92c820b8feaeb8c046d23a2a)
- [Added the `--incompatible_remove_rule_name_parameter` flag, which disables the `rule.name` parameter.](https://github.com/bazelbuild/bazel/commit/64491051223cdf28e7bf015baa131c7d30b2e2d8)
- [Added the `--experimental_debug_selects_always_succeed` flag, which causes `select` functions with no matching clause to return an empty value, instead of failing.](https://github.com/bazelbuild/bazel/commit/b615d0df882d1564dc1d62d40cf50490a9bc3738)
- [Added the `--experimental_get_fixed_configured_action_env` flag, which causes `action.env` to return fixed environment variables specified through features configuration.](https://github.com/bazelbuild/bazel/commit/e82beda0257958892ff8a15f3eae5c8f2f509118)
- [Added the `--experimental_lazy_template_expansion` flag, which adds support for lazily computed substitutions to `ctx.actions.expand_template()`.](https://github.com/bazelbuild/bazel/commit/cc74b11bc1a0a0150abd37e82f913c4b95eb16bb)
- [Added `dict` union operators (`|` and `|=`).](https://github.com/bazelbuild/bazel/commit/b1deea40aeb81f4c7e23594e0166a6a653a75b65)
- [Added the `coverage_support_files` and `coverage_environment` parameters to `coverage_common.instrumented_files_info`.](https://github.com/bazelbuild/bazel/commit/0a13dd69aa27ec643dd44637712b70c24b924c49)
- [Added the `init` parameter to `provider()`, which allows for performing pre-processing and validation of field values.](https://github.com/bazelbuild/bazel/commit/fc13ba271f138c804869d0f99d751e25518b5326)
- [Added `load` visibility.](https://bazel.build/versions/6.0.0/concepts/visibility#load-visibility)
- [`dict()` and `dict.update()` now accept arbitrary maps, not just `dict`s.](https://github.com/bazelbuild/bazel/commit/dfa9c62abda21dcf187df9d2a2c00ecefda223e0)
- [`testing.ExecutionInfo` can now be used as a provider key.](https://github.com/bazelbuild/bazel/commit/40a6cb093e9a9902a35943b97e2ae6b362a5f005)
- [The `symlinks` and `root_symlinks` parameters of the `ctx.runfiles` function now accept `depset`s.](https://github.com/bazelbuild/bazel/commit/aaf87f4ef3e19cbf1e5ab28f50cd8fb90dfd115e)
- [Starlark flags no longer warn for incompatible commands.](https://github.com/bazelbuild/bazel/commit/f717d6a7484d1c7d624b0f39822e6fd61cfe7ce6)
- [Moved `analysis_test` to `testing.analysis_test`.](https://github.com/bazelbuild/bazel/commit/5e80514244fd855b986790f2d981c2ed614e6940)
- [The `\a`, `\b`, `\f`, and `\v` escape sequences are now supported.](https://github.com/bazelbuild/bazel/commit/d0fde13454a71355b7695922c2179d8756b4632f)
- [`print()` statements are now emitted only if the line of code is executed.](https://github.com/bazelbuild/bazel/commit/3bda5c945caff86475a3da9bd5875e8ff94eab71)
- [`native.existing_rule/s()` with `--incompatible_existing_rules_immutable_view` can now be encoded as json and passed as `**kwargs`.](https://github.com/bazelbuild/bazel/commit/cf99f8476df4110ac749deda15340ebde74a2116)
- [Fixed incremental builds that transition on flags when the default value of those flags changed.](https://github.com/bazelbuild/bazel/commit/2f7d965287ddfa056b169cf16144d05f78d03c7d)
- [Fixed preserving the relative order of explicit and expanded Starlark flags.](https://github.com/bazelbuild/bazel/commit/9f2542f91e99b40de65506c8f4768ec0003a4e5d)
- [Fixed `ctx.actions.symlink(target_path = ...)` incorrectly making paths absolute.](https://github.com/bazelbuild/bazel/commit/d834905a158d253e837597175f4905e23266d0c7)

[starlarkification]: https://youtu.be/6_RrNxuny6Y?t=232

## Misc

There were a handful of changes that I couldn't find a nice home for in the sections above,
but I still felt were important or interesting enough to call attention to:

- [Added the `--experimental_output_directory_naming_scheme` flag, which modifies the way the output directory is calculated.](https://github.com/bazelbuild/bazel/commit/52d1d4ae9bfee7379093d46258fa3a56e4d69e61)
- [Added the `--incompatible_check_testonly_for_output_files` flag, which checks `testonly` for prerequisite targets that are output files by looking up `testonly` of the generating rule.](https://github.com/bazelbuild/bazel/commit/65388c330141736d505cec50b4cc02a5d65ed5de)
- [Added the `--incompatible_fix_package_group_reporoot_syntax` flag, which changes the meaning of `"//..."` to refer to all packages in the current repository instead of all packages in any repository.](https://github.com/bazelbuild/bazel/commit/1473988aa1e9b92c42fcbad4e155f247f1983d13)
- [Added the `--incompatible_package_group_has_public_syntax` flag, which allows writing `"public"` or `"private"` in the `package_group.packages` attribute to refer to all packages or no packages respectively.](https://github.com/bazelbuild/bazel/commit/1473988aa1e9b92c42fcbad4e155f247f1983d13)
- [Added the `--incompatible_unambiguous_label_stringification` flag, which enables unambiguous stringification of `Label`s.](https://github.com/bazelbuild/bazel/issues/16196)
- [Added the `bazel leaf` command.](https://github.com/bazelbuild/bazel/commit/b82a8e9124a046f1a530ec4ba54e9c90383f82e9)
- [`bazel config` now shows output path prefixes.](https://github.com/bazelbuild/bazel/commit/507d85fd6dcf276add3b46158eb5bf36a4179db5)
- [`BAZEL_TEST=1` is now set as a test environment variable.](https://github.com/bazelbuild/bazel/commit/830d4649b42451161adbdf907d9935e648f17d8c)
- [Progress updates no longer have their delay increased when there is no cursor control.](https://github.com/bazelbuild/bazel/commit/60e9bf339f319d16b738619a05b1dd5d122ef852)
- [Fixed native rules not merging default outputs into the transitive runfiles.](https://github.com/bazelbuild/bazel/commit/7cc786ab51dacc7e2eade2ab9c2b440bf9d29972)
- [Fixed `--nobuild_runfiles_links` when used with `--run_under`](https://github.com/bazelbuild/bazel/commit/3badca3f7e539e4b56fd5c502233c03b9934b813)

## Summary

As you can see,
Bazel 6.0 was another _massive_ release.
Thankfully,
through the [rolling releases][rolling-releases] process,
people were able to test,
or even actively depend on,
these changes well before the first 6.0 release candidate was cut.

I expect [Bazel 6.1][bazel-6-1] to be a fast follow with some changes that missed the final release candidate.
Work on Bazel 7.0 is well underway as well,
and I look forward to summarizing its changes next year.

[bazel-6-1]: https://github.com/bazelbuild/bazel/milestone/46
[rolling-releases]: https://bazel.build/versions/6.0.0/release/versioning#rolling-releases
