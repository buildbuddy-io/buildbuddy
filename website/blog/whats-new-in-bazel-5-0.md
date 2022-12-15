---
slug: whats-new-in-bazel-5-0
title: "What's New in Bazel 5.0"
description: We reviewed nearly 3,000 commits and summarized them, so you don’t have to!
author: Brentley Jones
author_title: Developer Evangelist @ BuildBuddy
date: 2022-01-19:12:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/bazel_5_0.png
tags: [bazel]
---

[Bazel 5.0][bazel-5-0] includes [nearly 3,000 changes][diff] since 4.2.2.
It's the first major release since 4.0 was release in January of last year,
and it's Bazel's [second LTS release][lts-releases].
Since there were so many changes,
many of them quite impactful,
I felt I needed to review them all and provide a nice summary for y'all.
So that's what I did 😄.

[bazel-5-0]: https://blog.bazel.build/2022/01/19/bazel-5.0.html
[diff]: https://github.com/bazelbuild/bazel/compare/4.2.2...5.0.0
[lts-releases]: https://docs.bazel.build/versions/5.0.0/versioning.html#lts-releases

<!-- truncate -->

The end result was quite big though,
so I've included a table of contents to allow easy navigation to the changes that interest you the most:

<nav class="toc">

- [Command-line flag changes](#command-line-flag-changes)
  - [Renamed](#renamed)
  - [Default values changed](#default-values-changed)
  - [Deprecated](#deprecated)
  - [No-op](#no-op)
  - [Removed](#removed)
- [Remote](#remote)
  - [gRPC](#grpc)
  - [Remote caching (RBC)](#remote-caching-rbc)
  - [Remote execution (RBE)](#remote-execution-rbe)
  - [Build Event Service (BES)](#build-event-service-bes)
- [Logging](#logging)
  - [Build Event Protocol (BEP)](#build-event-protocol-bep)
  - [Timing Profile](#timing-profile)
  - [Execution Log](#execution-log)
- [Query](#query)
- [Dependency management](#dependency-management)
- [Platforms and toolchains](#platforms-and-toolchains)
- [Execution platforms](#execution-platforms)
  - [Linux](#linux)
  - [macOS](#macos)
- [Target Platforms](#target-platforms)
  - [Android](#android)
  - [Apple](#apple)
- [Languages](#languages)
  - [C and C++](#c-and-c)
  - [Java](#java)
  - [Objective-C](#objective-c)
- [Rules Authoring](#rules-authoring)
  - [Aspects](#aspects)
  - [Persistent workers](#persistent-workers)
  - [Starlark](#starlark)
- [Misc](#misc)

</nav>

## Command-line flag changes

Bazel's [LTS strategy][lts-releases] allows for breaking changes between major versions.
In particular,
it allows for command-line flags to be removed,
renamed,
made to do nothing,
or have their default values changed.
In the following sections I collected all such flag changes I could find.

### Renamed

- [`--incompatible_use_lexicographical_unordered_output` is now `--incompatible_lexicographical_output`.](https://github.com/bazelbuild/bazel/commit/2ee3c2b3b60462b81a56fa396d5935ce02052f6d)
- [`--experimental_run_validations` is now `--run_validations`.](https://github.com/bazelbuild/bazel/commit/374cb1f925430c5c75c690ba225f135a6095eb89)
- [`--experimental_existing_rules_immutable_view` is now `--incompatible_existing_rules_immutable_view`.](https://github.com/bazelbuild/bazel/commit/35182000bfc5303254008896481b1350c91c3256)
- [`--project_id` is now `--bes_instance_name`.](https://github.com/bazelbuild/bazel/commit/2b48c6b9a447756fcb3295b8a75899b96efa7fd4)

### Default values changed

- [`--analysis_testing_deps_limit=800`](https://github.com/bazelbuild/bazel/commit/de3b3caed60d47da7e6464478ec9eb814bc794db)
- [`--experimental_enable_aggregating_middleman=false`](https://github.com/bazelbuild/bazel/commit/64d5bae3b805fad68e9f80512595702472f086f1)
- [`--experimental_exec_groups=true`](https://github.com/bazelbuild/bazel/commit/fa4e14b2aef03b93eef4de3239c57adcfdc7db99)
- [`--experimental_forward_instrumented_files_info_by_default=true`](https://github.com/bazelbuild/bazel/commit/5b216b27435aeb9eb9c3bd3c552d6498e1050cc7)
- [`--experimental_jlpl_enforce_strict_deps=true`](https://github.com/bazelbuild/bazel/commit/0c1257ed4e1b83f8d0f6c79d641f6bfcf4d1cfc4)
- [`--experimental_no_product_name_out_symlink=true`](https://github.com/bazelbuild/bazel/commit/06bd3e8c0cd390f077303be682e9dec7baf17af2)
- [`--experimental_profile_cpu_usage=true`](https://github.com/bazelbuild/bazel/commit/f56b1349c84ea691b04e0fd40f28ef2373e2c855)
- [`--experimental_skyframe_cpu_heavy_skykeys_thread_pool_size=HOST_CPUS`](https://github.com/bazelbuild/bazel/commit/38dd8aaa50fb2e4767c9388446a723c4ba231d86)
- [`--include_aspect=true`](https://github.com/bazelbuild/bazel/commit/44e46b836a580c2fe3e96f39f4c6e02516ca4a0b)
- [`--incompatible_disable_depset_items=true`](https://github.com/bazelbuild/bazel/commit/ef967be1718dd7662ed83ea7ef2899fc09576a8e)
- [`--incompatible_disallow_resource_jars=true`](https://github.com/bazelbuild/bazel/commit/fec2fdb1a4c39d1ac313a3b9d1286d9ff3db9e8e)
- [`--incompatible_disallow_strict_deps_for_jlpl=true`](https://github.com/bazelbuild/bazel/commit/10b3479eacb692e5561cf6175fdf6138731178e3)
- [`--incompatible_display_source_file_location=true`](https://github.com/bazelbuild/bazel/commit/32721fb6a1bba90e2d164e79dcec92f2a90d5cc0)
- [`--incompatible_java_common_parameters=true`](https://github.com/bazelbuild/bazel/commit/cfaa88b520a6a14b9d6fd5c1cb06ec8c2ce608db)
- [`--incompatible_override_toolchain_transition`](https://github.com/bazelbuild/bazel/commit/4b47d6f849bb2eca669f48cfdb5d1796100920f3)
- [`--incompatible_require_javaplugininfo_in_javacommon=true`](https://github.com/bazelbuild/bazel/commit/1c062e26226d71c8e4d1fa9a72369880e867d5df)
- [`--incompatible_top_level_aspects_dependency=true`](https://github.com/bazelbuild/bazel/commit/9a765c8b498a72e20af6c391bef01e794913c317)
- [`--incompatible_use_toolchain_resolution_for_java_rules=true`](https://github.com/bazelbuild/bazel/commit/c2485f67601cf7c79c5a49b29fb23194f7a7e9ab)
- [`--trim_test_configuration=true`](https://github.com/bazelbuild/bazel/commit/ebac27ec5a6063556482841da98d63d1abcf1e44)

### Deprecated

- [`--bep_publish_used_heap_size_post_build`](https://github.com/bazelbuild/bazel/commit/a5127917b3aa0a4f64214d2203d5f0f396facb38)
- [`--experimental_force_gc_after_build`](https://github.com/bazelbuild/bazel/commit/ae2a6a2dc909e468a284913c410fde995cf51095)
- [`--experimental_required_aspects`](https://github.com/bazelbuild/bazel/commit/88a02cdda6f854913ba01e5b2666eb27da828cd6)
- [`--{,no}experimental_send_archived_tree_artifact_inputs`](https://github.com/bazelbuild/bazel/commit/37115a5312fec952c4051e036ed1d1d08b053f09)
- [`--experimental_spawn_scheduler`](https://github.com/bazelbuild/bazel/commit/b41576b2f5bb87f3817dfb1ee09493b8220a9634)
- [`--watchfs`](https://github.com/bazelbuild/bazel/commit/03bd0eb6ecd80f824ee8d5b028db96c43721a3b6)

### No-op

These flags now do nothing, but still exist to allow for migration off of them:

- [`--all_incompatible_changes`](https://github.com/bazelbuild/bazel/commit/7300231553b04828174f14e5840ed4e2d11dea72)
- [`--bep_publish_used_heap_size_post_build`](https://github.com/bazelbuild/bazel/commit/d9a523ef5bc704c1aaf7e692f1f8ba9c449a8d86)
- [`--check_constraint`](https://github.com/bazelbuild/bazel/commit/d623892380fdfd43f42bbbfce6b338624cbb3de4)
- [`--experimental_cc_skylark_api_enabled_packages`](https://github.com/bazelbuild/bazel/commit/ba258e8296ff7a110b32586a83ef9534f8e83d0e)
- [`--experimental_force_gc_after_build`](https://github.com/bazelbuild/bazel/commit/cdeb49fd6a12b0d8e2ac1cbb228eaabbf2114554)
- [`--{experimental_,}json_trace_compression`](https://github.com/bazelbuild/bazel/commit/8d391642f2607e55931c4ec9abbd27376a07653b)
- [`--experimental_shadowed_action`](https://github.com/bazelbuild/bazel/commit/3e8cda19b7b82290d0e339731121279a41b393ea)
- [`--experimental_skyframe_eval_with_ordered_list`](https://github.com/bazelbuild/bazel/commit/35ef799700e354231d7f589d19d0b5b5e4767fa9)
- [`--experimental_starlark_config_transitions`](https://github.com/bazelbuild/bazel/commit/6619d77d10415b3a65c434dfd16698634cdf5108)
- [`--experimental_multi_threaded_digest`](https://github.com/bazelbuild/bazel/commit/6ac6954224b2b74c18d3218dfa299424cbc944fb)
- [`--experimental_profile_cpu_usage`](https://github.com/bazelbuild/bazel/commit/3c5e531a30127b43a63e6bfa202de5e027def84a)
- [`--incompatible_applicable_licenses`](https://github.com/bazelbuild/bazel/commit/af3add80ac6f243b6653dea093d9d083a9e575f9)
- [`--incompatible_dont_collect_so_artifacts`](https://github.com/bazelbuild/bazel/commit/144a62a16e9dee54391d3ed8bb1a5597651d3223)
- [`--incompatible_load_python_rules`](https://github.com/bazelbuild/bazel/commit/253933f3adda134494a4f55838b3e16e54652f23)
- [`--incompatible_use_toolchain_resolution_for_java_rules`](https://github.com/bazelbuild/bazel/commit/5b8f054dd20f506e2526ae3b7f89066a5aaca47e)
- [`--legacy_dynamic_scheduler`](https://github.com/bazelbuild/bazel/commit/5b04895ec224b00f7924e15ad6a1b4f3a6e89539)
- [`--use_singlejar_apkbuilder`](https://github.com/bazelbuild/bazel/commit/b213637d406acfcef8f8ed926bff3ad15ba8c15f)

### Removed

- [`--apple_sdk`](https://github.com/bazelbuild/bazel/commit/166771ed928a6688d954a968c9343994fc83419d)
- [`--bep_publish_used_heap_size_post_build`](https://github.com/bazelbuild/bazel/commit/6555d957c68b0b9437217b5665e996ebc0f98e3b)
- [`--enable_apple_binary_native_protos`](https://github.com/bazelbuild/bazel/commit/3135f49fbb7584b9487161401493edd98473f544)
- [`--enable_runfiles`](https://github.com/bazelbuild/bazel/commit/5a91c25ca43547870bcf73cf4b427277c7f62d8d)
- [`--experimental_dynamic_configs`](https://github.com/bazelbuild/bazel/commit/e68a7d01c7ed77ac1a8400216468eddf899a4af3)
- [`--experimental_enable_aggregating_middleman`](https://github.com/bazelbuild/bazel/commit/e95646972603d9f630ff69cca0b6cf75cd3373ee)
- [`--experimental_exec_groups`](https://github.com/bazelbuild/bazel/commit/3973b5f3573e223f259556938aa4adc8a9fd21cf)
- [`--experimental_forward_instrumented_files_info_by_default`](https://github.com/bazelbuild/bazel/commit/c411d9ac0714a669f8970b19a6086185fc2a1290)
- [`--experimental_interleave_loading_and_analysis`](https://github.com/bazelbuild/bazel/commit/544b51da6d15f7446b4855a422b1d1924420f1ab)
- [`--experimental_nested_set_as_skykey_threshold`](https://github.com/bazelbuild/bazel/commit/359ceb4135ac69e7f861d16b8658c5768d720a2c)
- [`--experimental_no_product_name_out_symlink`](https://github.com/bazelbuild/bazel/commit/b621b1b1866868a1dcb060fe7b6833df0c729761)
- [`--experimental_objc_enable_module_maps`](https://github.com/bazelbuild/bazel/commit/427b4dd6b065b9b9c62add633413af897c01e612)
- [`--experimental_query_failure_exit_code_behavior`](https://github.com/bazelbuild/bazel/commit/c1695ef7ecd5ca26721a97463d2f159234711da9)
- [`--experimental_starlark_config_transitions`](https://github.com/bazelbuild/bazel/commit/28a1e6e5fdc5567fa23ab2c7bf87eec5b9b78766)
- [`--experimental_ui_mode`](https://github.com/bazelbuild/bazel/commit/5752762930d69a57ec1c734b83e5d56ab9006704)
- [`--incompatible_enable_execution_transition`](https://github.com/bazelbuild/bazel/commit/f0c6eab2582cefdb37b363c7f37c0b1b12bed76a)
- [`--incompatible_ignore_duplicate_top_level_aspects`](https://github.com/bazelbuild/bazel/commit/ed251187b078c4262bbbc1da72015ce12f9964f4)
- [`--incompatible_objc_compile_info_migration`](https://github.com/bazelbuild/bazel/commit/bcdd55d8956e22758fe3d866427e2dc3ebc4e31b)
- [`--incompatible_objc_provider_remove_compile_info`](https://github.com/bazelbuild/bazel/commit/952c0d7615d931bd912ebf2c6ab5d3f43462891a)
- [`--incompatible_prefer_unordered_output`](https://github.com/bazelbuild/bazel/commit/e7202e3eef51f2dea5cd93f504c89060de6c9bb3)
- [`--incompatible_prohibit_aapt1`](https://github.com/bazelbuild/bazel/commit/aefd107e163087d7dbc822b3342455d91669a58e)
- [`--incompatible_require_java_toolchain_header_compiler_direct`](https://github.com/bazelbuild/bazel/commit/f9db4fb91bf62a831e73bf09431619a361f0fcc5)
- [`--is_stderr_atty`](https://github.com/bazelbuild/bazel/commit/4d9c6f9eed87aa19798a445f37a270c571766c96)
- [`--javabase` and `--java_toolchain`](https://github.com/bazelbuild/bazel/commit/e1c404e1c87c4a5dec227b160c8c4e81686a3f9f)

## Remote

One of Bazel's most powerful features is its ability to use [remote caching and remote execution][remote-explained].
Numerous improvements and fixes to Bazel's remote capabilities are included in Bazel 5.0.

[remote-explained]: bazels-remote-caching-and-remote-execution-explained.md

### gRPC

Bazel uses gRPC as a protocol for most of its remote capabilities.
There were a couple changes that applied at this foundational level:

- [Added mnemonic and label to remote cache and remote execution requests.](https://github.com/bazelbuild/bazel/commit/a750a56f8f4061516ec3056ae8a8295ea8279903)
- [Bazel no longer crashes on errors returned while creating a gRPC connection.](https://github.com/bazelbuild/bazel/commit/b0ae0afd451bbe374be24c905579cece793b90c7)

### Remote caching (RBC)

Using a remote cache is one of the most popular ways of speeding up a Bazel build.
Thankfully these changes make using a remote cache both more performant and more reliable:

- [Added the `--experimental_remote_cache_compression` flag, which compresses gRPC uploads/downloads.](https://github.com/bazelbuild/bazel/commit/50274a9f714616d4735a560db7f617e53fb8d01b)
- [Added the `--experimental_remote_cache_async` flag, which makes uploads happen in the background.](https://github.com/bazelbuild/bazel/commit/7f08b7841fcf4c7d7d09b69f9ec1f24969aba8a1)
- [Added the `--experimental_remote_merkle_tree_cache` flag, which can speed up action cache hit checking.](https://github.com/bazelbuild/bazel/commit/3947c836cf0c966882a1524e40fa8c3721ac5b07)
- [Added the `--experimental_action_cache_store_output_metadata` flag, which can speed up Remote Build without the Bytes after a Bazel server restart.](https://github.com/bazelbuild/bazel/commit/4e29042fd77eae565a711655df6150500cb6e915)
- [Added the `no-remote-cache-upload` tag, which allows downloading, but not uploading, outputs from the remote cache.](https://github.com/bazelbuild/bazel/commit/bfc24139d93f8643686d91596ba347df2e01966a)
- [Reduced `FindMissingBlobs` calls when using a combined cache.](https://github.com/bazelbuild/bazel/commit/dc32f0b6a5d93e4c4426815cecddbc26c1f5d7ac)
- [`FindMissingBlobs` calls and file uploads are now deduplicated.](https://github.com/bazelbuild/bazel/commit/db15e47d0391d904c29e6e5c632089e2479e62c2)
- [Fixed "file not found" errors when remote cache is changed from enabled to disabled.](https://github.com/bazelbuild/bazel/commit/f94898915268be5670fb1e93a16c03e9b14d2a58)
- [`chmod 0555` is now consistently set on outputs.](https://github.com/bazelbuild/bazel/commit/11066c7731b5a16f2f11db93f6716a1595650aad)

### Remote execution (RBE)

For some projects,
using remote execution is the ultimate performance unlock for their Bazel builds.
In addition to the remote caching changes covered above,
which also apply to remote execution,
the following changes improve the remote execution experience:

- [Added the `--experimental_dynamic_skip_first_build` flag, which skips dynamic execution until there has been a successful build.](https://github.com/bazelbuild/bazel/commit/1e42b94b1c35eca37f45c58c048b38e20bd812bc)
- [Improved debug output when using the `--experimental_debug_spawn_scheduler` flag.](https://github.com/bazelbuild/bazel/commit/060d5966adf2c8035276cd1159601567bea57757)
- [Improved performance of workers when using dynamic execution.](https://github.com/bazelbuild/bazel/commit/6080c1e07f4229ea72eacd04faa9302e44955a84)
- [Improved dynamic execution's local execution delay logic.](https://github.com/bazelbuild/bazel/commit/04754efc1868df499b63cba341f7c90ad18aa425)
- [The disk cache can now be used with remote execution.](https://github.com/bazelbuild/bazel/commit/cf57d036c2e1b608ca902267fbbdfeb7ee5aa166)
- [Target-level `exec_properties` now merge with values from `--remote_default_exec_properties`.](https://github.com/bazelbuild/bazel/commit/713abde2576d7e3c3b584eed80097cd53c9cf082)
- [Failure messages from remote execution are now propagated.](https://github.com/bazelbuild/bazel/commit/399a5beb37a2620c9494f7eef774b99dc7ccab6d)

### Build Event Service (BES)

Using a build event service can give you unparalleled insight into your Bazel builds at scale.
There were some nice changes to BES support,
though I think the improvements to how it interacts with the remote cache are especially noteworthy.

- [Added the `--incompatible_remote_build_event_upload_respect_no_cache` flag, which prevents the BES uploader from uploading the outputs of actions that shouldn't be cached remotely.](https://github.com/bazelbuild/bazel/commit/bfc24139d93f8643686d91596ba347df2e01966a)
- [Added the `--bes_header` flag, which allows passing extra headers to the BES server.](https://github.com/bazelbuild/bazel/commit/ef42d1365d0f508d3d817997b5049639a72100ab)
- [Added logging when a blob upload isn't uploaded in time.](https://github.com/bazelbuild/bazel/commit/71b50f6b4c6e462863c8433a163c9eb23b0c5730)
- [The `--build_event_binary_file` flag now implies `--bes_upload_mode=wait_for_upload_complete`.](https://github.com/bazelbuild/bazel/commit/62060babcc26f3cac3db020e423daec8755fd426)
- [BES referenced blobs are no longer stored in the disk cache.](https://github.com/bazelbuild/bazel/commit/dc59d9e8f7937f2e317c042e8da8f97ba6b1237e)
- [The BES uploader now uses the same code as the remote cache uploader and is now more robust to errors.](https://github.com/bazelbuild/bazel/commit/e855a26691daeac81c1e823ad65c7707062e0bd2)
- [Fixed the BES uploader to handle errors in more edge cases.](https://github.com/bazelbuild/bazel/commit/c51eb5730745017d008af9a49dcfcc975d7c0283)

## Logging

Bazel offers various ways to gain insight into your build.
It's not too surprising then that there were over 30 changes to these capabilities in Bazel 5.0.

### Build Event Protocol (BEP)

The build event protocol is used by [build event services](#build-event-service-bes),
so all of these changes could have also been listed in that section as well.
The BEP can also be collected locally with [`--build_event_json_file`][build_event_json_file] and [`--build_event_binary_file`][build_event_binary_file].

The vast majority of changes added additional information to the BEP,
though some are fixes and improvements:

- [Added `cumulative_metrics` field to `BuildMetrics`.](https://github.com/bazelbuild/bazel/commit/77a980f3ec2496d3f952987e3eaf9df8560a114d)
- [Added `worker_metrics` field to `BuildMetrics`.](https://github.com/bazelbuild/bazel/commit/c674101d004eccd795a06e19674e317b04b1b36a)
- [Added `action_data` field to `BuildMetrics.ActionSummary`.](https://github.com/bazelbuild/bazel/commit/f572d3ba1e6c977bace5c638da1628724f7b3e1f)
- [Added `source_artifact_bytes_read` field to `BuildMetrics.ActionSummary`.](https://github.com/bazelbuild/bazel/commit/0a23a5e7867101f8c16fc4778802ed433ef107d3)
- [Added `analysis_phase_time_in_ms` field to `BuildMetrics.TimingMetrics`.](https://github.com/bazelbuild/bazel/commit/34b2947415eab54382921f917c8ffad5feedccb7)
- [Added `garbage_metrics` field to `BuildMetrics.MemoryMetrics`.](https://github.com/bazelbuild/bazel/commit/8965d25af90ba975cd207c84fa461b499d8d51c9)
- [Added count and size of output files and top-level files seen during a build to `BuildMetrics.ArtifactMetrics`.](https://github.com/bazelbuild/bazel/commit/dd8afa0fc79a7ff0fc0633eed6d592e0efd3bbfc)
- [Added `incomplete` field to `TargetComplete.OutputGroup`.](https://github.com/bazelbuild/bazel/commit/3e1ba0c3b8ef8588b0424291b498a555fe510d44)
- [Added the `--experimental_bep_target_summary` flag, which adds the `TargetSummary event`, which is intended to summarize all `TargetComplete` (including for aspects) and `TestSummary` messages for a given configured target.](https://github.com/bazelbuild/bazel/commit/8d481542111cff64ec28034bd71c83e54f2da28d)
- [Added `attempt_count` field to `TestSummary`.](https://github.com/bazelbuild/bazel/commit/766cd0ed2906546b3d2c75771d65b15aeb07bb4e)
- [Added test suite expansions.](https://github.com/bazelbuild/bazel/commit/7fb0c168f79658a5422607c67f31bcabb66158c6)
- [Added conflicting action output reporting.](https://github.com/bazelbuild/bazel/commit/248792062f283866d9b93bf47b135309fd65c8c3)
- [Replaced `remote_cache_hits` field with a more detailed `runner_count`.](https://github.com/bazelbuild/bazel/commit/f880948ec24efd08b2fde8213304192561c9ad61)
- [Timestamps and durations now use Well Known Types.](https://github.com/bazelbuild/bazel/commit/e5c832a16e475f636c845b0247731f65df5e258c)
- [Improved reporting of Starlark build settings.](https://github.com/bazelbuild/bazel/commit/bf31feb7b8a1428c2ad0e3a4e73c655e7582aa08)
- [The `used_heap_size_post_build` field is now populated when the `--memory_profile` flag is set.](https://github.com/bazelbuild/bazel/commit/3d585234f6b1861d174b21242cdfa48022464e82)
- [Made `--bep_publish_used_heap_size_post_build` work for `query` and other non-`build` commands.](https://github.com/bazelbuild/bazel/commit/1c3bc902c5c93568f8f86125fe57580ba8186d04)
- [You can now assume the `named_set_of_files` event will appear before any event referencing that `named_set` by ID; this allows consumers to process the files for such events (eg. `TargetCompleted`) immediately.](https://github.com/bazelbuild/bazel/commit/fcf9dd50e27a1f77e1c30dfedaabc6118319e5e1)
- [The BEP now includes all files from successful actions in requested output groups; previously, an output group's files were excluded if any file in the output group was not produced due to a failing action.](https://github.com/bazelbuild/bazel/commit/d2f93fd969f5eca042d029a1b8016d83ce8ce165)

[build_event_binary_file]: https://docs.bazel.build/versions/5.0.0/command-line-reference.html#flag--build_event_binary_file
[build_event_json_file]: https://docs.bazel.build/versions/5.0.0/command-line-reference.html#flag--build_event_json_file

### Timing profile

The action timing profile,
which is enabled by default with [`--profile`][profile],
is viewable both [locally in Chrome][performance-profiling] and on [build event services](#build-event-service-bes).
These changes add more detail and clarity to the profile:

- [Added server heap usage.](https://github.com/bazelbuild/bazel/commit/f4fbbd653822ef7306f60144515e7ef551b2d39b)
- [Added system CPU and memory metrics.](https://github.com/bazelbuild/bazel/commit/ec2eda1b56a5197ee2d019f58d89a68b17974b13)
- [Added file system traversal tracing.](https://github.com/bazelbuild/bazel/commit/45d82cadb52f9fa0ee23cf2c45eb1cd149587852)
- [Added `mobile-install` tracing.](https://github.com/bazelbuild/bazel/commit/559db4e9c35e5c0f840a26a80b171129044329c8)
- [Added worker execution tracing.](https://github.com/bazelbuild/bazel/commit/786b418d1fcd1ad1df66237be45682624007ed2e)
- [Renamed action count field from `cpu` to `action`.](https://github.com/bazelbuild/bazel/commit/c5f87ea56a8b1f90525b8a6407dba8778524ca60)
- [Renamed "grpc-command" and "Service Thread" threads to "Main Thread" and "Garbage Collector".](https://github.com/bazelbuild/bazel/commit/a03674e6297ed5f6f740889cba8780d7c4ffe05c)
- [The profile now mentions when an action is acquiring resources.](https://github.com/bazelbuild/bazel/commit/fa9fabb3f699f1e8e8ebbe22228fc4acbc9df2ba)

[performance-profiling]: https://docs.bazel.build/versions/5.0.0/skylark/performance.html#performance-profiling
[profile]: https://docs.bazel.build/versions/5.0.0/command-line-reference.html#flag--profile

### Execution log

Bazel logs all of the [spawns][spawns] it executes in the execution log,
which is enabled with the [`--execution_log_json_file`][execution_log_json_file] or [`--execution_log_binary_file`][execution_log_binary_file] flags.
This feature is relatively stable,
with just a single noticeable change:

- [Added wall time duration.](https://github.com/bazelbuild/bazel/commit/f92d80ce649a9abcb3e674a6e7c7eaf3b4f08ecd)

[execution_log_binary_file]: https://docs.bazel.build/versions/5.0.0/command-line-reference.html#flag--execution_log_binary_file
[execution_log_json_file]: https://docs.bazel.build/versions/5.0.0/command-line-reference.html#flag--execution_log_json_file
[spawns]: bazels-remote-caching-and-remote-execution-explained.md#spawns

## Query

`bazel build` wasn't the only command to get improvements in this release.
Here are some changes that were made to the `query` family of commands:

- [Added the `--incompatible_lexicographical_output` flag (on by default), which lexicographically sorts the output of `query --order_output=auto`.](https://github.com/bazelbuild/bazel/commit/acbceddd97fb86dca96d0cd1ec45807335d7f7fc)
- [Added the `--deduplicate_depsets` flag (on by default) to `aquery`, which removes duplicate subsets in `dep_set_of_files`.](https://github.com/bazelbuild/bazel/commit/28ffaa259e1df63b64940344bb3b72999a8a1de6)
- [Added `--keep_going` functionality to graphless `query`.](https://github.com/bazelbuild/bazel/commit/a01371abaae4b2329877e25ff13d1a99e35034f1)
- [Added execution platform information to `aquery` output.](https://github.com/bazelbuild/bazel/commit/11c09c5254e6dc5e17016a17c345fe374b3d799d)
- [Fixed output of `config_setting` visibility.](https://github.com/bazelbuild/bazel/commit/5dfffefef58da0c426cfa0b3d70132dde77950b0)

## Dependency management

A new
(currently experimental)
external dependency system,
codenamed [Bzlmod][bzlmod],
was added in Bazel 5.0.
Besides for all of the changes needed to support Bzlmod,
there was one more notable dependency management related change:

- [Added the `--experimental_repository_downloader_retries` flag, which allows Bazel to retry certain repository download errors.](https://github.com/bazelbuild/bazel/commit/a1137ec1338d9549fd34a9a74502ffa58c286a8e)

[bzlmod]: https://docs.bazel.build/versions/5.0.0/bzlmod.html

## Platforms and toolchains

The C++, Android, and Apple rules are being migrated to support [building with Platforms][building-with-platforms].
While [progress has been made][platforms-progress],
they don't fully support it yet in Bazel 5.0.
For C++ projects,
it's recommended that the `--incompatible_enable_cc_toolchain_resolution` flag is used,
to help the Bazel team discover any issues in the wide variety of projects that exist.

Here are some of the platforms and toolchains related changes which weren't tied to any of those migrations:

- [Added the `--experimental_platform_in_output_dir` flag, which causes the output dir name to use a non-default platform name instead of the CPU.](https://github.com/bazelbuild/bazel/commit/daecf427ec0bf0e963c324783062c4f5b61ff679)
- [`target_compatible_with` can now be used with all non-workspace rules.](https://github.com/bazelbuild/bazel/commit/d052ececddf54587c576f876ce13d4b8f4aacb0b)
- [The `--toolchain_resolution_debug` flag now accepts regexes matching targets, as well as toolchain types, when choosing what debug messages to print.](https://github.com/bazelbuild/bazel/commit/545befb13a64468377e3792a8f56c67cac245fa1)
- [The toolchain transition is now enabled for all toolchains.](https://github.com/bazelbuild/bazel/commit/4b47d6f849bb2eca669f48cfdb5d1796100920f3)
- [Progress is being made on changing all `host` configurations to `exec`.](https://github.com/bazelbuild/bazel/commit/8d66a4171baddcbe1569972f019e54130111202c)

[building-with-platforms]: https://docs.bazel.build/versions/5.0.0/platforms-intro.html
[platforms-progress]: https://github.com/bazelbuild/bazel/issues/6431#issuecomment-978329014

## Execution platforms

Execution platforms are [platforms][platforms] which build tools execute on.
These include the host platform on which Bazel runs.

In the following sections I collected notable changes for Linux and macOS.
I'm sure there were some for Windows as well,
but since I don't use Bazel on Windows,
none of the changes stood out to me as pertaining only to it.

[platforms]: https://docs.bazel.build/versions/5.0.0/platforms.html

### Linux

I only noticed a single change that was directly related to Linux execution:

- [Added the `--experimental_use_hermetic_linux_sandbox` flag, which configures `linux-sandbox` to use a `chroot` environment.](https://github.com/bazelbuild/bazel/commit/11f7d8040fadc595589ee264561606dc2a83685d)

### macOS

On the other hand,
macOS had a lot of changes related to it:

- [Added the `xcode_version_flag.precision` attribute to allow matching a subset of the version components.](https://github.com/bazelbuild/bazel/commit/7484c98e871ab3fa3408be53873b8848861d26be)
- [Added progress reporting in `xcode_configure`.](https://github.com/bazelbuild/bazel/commit/762f9e28f90b863064d81bdf61d3ad0e31084bc4)
- [Toolchain tools now compile as universal binaries if possible.](https://github.com/bazelbuild/bazel/commit/de5fc19b0956c900f2648b68a881fd3456c10e7e)
- [`xcode-locator` is now built as a universal binary.](https://github.com/bazelbuild/bazel/commit/6916fc1c4c49134ee76b9a725deddd1e6bcab24a)
- [Sandboxed actions can now run `/bin/ps`.](https://github.com/bazelbuild/bazel/commit/652d1cc233a49593767c01725974eb17b90dca4b)
- [If `--experimental_prefer_mutual_xcode` is passed, Bazel chooses the local default (instead of the newest mutually available version) if it's available both locally and remotely.](https://github.com/bazelbuild/bazel/commit/4bef502e027238b56e30b787013a525dba0cde3d)
- [Generated `xcode_version` targets now include product version aliases.](https://github.com/bazelbuild/bazel/commit/f52e218d99e26298b69c838793bd80f9ae7a226d)
- [`osx_archs.bzl` is now explicitly exported from the crosstool.](https://github.com/bazelbuild/bazel/commit/aeec9a876b9c4f7cb54548f8855a2e938387b91e)
- [Fixed typo in the cache directory name.](https://github.com/bazelbuild/bazel/commit/b5bbe28ce207375009babc142fd3e8ce915d3dc9)
- [Fixed `libtool` with params files.](https://github.com/bazelbuild/bazel/commit/48dd159808c5a874c4aabcd27d6e66610d41bb5a)
- [Fixed the `--experimental_prefer_mutual_xcode` flag not being used correctly for all parts of the build.](https://github.com/bazelbuild/bazel/commit/bb41ebc210989ca6aeaf4f776f4175c4b952e9fe)
- [Fixed a crash when a custom crosstool specifies a `DEVELOPER_DIR` environment variable.](https://github.com/bazelbuild/bazel/commit/1811e82ca4e68c2dd52eed7907c3d1926237e18a)
- [Fixed JSON output for `xcode-locator`.](https://github.com/bazelbuild/bazel/commit/1bae172b8af8cd60dafada4be45e1cdffe9a763d)

## Target platforms

Target platforms are [platforms][platforms] which you are ultimately building for.
I cover the Android and Apple platforms in the following sections,
as they still have some functionality provided by Bazel core,
instead of being fully supported by standalone Starlark rules.

### Android

- [Added the `--experimental_run_android_lint_on_java_rules` flag, which causes Java rules to invoke Android Lint.](https://github.com/bazelbuild/bazel/commit/909bec5afeec111a38d49d331ae24e7b8f041b5b)
- [Added the `android_sdk.legacy_main_dex_list_generator` attribute.](https://github.com/bazelbuild/bazel/commit/0ccbbde12e62773a69249372b9bcaa30ab223317)
- [Added support for location expansion in the `java_toolchain.android_lint_opts` attribute.](https://github.com/bazelbuild/bazel/commit/1c0194cc994d29b938d638e8625deaa19ffaf636)
- [Added support for symlinks in Android SDK repositories.](https://github.com/bazelbuild/bazel/commit/e41440e97c46a7f76a92e4b147eff02f1343451b)
- [The minimum Android build tools version for the Android rules is now 30.0.0.](https://github.com/bazelbuild/bazel/commit/0e652737988e3c115e98e1552f6fada52bc2b9a2)
- [`--apk_signing_method` now accepts the value `v4`.](https://github.com/bazelbuild/bazel/commit/f9df9d7318f51c1998c76bc50a5d48deacac5963)
- [Tags are now propagated to `AndroidBinary` actions.](https://github.com/bazelbuild/bazel/commit/8f927d2e612263c78f6f4122f634434c29c71c85)
- [`aapt2 convert` is now used instead of relinking final APK as static lib.](https://github.com/bazelbuild/bazel/commit/8465c173b97370704b277f12c6f827746c4abf4e)
- [The `--no-proguard-location-reference` flag is now passed to `aapt2` when linking and generating proguard configurations.](https://github.com/bazelbuild/bazel/commit/705b419f95060305de93e7861544bd73bb663f19)
- [Made Android Lint worker compatible.](https://github.com/bazelbuild/bazel/commit/65dc407929eeb2372da65d3d4043dac9881f3235)
- [Made `AarGeneratorAction` worker compatible.](https://github.com/bazelbuild/bazel/commit/64ffc098a23a4d72726e2efbf590cb8002abeec2)
- [Android Lint validations are now correctly propagated through `android_binary`'s split transition.](https://github.com/bazelbuild/bazel/commit/c6c63890460546c2d605812ad7e7baef19eb7100)
- [`android_binary` now always uses the bytecode optimizer's mnemonic.](https://github.com/bazelbuild/bazel/commit/c381a2745107a444c2b99888b7e91beee3c50993)
- [Fixed Android API level support in NDK crosstools.](https://github.com/bazelbuild/bazel/commit/b4c637c5e3af145295ad3f4ca732c228ab6a88cf)
- [Fixed `AarResourcesExtractor` action to produce consistent zips.](https://github.com/bazelbuild/bazel/commit/85ab374bafbf4f8f4dff1700e477b5dc4e20ccc5)
- [Fixed `--fat_apk_hwasan` when using `--{,android_}cpu` instead of `--fat_apk_cpu`.](https://github.com/bazelbuild/bazel/commit/a38eb75865a0cb90b50fbcab65436be4fe830483)

### Apple

- [Added support for the Apple Silicon iOS simulator.](https://github.com/bazelbuild/bazel/commit/c1ea2d4b9cb6afa33eabba285f7c962c2f954e23)
- [Added support for the Apple Silicon watchOS simulator.](https://github.com/bazelbuild/bazel/commit/9c1c622fed219cb6b9c0656ebe4a4f3c117029b9)
- [Added the `--host_macos_minimum_os` flag.](https://github.com/bazelbuild/bazel/commit/6345c806ad81f29b390d67ce6e1510b47bf82ddd)
- [Added support for LLVM BC files.](https://github.com/bazelbuild/bazel/commit/f53c389dbd4b0ae66307e9bd4a72b36b3d284ec3)
- [Added the `--incompatible_disable_native_apple_binary_rule` flag, which disables the native `apple_binary` rule.](https://github.com/bazelbuild/bazel/commit/65e9732c7d2a791ed2d6f9e90c8279acdbf1096f)
- [When building an iOS target it now defaults to the Apple Silicon iOS simulator when building on an Apple Silicon host.](https://github.com/bazelbuild/bazel/commit/ca5f67f9a73d3be20539c923a655997680328fc3)
- [Made `-c opt` strip dead code by default.](https://github.com/bazelbuild/bazel/commit/3114e806c33df3f1a9555f463dd17f028eef80c6)
- [Timestamps are now disabled in linking actions.](https://github.com/bazelbuild/bazel/commit/073ae810e4f61f56f68a31d5ee95f18633d176cc)
- [`--apple_bitcode=embedded` now applies to `cc_library` targets.](https://github.com/bazelbuild/bazel/commit/3439a52cd6cc92420fa8f21c5910e750b01c18c7)
- [Removed support for "nodeps" dynamic libraries.](https://github.com/bazelbuild/bazel/commit/ec5553352f2f661d39ac4cf665dd9b3c779e614c)
- [Lipo operations are now conditional with the `linkMultiArchBinary` Apple binary Starlark API.](https://github.com/bazelbuild/bazel/commit/066fba3486d8d395cb67720888bd8258a5ffc83a)
- [`--apple_crostool_top` is now preserved in host/exec transitions.](https://github.com/bazelbuild/bazel/commit/3ece10a9d6ea2236d8780f5ffe623d6374d4dc4a)

## Languages

While there are lots of programming languages that are supported through standalone Starlark rules,
some are still written as "native" rules in Bazel core,
or are bundled Starlark rules while [Starlarkification](#starlark) is in progress.
In the following sections I summarize the notable changes in support of these languages.

### C and C++

- [Added support for CUDA header file type (.cuh).](https://github.com/bazelbuild/bazel/commit/c750c529ab0646e40c60f645f51d468155418269)
- [Added `.rlib` as an allowed extension for static libraries.](https://github.com/bazelbuild/bazel/commit/ad0382340ab4ffb4832c11cb92ec68755ef0eec2)
- [Added support for location expansion in `cc_*.defines` attributes.](https://github.com/bazelbuild/bazel/commit/28fc8a1f7f7524d1b730da2a41341ce4aa1fea35)
- [Added the `cc_toolchain.target_transition_for_inputs` attribute.](https://github.com/bazelbuild/bazel/commit/26abd97974554887e9a4d140bf9f4800e23f1da8)
- [Added native options for `cc_shared_library`.](https://github.com/bazelbuild/bazel/commit/96afa0f01b49f8dfb4db02c1b2a6a60f3a846a01)
- [Added `-g` in `per_object_debug_info` for Clang 12 and GCC 11.](https://github.com/bazelbuild/bazel/commit/fa69b78ff2ab4138b5ec3fcdeb23ba406f7f8227)
- [Added support for clang's `libc++` to the Unix toolchain.](https://github.com/bazelbuild/bazel/commit/f1531dc2ebfb2db721a94f6f19081fa89af536da)
- [Added support for LLD to the Unix toolchain.](https://github.com/bazelbuild/bazel/commit/00e30ca5968d42b4a1e42327fa683debc1063b89)
- [Added the `external_include_paths` feature, which disables warnings from external headers.](https://github.com/bazelbuild/bazel/commit/08936aecb96f2937c61bdedfebcf1c5a41a0786d)
- [Added the `--incompatible_enable_cc_test_feature` flag, which switches from the use of build variables to the feature of the same name.](https://github.com/bazelbuild/bazel/commit/c74ae11562c44a49accc33dfae85e74036344f38)
- [Added the `--experimental_cpp_compile_resource_estimation` flag, which estimates precise resource usage for local execution of `CppCompileAction`.](https://github.com/bazelbuild/bazel/commit/f5196e2ce4e8e45ca49c271053dad8dd32be5080)
- [Added the `--experimental_use_cpp_compile_action_args_params_file` flag, which causes `CppCompileAction` to write exposed `action.args` to a parameters file.](https://github.com/bazelbuild/bazel/commit/dc914c6822a785ce139e8fc8bdf12594835f5674)
- [Added support for Starlark transitions on `--incompatible_enable_cc_toolchain_resolution`.](https://github.com/bazelbuild/bazel/commit/c4357cf248612b2451b4337e44757c934de22d5c)
- [Added the `dynamic_mode` attribute to the `cpp` fragment.](https://github.com/bazelbuild/bazel/commit/e2915a8fe56b1917510a922b71238c1b51a8972f)
- [C++ modules are no longer created if there are no modular headers.](https://github.com/bazelbuild/bazel/commit/09cba3b5c80b28ac1fe2a5a312f8d3b41bf3eecd)
- [Made the `abi_version` and `abi_glibc_version` attributes optional for cc toolchains.](https://github.com/bazelbuild/bazel/commit/9a5bef9983848535c8a48f783700b580ec44df3f)
- [Made `gcov` optional in cc toolchains.](https://github.com/bazelbuild/bazel/commit/e8a95ca927fb49b81787df77ffd775340fe715f2)
- [Renamed `target_transition_for_inputs` to `exec_transition_for_inputs` to better reflect the mechanism at play.](https://github.com/bazelbuild/bazel/commit/5448c7c5460d7928013a438bd2b88bb811362df9)
- [The `oso_prefix_is_pwd` feature now applies to C++ link actions.](https://github.com/bazelbuild/bazel/commit/9b4844a0ca3ee031c8b930c23d551da6a9050a85)
- [Fixed `CppCompileActionTemplate`'s handling of PIC.](https://github.com/bazelbuild/bazel/commit/03493609e8559aa3aab28d55b521a5bdb6e30b68)
- [Fixed build failures when the same shared lib is depended on multiple times.](https://github.com/bazelbuild/bazel/commit/b571f2c1b9272a14710e15f719dff8af86b79f06)
- [Fixed C++ code coverage when using remote execution.](https://github.com/bazelbuild/bazel/commit/ab1da7beb367147d7d2df17f53bbf0ed78118b69)
- [Fixed a performance issue when `cc_binary` depended on `cc_shared_library` targets.](https://github.com/bazelbuild/bazel/commit/503d56eda25b7e2a12431411bfb7ff286d028405)
- [Fixed a crash when feature configuration or cc toolchain were `None`.](https://github.com/bazelbuild/bazel/commit/d3b3425be23917e5b126d54a2e224be3c3af55e9)

### Java

- [Upgraded JDK11.](https://github.com/bazelbuild/bazel/commit/698c17ab7ea6f4617f1f35a6b3e3085e88c1693a)
- [Removed JDK14.](https://github.com/bazelbuild/bazel/commit/06f7340819f9eaf643d13400a1e279650caf6c56)
- [Added JDK16.](https://github.com/bazelbuild/bazel/commit/e2ed2fd5cad34f0e97f117df391e255b59d96b63)
- [Added JDK17.](https://github.com/bazelbuild/bazel/commit/eb7bf8cd9b6e0eccc7eddcfd2f5d1fb10242c30f)
- [Added support for Java versions with a single component.](https://github.com/bazelbuild/bazel/commit/2a3e194ff5efa4b73b59bcb87ce894f46d95b882)
- [Added support for worker cancellation.](https://github.com/bazelbuild/bazel/commit/05d04464b1ceca115ceba4410195b0a967e39aeb)
- [Added support for record attributes to `ijar`.](https://github.com/bazelbuild/bazel/commit/26229fe7a04791a512cc1320ce84130e97afe565)
- [Added support for sealed class attributes to `ijar`.](https://github.com/bazelbuild/bazel/commit/70ae39015e25acec226835201b6dd70f55716280)
- [Added the `--host_jvmopt` flag.](https://github.com/bazelbuild/bazel/commit/570f01968093d943b1f878f2cf57da4c20ba5355)
- [Added the `javabuilder_data` and `turbine_data` attributes to `java_toolchain`.](https://github.com/bazelbuild/bazel/commit/a1b19dfa09abe6d7d3b62f2253e4bfa718aec089)
- [Added the `http_jar.downloaded_file_path` attribute.](https://github.com/bazelbuild/bazel/commit/15b1840a5f45584bab82eaf141dae082cc3ce780)
- [Added mnenomic tags for java toolchain actions.](https://github.com/bazelbuild/bazel/commit/120ea6cbd8da0865a1d8fc7c251c9072a346baab)
- [Added `java_outputs` to `JavaPluginInfo`.](https://github.com/bazelbuild/bazel/commit/f73e28cd44d439e6bcecd73f45813ca13c352a93)
- [Java rules now use toolchain resolution.](https://github.com/bazelbuild/bazel/issues/7849)
- [Java branch coverage now applies Jacoco’s coverage filters.](https://github.com/bazelbuild/bazel/commit/065e2e8e76c5bf21bd797f3c8baebe909e85a6cf)
- [The local JDK is now attached to `JVM8_TOOLCHAIN_CONFIGURATION`.](https://github.com/bazelbuild/bazel/commit/7e48642db90977d37531053775be5ac9083078b7)
- [Only JDK repositories that are needed are downloaded now.](https://github.com/bazelbuild/bazel/commit/903c2720792574321d3e3591ca14a9d287819cb7)
- [`java_test` now has the `requires-darwin` execution requirement on macOS.](https://github.com/bazelbuild/bazel/commit/ca39c05b05527f83480852d26cd08be9bf0cba09)
- [Removed `ABSOLUTE_JAVABASE`.](https://github.com/bazelbuild/bazel/commit/2a07fedf87eae77ce53bc6f0807cc41d1ac743ab)
- [Removed the `jarFiles`, `resources` and `sourceJarsForJarFiles` attributes from `JavaSourceInfoProvider`.](https://github.com/bazelbuild/bazel/commit/34cfab54485de9b1fc424bbc2dee19ed46942f2c)
- [Removed the `JavaInfo.add_compile_time_jdeps` and `compile_time_jdeps` attributes.](https://github.com/bazelbuild/bazel/commit/4d54234df2711d52a80be053808a18852f6ff4ef)

### Objective-C

- [Added support for location expansion in the `objc_library.copts` attribute.](https://github.com/bazelbuild/bazel/commit/d966a0d6eaab557065cf06b5a7b23299f01142a8)
- [Swift module maps are no longer generated in `objc_library` and `objc_import` targets.](https://github.com/bazelbuild/bazel/commit/31bec271b148a826f6d1a527e6c087e5b2d9333f)
- [Removed the `generate_dsym` method from the `objc` fragment.](https://github.com/bazelbuild/bazel/commit/1bf58436a8fca8c704c8738520cc6d33d4f73da0)

## Rules authoring

Bazel's extensibility is powered by the ability to write custom [rules][rules].
Most rules used in a project will be provided by open source rule sets,
but projects may also define rules themselves.
Bazel 5.0 includes numerous changes that make custom rules more performant,
easier to write,
or even expand what is possible to do with them.

[rules]: https://docs.bazel.build/versions/5.0.0/skylark/rules.html

### Aspects

[Aspects][aspects] allow augmenting build dependency graphs with additional information and actions.
These changes expanded their capabilities:

- [Added the `--experimental_enable_aspect_hints` flag,](https://github.com/bazelbuild/bazel/commit/60ebb105dbf34f0b267ea7573157246b4c9bfcaf) [which adds the `aspect_hints` attribute to rules.](https://github.com/bazelbuild/bazel/commit/a2856bf66f4b4c519861a994c6e09bd263f2a31e)
- [Added the `--experimental_required_aspects` flag, which allows aspects to depend on other aspects.](https://github.com/bazelbuild/bazel/commit/f8c34080de1f9b935e3f6abf23b2a319e62c9052)
- [Added the `--incompatible_top_level_aspects_dependency` flag, which allows top-level aspect dependencies.](https://github.com/bazelbuild/bazel/commit/ed251187b078c4262bbbc1da72015ce12f9964f4)
- [When using the `--allow_analysis_failures` flag (for example, via `bazel-skylib`'s `analysistest` with `expect_failure = True`), analysis-time failures in aspect implementation functions will now be propagated and saved in `AnalysisFailureInfo`, just like analysis-time failures in rules.](https://github.com/bazelbuild/bazel/commit/020dd5f1ee9c2c388644dbcd45cda4ebf6b42876)

[aspects]: https://docs.bazel.build/versions/5.0.0/skylark/aspects.html

### Persistent workers

[Persistent workers][workers] improve build performance by sending multiple requests to long-running processes.
Here are some notable changes to persistent worker support:

- [Added support for worker cancellation.](https://github.com/bazelbuild/bazel/commit/e0d6f8b00490a5e7973e74680e9e0ca3f19dc6e1)
- [Added the `verbosity` field to the worker protocol, primarily controlled by the `--worker_verbose` flag.](https://github.com/bazelbuild/bazel/commit/40d33638c4abd0860006253b922e362bae05a238)
- [JSON based workers now have their requests formatted according to the ndjson spec.](https://github.com/bazelbuild/bazel/commit/299e50aae9d8c0b7f0d47aa2ce3d2658a3a80a94)

[workers]: https://docs.bazel.build/versions/5.0.0/persistent-workers.html

### Starlark

[As mentioned at Bazelcon][starlarkification],
progress is being made on migrating natives rules out of Bazel and into standalone Starlark rules.
In the Bazel 5.0 release progress was made on the Android, C++, Java, and Objective-C rules.

In addition to changes directly needed for Starlarkification,
for which there were many and I'm not going to list them here,
the Starlark language itself received performance and feature improvements:

- [Added support for nested `def` statements.](https://github.com/bazelbuild/bazel/commit/5ca20643e54e5cff1eb2939d044f86f96861176a)
- [Added support for lambda expressions.](https://github.com/bazelbuild/bazel/commit/50ce3f973cbc96a0326560a31b736a4f0ca8dc62)
- [Added support for `allow_closure` in the `args.add_all` and `args.add_joined` methods.](https://github.com/bazelbuild/bazel/commit/6e0050d81444cc09fca091cad4b105341c9e0e37)
- [Added support for augmented field assignment (`y.f += x`).](https://github.com/bazelbuild/bazel/commit/fbbac6b295b9ef0ea1999dc3ae9df29eceffbe88)
- [Added support for `%x`, `%X`, and `%o` conversions in `string % number`.](https://github.com/bazelbuild/bazel/commit/99b72266903d409c4eb9ebc852a4941cce7b0995)
- [Added 64-bit integer support to `StarlarkInt.{floordiv,mod}`.](https://github.com/bazelbuild/bazel/commit/3e459679f158afca39286d8a37179e4e5030ca43)
- [Added support for some subsitutions in action progress messages.](https://github.com/bazelbuild/bazel/commit/bfa364346890eb7950edb002877d569695544d48)
- [Added support for string build settings which accept multiple values.](https://github.com/bazelbuild/bazel/commit/a13f590b69bcbcaa10b1a49bfd9a4607dfbd8f47)
- [Starlark rules can now use native transitions in the `cfg` parameter.](https://github.com/bazelbuild/bazel/commit/d1619b7833461b3e4faf1a9585c0ab6950432a35)
- [Optimized `str(int)`.](https://github.com/bazelbuild/bazel/commit/8f97db114c7a60cc53db0d5bf1555bc580d554a0)
- [Optimized `() + tuple` and `tuple + ()`.](https://github.com/bazelbuild/bazel/commit/0ebb269454722f715dbdaba9ef14c21c11c6fd5f)
- [Optimized long integer multiplication.](https://github.com/bazelbuild/bazel/commit/9d3f2257ba5a97a21c9b3cc0680494c0816c793f)
- [Optimized `&`, `|`, `^`, and `~` for 64-bit integers.](https://github.com/bazelbuild/bazel/commit/fa421b82ceccc54004efd31071dc8c4350245be7)
- [Optimized `list(list)`.](https://github.com/bazelbuild/bazel/commit/d7d7f82729dd62c5c39154140bf1c6a54abdfe84)
- [Optimized Starlark transitions.](https://github.com/bazelbuild/bazel/commit/5b4fb0219b289eb3bbd423748f5400aaab2ec1cb)
- [Optimized `--experimental_existing_rules_immutable_view`.](https://github.com/bazelbuild/bazel/commit/dec8b5a8a0235fe726a4324ff5d02e3abe75a185)
- [Fixed `Label()` behavior when called with `@repo` parts.](https://github.com/bazelbuild/bazel/commit/463e8c80cd11d36777ddf80543aea7c53293f298)
- [Propagated `DefaultInfo` no longer loses runfiles.](https://github.com/bazelbuild/bazel/commit/62582bd74d2a154e94a9e1e64fcbeeae22fbf88c)

[starlarkification]: https://youtu.be/7M9c6x3WgIQ?t=282

## Misc

There were a handful of changes that I couldn't find a nice home for in the sections above,
but I still felt were important or interesting enough to call attention to:

- [Added the the `--incompatible_enforce_config_setting_visibility` flag, which makes `config_setting` honor the `visibility` attribute (defaulting to `//visibility:public`).](https://github.com/bazelbuild/bazel/commit/79989f9becc2edefe8b35f7db687bf8de03e3580)
- [Added the `--remove_all_convenience_symlinks` flag to the `clean` command, which deletes all symlinks in the workspace that have the `symlink_prefix` prefix and point into the output directory.](https://github.com/bazelbuild/bazel/commit/f3513c16018be07255ee51c88bfdf2f77efa920f)
- [Added the `--no-log-init` flag to docker sandbox.](https://github.com/bazelbuild/bazel/commit/d4390f81b306f3ae0b2f85c722965142e14dd356)
- [Added the `--experimental_keep_config_nodes_on_analysis_discard` flag (default on), which reduces long-running memory usage.](https://github.com/bazelbuild/bazel/commit/2f4ed67b2539762a36c4e0a28018d62ef8811477)
- [Added the `--experimental_reuse_sandbox_directories` flag, which causes reuse of already-created non-worker sandboxes with cleanup.](https://github.com/bazelbuild/bazel/commit/1adb51287f5826ac1a8b469c417780c84ea4b467)
- [Added the `--experimental_skyframe_cpu_heavy_skykeys_thread_pool_size` flag, which causes the loading/analysis phase of Skyframe to use 2 independent thread pools.](https://github.com/bazelbuild/bazel/commit/16c040854db04fed900a1d3abb5ff6a4337a2028)
- [Added the `--experimental_oom_sensitive_skyfunctions_semaphore_size` flag, which configures the semaphore in `ConfiguredTagetFunction`.](https://github.com/bazelbuild/bazel/commit/b5bfcc1ed0388fb5f0e3924d8fd33ea1bb3db0e1)
- [Added the `--experimental_retain_test_configuration_across_testonly` flag, which skips configuration trimming when when `testonly` is `true` on non-test targets.](https://github.com/bazelbuild/bazel/commit/309f4e1475efc08b20e406038d10eb30090fca82)
- [Test configurations are no longer trimmed when `--nodistinct_host_configuration` is on.](https://github.com/bazelbuild/bazel/commit/0b51d431bd43010ed4dc56f30b83da20fdb21bc6)
- [The `canonicalize-flags` command now inherits from the `build` command in order to pick up build-specified `--flag_alias` settings from rc files.](https://github.com/bazelbuild/bazel/commit/3e6e97585dd41e31b6ca3bfe3bed10abc3614fe4)
- [Bazel will no longer create a `bazel-out` symlink if `--symlink_prefix` is specified: the directory pointed to via the `bazel-out` symlink is accessible via `${symlink_prefix}-out`.](https://github.com/bazelbuild/bazel/commit/06bd3e8c0cd390f077303be682e9dec7baf17af2)
- [Set `TEST_RUN_NUMBER` when the test runs multiple times.](https://github.com/bazelbuild/bazel/commit/9e4216e0c8c6974f79c9fbea71bd9129b9439066)
- [All (instead of just C++) source files are now filtered for coverage output according to `--instrumentation_filter` and `--instrument_test_targets`.](https://github.com/bazelbuild/bazel/commit/f38e293317088396163115cec07099026d63533e)
- [`genrule.srcs` is now considered a source attribute for coverage.](https://github.com/bazelbuild/bazel/commit/fa118ae4b5d58648db574a08a60d5595d6b645dd)
- [`label_keyed_string_dict` attributes are now considered when gathering instrumented files for coverage.](https://github.com/bazelbuild/bazel/commit/9015f383220892b638d47257ddbd407b2ea07055)
- [Changed `DEFAULT_STUB_SHEBANG` to use python3 instead of python.](https://github.com/bazelbuild/bazel/commit/2945ef5072f659878dfd88b421c7b80aa4fb6c80)
- [Added the default `solib` dir to the `rpath` for shared libs with transitions.](https://github.com/bazelbuild/bazel/commit/20061f8fb7ed95924c6cdbaaaf3d06a64edf974e)
- [Removed `//visibility:legacy_public`.](https://github.com/bazelbuild/bazel/commit/0803ce4cbeb209689bb97f1b5107383ca201e2b6)
- [Removed `--action_graph` from the dump command.](https://github.com/bazelbuild/bazel/commit/7cf0c349b2e1675deea9519be4a805d8daff732e)
- [Added some fixes for the Starlark transition hash computation.](https://github.com/bazelbuild/bazel/commit/557a7e71eeb5396f2c87c909ddc025fde2678780)

## Summary

As you can see,
Bazel 5.0 was a _massive_ release.
Thankfully,
through the [rolling releases][rolling-releases] process,
people were able to test,
or even actively depend on,
these changes well before the first 5.0 release candidate was cut.

I expect Bazel 5.1 to be a fast follow with some changes that missed the final release candidate.
Work on Bazel 6.0 is well underway as well,
and I look forward to summarizing its changes later this year.

[rolling-releases]: https://docs.bazel.build/versions/5.0.0/versioning.html#rolling-releases
