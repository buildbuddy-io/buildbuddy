---
slug: lldb-stumbling
title: "Stumbling in LLDB"
description: "Different ways to fail using LLDB with Bazel"
authors: son
date: 2025-03-14:12:00:00
image: /img/blog/unusual.png
tags: [bazel, engineering]
---

After multiple fail attempts to use Bazel to produce a cc_binary and debug it with LLDB, I finally found the best solution: use somebody else's code.

Come with me and let's explore the different ways to fail using LLDB with Bazel.

<!-- truncate -->

## Introduction

At BuildBuddy we have multiple experiments cooking in dev. One of them is to play with [Google's Kythe]().

If you are unfamiliar with Kythe, well I don't blame you.
Internally known as Grok, Kythe is a pipeline that transforms source code into a universal AST.
That's right, one AST for Java, Go, Protobuf, C++, Javascript, Rust and more.

> Why would you want to do that? 🤔

This is used to power the code navigation in Google's internal code search tool.
You can get a taste of it here by opening this file [ActionExecutionFunction.java](https://cs.opensource.google/bazel/bazel/+/master:src/main/java/com/google/devtools/build/lib/skyframe/ActionExecutionFunction.java) and click around to find references, definitions, and more.
It's like mordern LSP/LSIF but for all languages, and predates LSP by a few years.

The special thing about Kythe is that it has native integration with Bazel.
By leveraging `--experimental_action_listener` flag, Kythe insert extra actions that shadows your original graph.
There, Kythe distils source code dependencies into multiple bundles that are later used to construct the universal graph representing your source code.

> I have not heard of `--experimental_action_listener` before, what is it?

It's a deprecated features in Bazel. The mordern replacement for it is [Bazel's Aspect](https://bazel.build/extending/aspects) which Kythe has yet to migrate to.

## The Broken Experiment

So yeah, we are experimenting with Kythe to see how to run it with Bazel.

Part of the experiment is to setup a BuildBuddy Workflows pipeline that run Kythe on our repository regularly.
And recently, as I was [upgrading several dependencies]() in our repos, that Kythe pipeline broke.

I won't bore you with all the problems encountered, but here is one that was particularly minor, yet took me down a deep rabbit hole. Here is a non-critial error we encountered repeatedly in our Kythe runs:

```
E0314 12:35:38.004348 1941520 file_descriptor_walker.cc:262] Unexpected location vector [] while walking some/path/file.proto
```

This error is thrown by Kythe's indexer when it tries to parse a `.proto` file and fails to find the location of a symbol.
The error is not critical, but it's annoying to see it repeated in the logs so many times.

So I dived in.

> Here we go again 🕳️

## The Rabbit Hole

First, we can inspect the proto files that is causing error.
Particularly, not all proto files in our repo was flagged by Kythe indexer, only a handful few.
By inspecting the suspects, I found that the common pattern was that these proto files were all `proto3` and had an `optional` field somewhere in them.
With that, we can create a minimal reproducible example.

```proto title=test.proto
syntax = "proto3";

package moo;

message Foo {
  optional int32 bar = 1;
}
```

Next, we need the Kythe indexer to run on this file.
The one we used in CI was a Linux binary while I was developing on a MacOS machine.
So I decided to build the indexer locally from source

```bash
> bazel build kythe/cxx/indexer/proto:indexer
...
INFO: Found 1 target...
Target //kythe/cxx/indexer/proto:indexer up-to-date:
  bazel-bin/kythe/cxx/indexer/proto/indexer
INFO: Elapsed time: 1.207s, Critical Path: 0.15s
INFO: 1184 processes: 745 action cache hit, 796 disk cache hit, 388 internal.
INFO: Build completed successfully, 1184 total actions

> bazel-bin/kythe/cxx/indexer/proto/indexer test.proto 1>/dev/null
E0314 12:49:25.570792 1954839 file_descriptor_walker.cc:262] Unexpected location vector [] while walking test.proto
```

Ok that's a good sign, the error is reproducible locally.

Now, typically I will start with skimming through file_descriptor_walker.cc and see if I can understand what's going on at line 262.
However, it's 2025, we have ~~LLDB~~ LLM based debugging.

### The LLM Fix

So let's try using Google Gemini to debug Google Kythe.

```bash
> tail -n +1 kythe/cxx/indexer/proto/file_descriptor_walker.cc test.proto |\
    llm -m gemini-2.0-flash-thinking-exp-01-21 \
       'I am getting this error `Unexpected location vector [] while walking` when I run it against test.proto'
```

and here is the output

```markdown
...
To fix this, we need to check if a path exists in `location_map_` before attempting to access its corresponding span. If the path is not found, it means there's no location information for that element in the `SourceCodeInfo`, and we should handle this gracefully, possibly by using a default or empty location, instead of causing an error.

Here's the modification to `FileDescriptorWalker::InitializeLocation` and the places where it's called to handle cases where the location is not found in `location_map_`.

**Modified `InitializeLocation` Function:**

...

**Modified Call Sites (Example - you need to modify all call sites similarly):**

...

**Apply this change to all locations where `InitializeLocation` is called, especially in:**

* `VisitImports`
* `VisitField`
* `VisitFields`
* `VisitNestedEnumTypes`
* `VisitNestedTypes`
* `VisitOneofs`
* `VisitMessagesAndEnums`
* `VisitEnumValues`
* `VisitAllFields`
* `VisitExtension`
* `VisitNestedFields`
* `VisitRpcServices`

**Reasoning for the fix:**

...
```

The AI was correctly able to identify the problem was inside the `FileDescriptorWalker:InitializeLocation` function, which looks like this

```cpp
void FileDescriptorWalker::InitializeLocation(const std::vector<int>& span,
                                              Location* loc) {
  loc->file = file_name_;
  absl::StatusOr<PartialLocation> possible_location = ParseLocation(span);
  if (possible_location.ok()) {
    PartialLocation partial_location = *possible_location;
    loc->begin = ComputeByteOffset(partial_location.start_line,
                                   partial_location.start_column);
    loc->end = ComputeByteOffset(partial_location.end_line,
                                 partial_location.end_column);
  } else {
    // Some error in the span, create a dummy location for now
    // Happens in case of proto1 files
    LOG(ERROR) << "Unexpected location vector [" << absl::StrJoin(span, ":")
               << "] while walking " << file_name_.path();
    loc->begin = 0;
    loc->end = 0;
  }
}

absl::StatusOr<PartialLocation> FileDescriptorWalker::ParseLocation(
    const std::vector<int>& span) const {
  PartialLocation location;
  if (span.size() == 4) {
    location.start_line = span[0] + 1;
    ...
  } else if (span.size() == 3) {
    location.start_line = span[0] + 1;
    ...
  } else {
    return absl::UnknownError("");
  }
  return location;
}
```

So it looks like the error is thrown when `ParseLocation` fails to parse the span.
And that would happen when the span is not of size 3 or 4.

However, the suggested fix was a decent sized refactoring of the `InitializeLocation` function which I have no interested in.
This error only happens to a few proto files, so it's quite clear that not all call sites of `InitializeLocation` are broken.

We need a better way to debug this.

### The PrintF Debugging

One thing Gemini got right was that there are only a handlful of parent `Visit*` functions that called `InitializeLocation`.
Most of which looks something like this 

```cpp
const std::vector<int>& span = location_map_[lookup_path];
Location location;
InitializeLocation(span, &location);
```

Where the `location_map_` is a private map of `FileDescriptorWalker` class defined in `file_descriptor_walker.h`.
This means we can add a `printf` statement to print out the function name when span is not of size 3 or 4.

```cpp
const std::vector<int>& span = location_map_[lookup_path];
if (span.size() != 3 && span.size() != 4) {
  LOG(ERROR) << "SLUONGNG: Unexpected span size " << span.size() << " in " << __FUNCTION__;
}
Location location;
InitializeLocation(span, &location);
```

and rebuild + rerun the indexer

```bash
> bazel build kythe/cxx/indexer/proto:indexer
INFO: Invocation ID: 5dac7891-1f84-467e-b613-f52df25c70a4
INFO: Analyzed target //kythe/cxx/indexer/proto:indexer (0 packages loaded, 0 targets configured).
INFO: Found 1 target...
Target //kythe/cxx/indexer/proto:indexer up-to-date:
  bazel-bin/kythe/cxx/indexer/proto/indexer
INFO: Elapsed time: 2.778s, Critical Path: 2.62s
INFO: 3 processes: 10 action cache hit, 1 internal, 2 darwin-sandbox.
INFO: Build completed successfully, 3 total actions

> bazel-bin/kythe/cxx/indexer/proto/indexer test.proto 1>/dev/null
E0314 14:50:38.588045 2027931 file_descriptor_walker.cc:648] SLUONGNG: Unexpected span size 0 in VisitOneofs
E0314 14:50:38.588135 2027931 file_descriptor_walker.cc:262] Unexpected location vector [] while walking test.proto
```

And there we have it, the error is thrown in `VisitOneofs` function. 

> This looks nice and clean inside a blog post.
> But in reality, I totally just print `SLUONGNG 1`, `SLUONGNG 2`, `SLUONGNG 3`... and run the indexer to see which number was printed last before the error printed.
> I am very grateful that this code path is not parallelized otherwise such approach would never have worked.

### Why Oneofs?

Now if you recall, our reduced proto file looks like this

```proto title=test.proto
syntax = "proto3";

package moo;

message Foo {
  optional int32 bar = 1;
}
```

So there is no `oneof` in this file, why is VisitOneofs being called?
Searching for occurences of `VisitOneofs` in the file, we see the following pattern.

```cpp
void FileDescriptorWalker::VisitNestedTypes(...) {
  ...
  for (int i = 0; i < dp->nested_type_count(); i++) {
    ...
    // Need to visit nested enum and message types first!
    VisitNestedTypes(vname, &v_name, nested_proto, lookup_path);
    VisitNestedEnumTypes(vname, &v_name, nested_proto, lookup_path);
    VisitOneofs(vname, v_name, nested_proto, lookup_path);
  }
}

void FileDescriptorWalker::VisitOneofs(const std::string& message_name,
                                       const VName& message,
                                       const Descriptor* dp,
                                       std::vector<int> lookup_path) {
  ScopedLookup nested_type_num(&lookup_path,
                               DescriptorProto::kOneofDeclFieldNumber);

  for (int i = 0; i < dp->oneof_decl_count(); i++) {
    ScopedLookup nested_index(&lookup_path, i);
    const OneofDescriptor* oneof = dp->oneof_decl(i);
    std::string vname = absl::StrCat(message_name, ".", oneof->name());

    VName v_name = builder_->VNameForDescriptor(oneof);
    AddComments(v_name, lookup_path);

    {
      // TODO: verify that this is correct for oneofs
      ScopedLookup name_num(&lookup_path, DescriptorProto::kNameFieldNumber);

      const std::vector<int>& span = location_map_[lookup_path];
      if (span.size() != 3 && span.size() != 4) {
        LOG(ERROR) << "SLUONGNG: Unexpected span size " << span.size() << " in " << __FUNCTION__;
      }
      Location location;
      InitializeLocation(span, &location);

      builder_->AddOneofToMessage(message, v_name, location);
      AttachMarkedSource(v_name,
                         GenerateMarkedSourceForDescriptor(oneof, builder_));
    }

    // No need to add fields; they're also fields of the message
  }
}

void FileDescriptorWalker::VisitMessagesAndEnums(...) {
  ...
  for (int i = 0; i < file_descriptor_->message_type_count(); i++) {
    ...
    // Visit nested types first and fields later for easy type resolution
    VisitNestedTypes(vname, &v_name, dp, lookup_path);
    VisitNestedEnumTypes(vname, &v_name, dp, lookup_path);
    VisitOneofs(vname, v_name, dp, lookup_path);
  }
  ...
}
```

So it looks like `VisitOneofs` is called in `VisitMessagesAndEnums` and `VisitNestedTypes` indiscriminately.
The implementation relies on `dp->oneof_decl_count()` to determine that there were no oneofs to visit and exits early.

However, our debug message was printed once, which means that `dp->oneof_decl_count()` was not zero 🤔.

At this point I was quite puzzled and consulted with Gemini again.
It suggested me to wrap the `InitializeLocation` call in an if statement that validates `span` instead.

> Fine, let's do this the old school way:
> Google it!

Turns out the first Google result for `oneof_decl_count` was the doc page for [OneofDescriptor](https://protobuf.dev/reference/cpp/api-docs/google.protobuf.descriptor/),
which immediately made me aware of an awefully similar sibbling function:

```
int	oneof_decl_count() const
The number of oneofs in this message type.

int	real_oneof_decl_count() const
The number of oneofs in this message type, excluding synthetic oneofs. more...
```

> Hmmm 🤔

The right answer is actually the third search result, [implementing_proto3_presence.md](https://github.com/protocolbuffers/protobuf/blob/main/docs/implementing_proto3_presence.md),
which said:

```markdown
#### To iterate over all oneofs

Old:

```c++
bool IterateOverOneofs(const google::protobuf::Descriptor* message) {
  for (int i = 0; i < message->oneof_decl_count(); i++) {
    const google::protobuf::OneofDescriptor* oneof = message->oneof(i);
    // ...
  }
}
```

New:

```c++
bool IterateOverOneofs(const google::protobuf::Descriptor* message) {
  // Real oneofs are always first, and real_oneof_decl_count() will return the
  // total number of oneofs, excluding synthetic oneofs.
  for (int i = 0; i < message->real_oneof_decl_count(); i++) {
    const google::protobuf::OneofDescriptor* oneof = message->oneof(i);
    // ...
  }
}
```
```

So it looks like `oneof_decl_count` was a relic since proto2 days and `real_oneof_decl_count` is the new way to go.

### The Fix

With that in mind, we can put together the fix as follow

```diff
--- a/kythe/cxx/indexer/proto/file_descriptor_walker.cc
+++ b/kythe/cxx/indexer/proto/file_descriptor_walker.cc
@@ -631,7 +631,7 @@ void FileDescriptorWalker::VisitOneofs(const std::string& message_name,
   ScopedLookup nested_type_num(&lookup_path,
                                DescriptorProto::kOneofDeclFieldNumber);

-  for (int i = 0; i < dp->oneof_decl_count(); i++) {
+  for (int i = 0; i < dp->real_oneof_decl_count(); i++) {
     ScopedLookup nested_index(&lookup_path, i);
     const OneofDescriptor* oneof = dp->oneof_decl(i);
     std::string vname = absl::StrCat(message_name, ".", oneof->name());
```

and test it with 

```bash
> bazel build kythe/cxx/indexer/proto:indexer
INFO: Invocation ID: e3e372f1-4e14-441b-8adb-faf409257ddc
INFO: Analyzed target //kythe/cxx/indexer/proto:indexer (0 packages loaded, 0 targets configured).
INFO: Found 1 target...
Target //kythe/cxx/indexer/proto:indexer up-to-date:
  bazel-bin/kythe/cxx/indexer/proto/indexer
INFO: Elapsed time: 2.170s, Critical Path: 2.03s
INFO: 3 processes: 10 action cache hit, 1 internal, 2 darwin-sandbox.
INFO: Build completed successfully, 3 total actions

> bazel-bin/kythe/cxx/indexer/proto/indexer test.proto 1>/dev/null
>
```

tada! 🎉 no more error.

This was submitted upstream as [PR#6161](https://github.com/kythe/kythe/pull/6161).

### The Aftermath

Well that's not what you are here for is it?
I promissed you LLDB, not LLM and printf, but this is really how it went down.

I got the bug, fixed the bug, went to dinner and... can't stop thinking about the bug.

> What if I cannot use AI? What if Googling doesn't work?
> What would I do instead?

Truth is that before adding the `printf` statements all over the places, I wanted to add only one `printf` statement in `InitializeLocation` function.
I want that `printf` statement to print out the stack trace of the current function call.
That way I can see the call chain that led to the error immediately without copy-pasting `printf` statements all over the place.

However, I could not find an easy way to print out the stack trace without additional dependencies and heavy modifications to the code.
And at this point, with an induced food comma from dinner, my mind wandered off to simpler solutions: 

> what if I have a debugger that let me inspect the stack trace?

I am on a MacOS laptop, so GDB, my familiar debugger is not available.
I will need to learn LLDB instead.

### LLDB 101

### Honey, where is my dSYM?

### Aquery Diff

### The best practice

### Conclusion
