// From https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/ExitCode.java#L38
export function exitCode(exitCode: string) {
  switch (exitCode) {
    case "SUCCESS":
      return "Succeeded";
    case "BUILD_FAILURE":
      return "Build failed";
    case "PARSING_FAILURE":
      return "Parsing failed";
    case "COMMAND_LINE_ERROR":
      return "Bad command line";
    case "TESTS_FAILED":
      return "Test failed";
    case "PARTIAL_ANALYSIS_FAILURE":
    case "ANALYSIS_FAILURE":
      return "Analysis failed";
    case "NO_TESTS_FOUND":
      return "No tests found";
    case "RUN_FAILURE":
      return "Run failed";
    case "INTERRUPTED":
      return "Interrupted";
    case "REMOTE_ERROR":
    case "REMOTE_ENVIRONMENTAL_ERROR":
      return "Remote failure";
    case "LOCAL_ENVIRONMENTAL_ERROR":
      return "Local environment failure";
    case "OOM_ERROR":
      return "Out of memory";
    case "BLAZE_INTERNAL_ERROR":
      return "Bazel internal failure";
    case "REMOTE_CACHE_EVICTED":
      return "Cache eviction";
    case "PUBLISH_ERROR":
    case "PERSISTENT_BUILD_EVENT_SERVICE_UPLOAD_ERROR":
      return "Build event failure";
    case "EXTERNAL_DEPS_ERROR":
      return "External dep failure";
  }
  return "Failed";
}
