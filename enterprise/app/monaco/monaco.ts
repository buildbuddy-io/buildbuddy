export function getLangHintFromFilePath(path: string) {
  if (!path) {
    return undefined;
  }
  if (
    path.endsWith(".bazel") ||
    path.endsWith("WORKSPACE") ||
    path.endsWith("BUILD") ||
    path.endsWith("MODULE") ||
    path.endsWith(".bzl")
  ) {
    return "python";
  }
  return undefined;
}
