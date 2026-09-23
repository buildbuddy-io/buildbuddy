"""General-purpose helpers for BuildBuddy rules."""

# buildifier: disable=unnamed-macro
def runfiles_directory():
    # The main repository has an empty canonical repo name, but a special
    # fixed runfiles directory name.
    return native.repo_name() or "_main"
