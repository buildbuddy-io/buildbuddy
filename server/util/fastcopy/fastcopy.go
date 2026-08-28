package fastcopy

import "flag"

var enableFastcopyReflinking = flag.Bool("executor.enable_fastcopy_reflinking", false, "If true, attempt to use `cp --reflink=auto` to link files")
var useMacOSHardlinks = flag.Bool("executor.use_hardlinks_macos", false, "If true, use hardlinks on macOS instead of copy-on-write clones")
