package fastcopy

import "flag"

var enableFastcopyReflinking = flag.Bool("executor.enable_fastcopy_reflinking", false, "If true, attempt to use `cp --reflink=auto` to link files")
