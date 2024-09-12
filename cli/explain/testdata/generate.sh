#!/usr/bin/env sh

export USE_BAZEL_VERSION=7.3.1
out="$BUILD_WORKSPACE_DIRECTORY/$1"
rm -f "$out"/*.pb.zstd

tmp=$(mktemp -d "$TMPDIR/explain.XXXXXX")
trap "rm -rf $tmp" EXIT
cd "$tmp" || exit 1
trap "bazel clean --expunge" EXIT

touch MODULE.bazel

mkdir -p src/main/java/com/example/lib
cat > src/main/java/com/example/lib/BUILD.bazel <<EOF
java_library(
    name = "lib",
    srcs = ["Lib.java"],
    visibility = ["//visibility:public"],
)
EOF
cat > src/main/java/com/example/lib/Lib.java <<EOF
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Lib";
    }
}
EOF

mkdir -p src/test/java/com/example/lib
cat > src/test/java/com/example/lib/BUILD.bazel <<EOF
java_test(
    name = "lib_test",
    srcs = ["LibTest.java"],
    test_class = "com.example.lib.LibTest",
    deps = ["//src/main/java/com/example/lib"],
)
EOF
cat > src/test/java/com/example/lib/LibTest.java <<EOF
package com.example.lib;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class LibTest {
    @Test
    public void testGetName() {
        assertEquals("Lib", Lib.getName());
    }
}
EOF

mkdir -p src/main/java/com/example/app
cat > src/main/java/com/example/app/BUILD.bazel <<EOF
java_library(
    name = "app_lib",
    srcs = ["App.java"],
    deps = ["//src/main/java/com/example/lib"],
)

java_binary(
    name = "app",
    runtime_deps = [":app_lib"],
)
EOF
cat > src/main/java/com/example/app/App.java <<EOF
package com.example.app;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello, " + com.example.lib.Lib.getName() + "!");
    }
}
EOF

bazel --nohome_rc --nosystem_rc build //src/main/java/com/example/app:app --experimental_execution_log_compact_file="$out/baseline.pb.zstd"
bazel --nohome_rc --nosystem_rc build //src/test/java/com/example/lib:lib_test --experimental_execution_log_compact_file="$out/build_test.pb.zstd"
bazel --nohome_rc --nosystem_rc test //src/test/java/com/example/lib:lib_test --experimental_execution_log_compact_file="$out/run_test.pb.zstd"
cat > src/main/java/com/example/lib/Lib.java <<EOF
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Lib"; // no-op change
    }
}
EOF
bazel --nohome_rc --nosystem_rc build //src/main/java/com/example/app:app --experimental_execution_log_compact_file="$out/build_noop_change.pb.zstd"
cat > src/main/java/com/example/lib/Lib.java <<EOF
package com.example.lib;

public class Lib {
    public static String getName() {
      return "Bar";
    }
}
EOF
bazel --nohome_rc --nosystem_rc build //src/main/java/com/example/app:app --experimental_execution_log_compact_file="$out/build_transitive_change.pb.zstd"
