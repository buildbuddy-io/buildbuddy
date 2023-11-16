package(default_visibility = ["//visibility:public"])

genrule(
    name = "rm-enterprise",
    outs = ["rm-enterprise.sh"],
    cmd = """
    cat <<'EOF' > $@
#!/bin/bash
rm -rf $$BUILD_WORKING_DIRECTORY/enterprise
EOF
""",
    executable = True,
)
