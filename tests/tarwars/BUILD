filegroup(
    name = "bigbad",
    srcs = ["bigbad.tar.gz"],
)

sh_test(
  name = "test",
  srcs = ["test.sh"],
  args = ["$(location :bigbad)"],
  data = [
    ":bigbad",
  ],
)
