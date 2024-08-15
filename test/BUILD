genrule(
    name = "test",
    outs = ["test.txt"],
    cmd = "echo 'test' > $(OUTS)",
    exec_properties = {"dockerCheckpoint": "true"},
)
