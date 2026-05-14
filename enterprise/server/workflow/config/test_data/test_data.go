package test_data

import _ "embed"

//go:embed basic.yaml
var BasicYaml []byte

//go:embed yaml_with_run_block.yaml
var YamlWithRunBlock []byte
