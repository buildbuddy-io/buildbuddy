package yaml

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const yamlInput = `
load:
  //rules/typescript:index.bzl: [ts_library, DEFAULT_LANGUAGES]
  //rules/other:index.bzl: [other_library, OTHER_THING]

package:
  default_visibility: ["//visibility:public"]

ts_library:
  name: alert
  srcs:
    - alert.tsx
  strict: true
  deps:
    - "//app/alert:alert_service"
    - "//app/components/banner"
    - "@npm//@types/react"
    - "@npm//react"
    - "@npm//rxjs"

ts_library:
  name: alert_service
  srcs: ["alert_service.ts"]
  strict: true
  deps: ["@npm//rxjs"]
  foo: DEFAULT_LANGUAGES

ts_library:
  - name: my_service
    srcs: 'select({":release_build": ["config/buildbuddy.release.yaml"], "//conditions:default": glob(["config/**"])})'
    strict: false
    deps: ["@npm//my_service"]
  - name: my_other_service
    srcs: 'select({":release_build": ["config/buildbuddy.release.yaml"], "//conditions:default": glob(["config/**"])})'
    strict: false
    deps: ["@npm//my_other_service"]

exports_files:
  srcs: glob(["*.css"])

raw: |
  # here is a raw comment
`

const expectedStarlark = `load("//rules/typescript:index.bzl", "ts_library", "DEFAULT_LANGUAGES")

load("//rules/other:index.bzl", "other_library", "OTHER_THING")

package(default_visibility = ["//visibility:public"])

ts_library(name = "alert", srcs = ["alert.tsx"], strict = True, deps = ["//app/alert:alert_service", "//app/components/banner", "@npm//@types/react", "@npm//react", "@npm//rxjs"])

ts_library(name = "alert_service", srcs = ["alert_service.ts"], strict = True, deps = ["@npm//rxjs"], foo = DEFAULT_LANGUAGES)

ts_library(name = "my_service", srcs = select({":release_build": ["config/buildbuddy.release.yaml"], "//conditions:default": glob(["config/**"])}), strict = False, deps = ["@npm//my_service"])

ts_library(name = "my_other_service", srcs = select({":release_build": ["config/buildbuddy.release.yaml"], "//conditions:default": glob(["config/**"])}), strict = False, deps = ["@npm//my_other_service"])

exports_files(srcs = glob(["*.css"]))

# here is a raw comment

`

func TestTranslate(t *testing.T) {
	translator := New()

	output, err := translator.Translate("input.js", yamlInput)
	assert.Nil(t, err)

	assert.Equal(t, expectedStarlark, output)
}
