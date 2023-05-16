package builtins

var Rules = []string{
	// https://docs.bazel.build/versions/master/be/android.html
	"aar_import",
	"android_binary",
	"android_device",
	"android_instrumentation_test",
	"android_library",
	"android_local_test",
	"android_ndk_repository",
	"android_sdk_repository",

	// https://docs.bazel.build/versions/master/be/c-cpp.html
	"cc_binary",
	"cc_import",
	"cc_library",
	"cc_proto_library",
	"cc_test",
	"cc_toolchain",
	"cc_toolchain_suite",
	"fdo_prefetch_hints",
	"fdo_profile",

	// https://docs.bazel.build/versions/master/be/java.html
	"java_binary",
	"java_import",
	"java_library",
	"java_lite_proto_library",
	"java_package_configuration",
	"java_plugin",
	"java_proto_library",
	"java_runtime",
	"java_test",
	"java_toolchain",

	// https://docs.bazel.build/versions/master/be/objective-c.html
	"apple_binary",
	"apple_static_library",
	"j2objc_library",
	"objc_import",
	"objc_library",
	"objc_proto_library",

	// https://docs.bazel.build/versions/master/be/protocol-buffer.html
	"proto_lang_toolchain",
	"proto_library",

	// https://docs.bazel.build/versions/master/be/python.html
	"py_binary",
	"py_library",
	"py_runtime",
	"py_test",

	// https://docs.bazel.build/versions/master/be/shell.html
	"sh_binary",
	"sh_library",
	"sh_test",

	// https://docs.bazel.build/versions/master/be/extra-actions.html
	"action_listener",
	"extra_action",

	// https://docs.bazel.build/versions/master/be/general.html
	"alias",
	"config_setting",
	"filegroup",
	"genquery",
	"genrule",
	"test_suite",

	// https://docs.bazel.build/versions/master/be/platform.html
	"constraint_setting",
	"constraint_value",
	"platform",
	"toolchain",
	"toolchain_type",

	// Functions used toplevelin BUILD files
	"exports_files",
	"package",
	"package_group",

	// Functions used toplevel in MODULE files
	"archive_override",
	"bazel_dep",
	"git_override",
	"local_path_override",
	"module",
	"multiple_version_override",
	"register_execution_platforms",
	"register_toolchains",
	"single_version_override",
	"use_extension",
	"use_repo",

	// Functions used toplevel in WORKSPACE files
	"bind",
	"register_execution_platforms",
	"register_toolchains",
	"workspace",
	"local_repository",
	"new_local_repository",
}

var AllGlobals = []string{
	"all",
	"any",
	"bool",
	"dict",
	"dir",
	"enumerate",
	"fail",
	"float",
	"getattr",
	"hasattr",
	"hash",
	"int",
	"len",
	"list",
	"max",
	"min",
	"print",
	"range",
	"repr",
	"reversed",
	"sorted",
	"str",
	"tuple",
	"type",
	"zip",
}

var BuildGlobals = []string{
	"depset",
	"existing_rule",
	"existing_rules",
	"glob",
	"module_name",
	"module_version",
	"package_name",
	"package_relative_label",
	"repository_name",
	"select",
	"subpackages",
}

var BzlGlobal = []string{
	"analysis_test_transition",
	"aspect",
	"configuration_field",
	"depset",
	"exec_group",
	"module_extension",
	"provider",
	"repository_rule",
	"rule",
	"select",
	"tag_class",
	"visiblity",
}
