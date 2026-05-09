package fix

import "testing"

func TestMacroNameForDepFile(t *testing.T) {
	for _, tc := range []struct {
		path, want string
	}{
		{"go.mod", "install_go_mod_dependencies"},
		{"sub/dir/go.mod", "install_sub_dir_go_mod_dependencies"},
		{"weird-name.mod", "install_weird_name_mod_dependencies"},
		{"a.b.c", "install_a_b_c_dependencies"},
		{"already_ok", "install_already_ok_dependencies"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			if got := macroNameForDepFile(tc.path); got != tc.want {
				t.Errorf("macroNameForDepFile(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}
