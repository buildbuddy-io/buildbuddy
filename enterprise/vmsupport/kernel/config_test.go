package kernel_test

import (
	"embed"
	"strings"
	"testing"
)

//go:embed microvm-kernel-*.config
var configs embed.FS

func TestGuestKernelConfigsEnableEBPF(t *testing.T) {
	requiredOptions := []string{
		"CONFIG_BPF_SYSCALL=y",
		"CONFIG_BPF_JIT=y",
		"CONFIG_BPF_UNPRIV_DEFAULT_OFF=y",
		"CONFIG_CGROUP_BPF=y",
		"CONFIG_DEBUG_FS=y",
		"CONFIG_KPROBES=y",
		"CONFIG_KPROBE_EVENTS=y",
		"CONFIG_UPROBES=y",
		"CONFIG_UPROBE_EVENTS=y",
		"CONFIG_BPF_EVENTS=y",
		"CONFIG_TRACING=y",
		"CONFIG_NETFILTER_XT_MATCH_BPF=y",
		"CONFIG_NET_CLS_BPF=y",
		"CONFIG_NET_ACT_BPF=y",
	}
	disabledOptions := []string{
		"# CONFIG_MODULES is not set",
	}

	configNames := []string{
		"microvm-kernel-aarch64-v5.10.config",
		"microvm-kernel-x86_64-v5.15.config",
		"microvm-kernel-x86_64-v6.1.config",
	}
	for _, name := range configNames {
		t.Run(name, func(t *testing.T) {
			b, err := configs.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			lines := map[string]struct{}{}
			for line := range strings.SplitSeq(string(b), "\n") {
				lines[line] = struct{}{}
			}
			for _, opt := range requiredOptions {
				if _, ok := lines[opt]; !ok {
					t.Fatalf("missing %s", opt)
				}
			}
			for _, opt := range disabledOptions {
				if _, ok := lines[opt]; !ok {
					t.Fatalf("missing %s", opt)
				}
			}
		})
	}
}
