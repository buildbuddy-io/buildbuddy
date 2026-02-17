package ui

import (
	"fmt"
	"strings"
	"time"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

func formatStatus(inv *inpb.Invocation) string {
	switch inv.GetInvocationStatus() {
	case inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS:
		if inv.GetSuccess() {
			return "Passed"
		}
		return "Failed"
	case inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS:
		return "In progress"
	case inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS:
		return "Disconnected"
	default:
		return "Unknown"
	}
}

func formatDuration(usec int64) string {
	d := time.Duration(usec) * time.Microsecond
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	if m >= 60 {
		h := m / 60
		m = m % 60
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	return fmt.Sprintf("%dm%02ds", m, s)
}

func formatTimeAgo(usec int64) string {
	t := time.UnixMicro(usec)
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		m := int(d.Minutes())
		return fmt.Sprintf("%dm ago", m)
	case d < 24*time.Hour:
		h := int(d.Hours())
		return fmt.Sprintf("%dh ago", h)
	default:
		days := int(d.Hours() / 24)
		return fmt.Sprintf("%dd ago", days)
	}
}

func formatPattern(patterns []string) string {
	if len(patterns) == 0 {
		return ""
	}
	return strings.Join(patterns, " ")
}

func formatBytes(b int64) string {
	switch {
	case b < 1024:
		return fmt.Sprintf("%d B", b)
	case b < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	case b < 1024*1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	default:
		return fmt.Sprintf("%.1f GB", float64(b)/(1024*1024*1024))
	}
}

func formatActiveFilters(f filters) string {
	var chips []string
	if f.user != "" {
		chips = append(chips, filterStyle.Render("user:"+f.user))
	}
	if f.repo != "" {
		chips = append(chips, filterStyle.Render("repo:"+f.repo))
	}
	if f.branch != "" {
		chips = append(chips, filterStyle.Render("branch:"+f.branch))
	}
	if len(chips) == 0 {
		return ""
	}
	return " " + strings.Join(chips, " ")
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
