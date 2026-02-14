package ui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type detailModel struct {
	api          *apiClient
	invocationID string
	invocation   *inpb.Invocation
	logs         string
	viewport     viewport.Model
	loading      bool
	err          error
	width        int
	height       int
	goBack       bool
	ready        bool
}

func newDetailModel(api *apiClient, invocationID string) detailModel {
	return detailModel{
		api:          api,
		invocationID: invocationID,
		loading:      true,
	}
}

func (m detailModel) Init() tea.Cmd {
	return tea.Batch(
		m.api.fetchInvocationDetail(m.invocationID),
		m.api.fetchBuildLogs(m.invocationID),
	)
}

func (m detailModel) Update(msg tea.Msg) (detailModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc":
			m.goBack = true
			return m, nil
		}

	case invocationDetailMsg:
		if msg.err != nil {
			m.err = msg.err
			m.loading = false
			return m, nil
		}
		m.invocation = msg.invocation
		m.loading = m.logs == "" && m.invocation == nil
		m.updateContent()
		return m, nil

	case buildLogsMsg:
		if msg.err != nil {
			// Non-fatal: just show what we have
			m.logs = fmt.Sprintf("(error loading logs: %s)", msg.err)
		} else {
			m.logs = msg.logs
		}
		m.loading = false
		m.updateContent()
		return m, nil

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		headerHeight := 3
		if !m.ready {
			m.viewport = viewport.New(msg.Width, msg.Height-headerHeight)
			m.viewport.YPosition = headerHeight
			m.ready = true
		} else {
			m.viewport.Width = msg.Width
			m.viewport.Height = msg.Height - headerHeight
		}
		m.updateContent()
		return m, nil
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m *detailModel) updateContent() {
	if !m.ready {
		return
	}
	m.viewport.SetContent(m.renderContent())
}

func (m detailModel) renderContent() string {
	if m.invocation == nil {
		if m.err != nil {
			return errorStyle.Render("Error: " + m.err.Error())
		}
		return "Loading..."
	}

	var b strings.Builder
	inv := m.invocation

	// Metadata section
	b.WriteString(sectionStyle.Render("Build Metadata"))
	b.WriteString("\n")
	writeField(&b, "User", inv.GetUser())
	writeField(&b, "Host", inv.GetHost())
	writeField(&b, "Command", inv.GetCommand()+" "+formatPattern(inv.GetPattern()))
	writeField(&b, "Duration", formatDuration(inv.GetDurationUsec()))
	writeField(&b, "Branch", inv.GetBranchName())
	writeField(&b, "Commit", truncate(inv.GetCommitSha(), 12))
	writeField(&b, "Repo", inv.GetRepoUrl())
	writeField(&b, "Exit Code", inv.GetBazelExitCode())
	writeField(&b, "Actions", fmt.Sprintf("%d", inv.GetActionCount()))

	// Cache stats
	cs := inv.GetCacheStats()
	if cs != nil {
		b.WriteString("\n")
		b.WriteString(sectionStyle.Render("Cache Stats"))
		b.WriteString("\n")

		acTotal := cs.GetActionCacheHits() + cs.GetActionCacheMisses()
		if acTotal > 0 {
			hitRate := float64(cs.GetActionCacheHits()) / float64(acTotal) * 100
			writeField(&b, "AC Hit Rate", fmt.Sprintf("%.0f%% (%d/%d)", hitRate, cs.GetActionCacheHits(), acTotal))
		}
		writeField(&b, "CAS Hits", fmt.Sprintf("%d", cs.GetCasCacheHits()))
		writeField(&b, "CAS Misses", fmt.Sprintf("%d", cs.GetCasCacheMisses()))
		writeField(&b, "Downloaded", formatBytes(cs.GetTotalDownloadSizeBytes()))
		writeField(&b, "Uploaded", formatBytes(cs.GetTotalUploadSizeBytes()))
		if cs.GetTotalCachedActionExecUsec() > 0 {
			writeField(&b, "Time Saved", formatDuration(cs.GetTotalCachedActionExecUsec()))
		}
	}

	// Build logs
	b.WriteString("\n")
	b.WriteString(sectionStyle.Render("Build Logs"))
	b.WriteString("\n")
	if m.logs != "" {
		b.WriteString(m.logs)
	} else {
		b.WriteString("Loading logs...")
	}

	return b.String()
}

func writeField(b *strings.Builder, label, value string) {
	if value == "" {
		return
	}
	b.WriteString(labelStyle.Render(label) + valueStyle.Render(value) + "\n")
}

func (m detailModel) View() string {
	if !m.ready {
		return "Initializing..."
	}

	inv := m.invocation
	var header string
	if inv != nil {
		header = titleStyle.Render(formatStatus(inv)+" "+inv.GetCommand()+" "+truncate(formatPattern(inv.GetPattern()), 40)) + "\n"
	} else {
		header = titleStyle.Render("Loading build "+m.invocationID) + "\n"
	}

	help := helpStyle.Render("  ↑/↓/j/k: scroll • q/esc: back to list")

	return header + m.viewport.View() + "\n" + help
}
