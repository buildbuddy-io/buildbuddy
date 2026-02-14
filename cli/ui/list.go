package ui

import (
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type listModel struct {
	api          *apiClient
	filters      filters
	table        table.Model
	invocations  []*inpb.Invocation
	loading      bool
	err          error
	width        int
	height       int
	selectedID   string
	switchOrg    bool
	hasOrgPicker bool
}

func newListModel(api *apiClient, f filters, hasOrgPicker bool) listModel {
	columns := []table.Column{
		{Title: "Status", Width: 14},
		{Title: "User", Width: 12},
		{Title: "Command", Width: 8},
		{Title: "Pattern", Width: 30},
		{Title: "Branch", Width: 16},
		{Title: "Duration", Width: 10},
		{Title: "When", Width: 10},
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithFocused(true),
		table.WithHeight(20),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(colorDim).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("#ffffff")).
		Background(lipgloss.Color("#5533cc")).
		Bold(false)
	t.SetStyles(s)

	return listModel{
		api:          api,
		filters:      f,
		table:        t,
		loading:      true,
		hasOrgPicker: hasOrgPicker,
	}
}

func (m listModel) Init() tea.Cmd {
	return tea.Batch(
		m.api.fetchInvocations(m.filters),
		autoRefreshCmd(),
	)
}

func (m listModel) Update(msg tea.Msg) (listModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter":
			if sel := m.table.SelectedRow(); len(sel) > 0 {
				idx := m.table.Cursor()
				if idx >= 0 && idx < len(m.invocations) {
					m.selectedID = m.invocations[idx].GetInvocationId()
				}
			}
			return m, nil
		case "r":
			m.loading = true
			return m, m.api.fetchInvocations(m.filters)
		case "o":
			if m.hasOrgPicker {
				m.switchOrg = true
				return m, nil
			}
		}

	case invocationsMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.err = nil
		cursor := m.table.Cursor()
		m.invocations = msg.invocations
		m.table.SetRows(m.buildRows())
		if cursor < len(m.invocations) {
			m.table.SetCursor(cursor)
		}
		return m, nil

	case tickMsg:
		return m, tea.Batch(
			m.api.fetchInvocations(m.filters),
			autoRefreshCmd(),
		)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.table.SetHeight(msg.Height - 5)
		m.updateColumnWidths()
		return m, nil
	}

	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m *listModel) updateColumnWidths() {
	if m.width < 60 {
		return
	}
	// Fixed columns: Status(14) + User(12) + Command(8) + Duration(10) + When(10) = 54
	// Plus separators ~6
	fixed := 60
	remaining := m.width - fixed
	patternW := remaining * 2 / 3
	branchW := remaining - patternW
	if patternW < 10 {
		patternW = 10
	}
	if branchW < 8 {
		branchW = 8
	}

	columns := []table.Column{
		{Title: "Status", Width: 14},
		{Title: "User", Width: 12},
		{Title: "Command", Width: 8},
		{Title: "Pattern", Width: patternW},
		{Title: "Branch", Width: branchW},
		{Title: "Duration", Width: 10},
		{Title: "When", Width: 10},
	}
	m.table.SetColumns(columns)
}

func (m listModel) buildRows() []table.Row {
	rows := make([]table.Row, len(m.invocations))
	for i, inv := range m.invocations {
		rows[i] = table.Row{
			formatStatus(inv),
			truncate(inv.GetUser(), 12),
			inv.GetCommand(),
			truncate(formatPattern(inv.GetPattern()), 30),
			truncate(inv.GetBranchName(), 16),
			formatDuration(inv.GetDurationUsec()),
			formatTimeAgo(inv.GetUpdatedAtUsec()),
		}
	}
	return rows
}

func (m listModel) View() string {
	title := titleStyle.Render("BuildBuddy Builds")
	filterStr := formatActiveFilters(m.filters)

	var status string
	if m.loading && len(m.invocations) == 0 {
		status = "\n  Loading builds..."
	} else if m.err != nil {
		status = "\n  " + errorStyle.Render("Error: "+m.err.Error())
	}

	helpText := "  ↑/↓/j/k: navigate • enter: view details • r: refresh"
	if m.hasOrgPicker {
		helpText += " • o: switch org"
	}
	helpText += " • q: quit"
	help := helpStyle.Render(helpText)

	return title + filterStr + status + "\n" + m.table.View() + "\n" + help
}
