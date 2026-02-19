package ui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	lgtable "github.com/charmbracelet/lipgloss/table"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type listModel struct {
	api          *apiClient
	filters      filters
	invocations  []*inpb.Invocation
	loading      bool
	err          error
	width        int
	height       int
	cursor       int
	offset       int
	selectedID   string
	switchOrg    bool
	hasOrgPicker bool
}

type listColumnWidths struct {
	status   int
	user     int
	command  int
	pattern  int
	branch   int
	duration int
	when     int
}

func newListModel(api *apiClient, f filters, hasOrgPicker bool) listModel {
	return listModel{
		api:          api,
		filters:      f,
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
		case "up", "k":
			m.moveCursor(-1)
			return m, nil
		case "down", "j":
			m.moveCursor(1)
			return m, nil
		case "enter":
			if m.cursor >= 0 && m.cursor < len(m.invocations) {
				m.selectedID = m.invocations[m.cursor].GetInvocationId()
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
		m.invocations = msg.invocations
		m.clampCursorAndOffset()
		return m, nil

	case tickMsg:
		return m, tea.Batch(
			m.api.fetchInvocations(m.filters),
			autoRefreshCmd(),
		)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.clampCursorAndOffset()
		return m, nil
	}

	return m, nil
}

func (m *listModel) moveCursor(delta int) {
	if len(m.invocations) == 0 {
		return
	}
	m.cursor += delta
	m.clampCursorAndOffset()
}

func (m *listModel) clampCursorAndOffset() {
	if len(m.invocations) == 0 {
		m.cursor = 0
		m.offset = 0
		return
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= len(m.invocations) {
		m.cursor = len(m.invocations) - 1
	}

	visibleRows := m.visibleRows()
	maxOffset := len(m.invocations) - visibleRows
	if maxOffset < 0 {
		maxOffset = 0
	}
	if m.offset < 0 {
		m.offset = 0
	}
	if m.offset > maxOffset {
		m.offset = maxOffset
	}
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+visibleRows {
		m.offset = m.cursor - visibleRows + 1
	}
}

func (m listModel) visibleRows() int {
	rows := m.tableAreaHeight() - 2 // account for header row + header separator
	if rows < 1 {
		return 1
	}
	return rows
}

func (m listModel) showStatusLine() bool {
	return (m.loading && len(m.invocations) == 0) || m.err != nil
}

func (m listModel) tableAreaHeight() int {
	// title/filter line + help line are always visible.
	h := m.height - 2
	// Loading / error status line is conditionally visible.
	if m.showStatusLine() {
		h--
	}
	if h < 3 {
		return 3
	}
	return h
}

func (m listModel) columnWidths() listColumnWidths {
	widths := listColumnWidths{
		status:   14,
		user:     12,
		command:  14,
		pattern:  30,
		branch:   16,
		duration: 10,
		when:     12,
	}

	if m.width <= 0 {
		return widths
	}

	// Non-resizable content widths: Status(14) + User(12) + Command(14) + Duration(10) + When(12) = 62
	// Pattern and Branch consume the remaining width.
	fixed := 62
	remaining := m.width - fixed
	patternW := remaining * 2 / 3
	branchW := remaining - patternW
	if patternW < 10 {
		patternW = 10
	}
	if branchW < 8 {
		branchW = 8
	}
	widths.pattern = patternW
	widths.branch = branchW

	return widths
}

func (m listModel) buildRows(widths listColumnWidths) [][]string {
	rows := make([][]string, len(m.invocations))
	for i, inv := range m.invocations {
		rows[i] = []string{
			formatStatus(inv),
			truncate(inv.GetUser(), widths.user),
			truncate(inv.GetCommand(), widths.command),
			truncate(formatPattern(inv.GetPattern()), widths.pattern),
			truncate(inv.GetBranchName(), widths.branch),
			formatDuration(inv.GetDurationUsec()),
			formatTimeAgo(inv.GetUpdatedAtUsec()),
		}
	}
	return rows
}

func (m listModel) renderTable() string {
	widths := m.columnWidths()
	allRows := m.buildRows(widths)
	tableHeight := m.tableAreaHeight()
	start := m.offset
	if start < 0 {
		start = 0
	}
	if start > len(allRows) {
		start = len(allRows)
	}
	end := start + (tableHeight - 2)
	if end < start {
		end = start
	}
	if end > len(allRows) {
		end = len(allRows)
	}
	visibleRows := allRows[start:end]

	t := lgtable.New().
		Headers("Status", "User", "Command", "Pattern", "Branch", "Duration", "When").
		Rows(visibleRows...).
		Border(lipgloss.NormalBorder()).
		BorderTop(false).
		BorderBottom(false).
		BorderLeft(false).
		BorderRight(false).
		BorderColumn(false).
		BorderRow(false).
		BorderHeader(true).
		BorderStyle(lipgloss.NewStyle().Foreground(colorDim)).
		Wrap(false).
		StyleFunc(func(row, col int) lipgloss.Style {
			style := lipgloss.NewStyle().
				Width(columnWidth(widths, col)).
				MaxWidth(columnWidth(widths, col)).
				Padding(0, 1)

			if row == lgtable.HeaderRow {
				return style.Bold(true)
			}

			absoluteRow := start + row
			if absoluteRow == m.cursor {
				style = style.Background(lipgloss.Color("#5533cc")).Foreground(colorWhite)
			}

			if row >= 0 && row < len(visibleRows) && col == 0 {
				style = style.Inherit(statusTextStyle(visibleRows[row][0]))
			}

			return style
		})

	table := t.String()
	frame := lipgloss.NewStyle().Height(tableHeight)
	if m.width > 0 {
		frame = frame.Width(m.width)
	}
	return frame.Render(table)
}

func columnWidth(widths listColumnWidths, col int) int {
	switch col {
	case 0:
		return widths.status
	case 1:
		return widths.user
	case 2:
		return widths.command
	case 3:
		return widths.pattern
	case 4:
		return widths.branch
	case 5:
		return widths.duration
	case 6:
		return widths.when
	default:
		return 0
	}
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

	return title + filterStr + status + "\n" + m.renderTable() + "\n" + help
}
