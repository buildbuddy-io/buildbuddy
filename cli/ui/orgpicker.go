package ui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

type orgPickerModel struct {
	groups   []*grpb.Group
	cursor   int
	selected string
}

func newOrgPickerModel(groups []*grpb.Group, selectedGroupID string) orgPickerModel {
	cursor := 0
	for i, g := range groups {
		if g.GetId() == selectedGroupID {
			cursor = i
			break
		}
	}
	return orgPickerModel{
		groups: groups,
		cursor: cursor,
	}
}

func (m orgPickerModel) Init() tea.Cmd {
	return nil
}

func (m orgPickerModel) Update(msg tea.Msg) (orgPickerModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.groups)-1 {
				m.cursor++
			}
		case "enter":
			if m.cursor >= 0 && m.cursor < len(m.groups) {
				m.selected = m.groups[m.cursor].GetId()
			}
		}
	}

	return m, nil
}

func (m orgPickerModel) View() string {
	s := titleStyle.Render("Select Organization") + "\n\n"
	for i, g := range m.groups {
		cursor := "  "
		if i == m.cursor {
			cursor = "> "
		}
		name := g.GetName()
		if name == "" {
			name = g.GetId()
		}
		line := fmt.Sprintf("%s%s", cursor, name)
		if i == m.cursor {
			line = lipglossHighlight.Render(line)
		} else {
			line = lipglossNormal.Render(line)
		}
		s += line + "\n"
	}
	s += "\n" + helpStyle.Render("  ↑/↓/j/k: navigate • enter: select • ctrl+c: quit")
	return s
}
