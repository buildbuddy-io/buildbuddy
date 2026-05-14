package ui

import (
	tea "github.com/charmbracelet/bubbletea"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

type viewState int

const (
	viewOrgPicker viewState = iota
	viewList
	viewDetail
)

type rootModel struct {
	api          *apiClient
	f            filters
	state        viewState
	orgPicker    orgPickerModel
	list         listModel
	detail       detailModel
	hasOrgPicker bool
	width        int
	height       int
}

func newRootModelWithOrgPicker(api *apiClient, f filters, groups []*grpb.Group, selectedGroupID string) rootModel {
	return rootModel{
		api:          api,
		f:            f,
		state:        viewOrgPicker,
		orgPicker:    newOrgPickerModel(groups, selectedGroupID),
		hasOrgPicker: true,
	}
}

func newRootModelWithList(api *apiClient, f filters) rootModel {
	return rootModel{
		api:   api,
		f:     f,
		state: viewList,
		list:  newListModel(api, f, false),
	}
}

func (m rootModel) Init() tea.Cmd {
	switch m.state {
	case viewOrgPicker:
		return m.orgPicker.Init()
	case viewList:
		return m.list.Init()
	case viewDetail:
		return m.detail.Init()
	}
	return nil
}

func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "q":
			if m.state == viewList || m.state == viewOrgPicker {
				return m, tea.Quit
			}
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	}

	switch m.state {
	case viewOrgPicker:
		var cmd tea.Cmd
		m.orgPicker, cmd = m.orgPicker.Update(msg)
		if m.orgPicker.selected != "" {
			m.api.groupID = m.orgPicker.selected
			m.orgPicker.selected = ""
			m.state = viewList
			m.list = newListModel(m.api, m.f, m.hasOrgPicker)
			initCmd := m.list.Init()
			sizeCmd := func() tea.Msg {
				return tea.WindowSizeMsg{Width: m.width, Height: m.height}
			}
			return m, tea.Batch(initCmd, sizeCmd)
		}
		return m, cmd

	case viewList:
		var cmd tea.Cmd
		m.list, cmd = m.list.Update(msg)
		if m.list.switchOrg {
			m.list.switchOrg = false
			m.state = viewOrgPicker
			return m, nil
		}
		if m.list.selectedID != "" {
			id := m.list.selectedID
			m.list.selectedID = ""
			m.state = viewDetail
			m.detail = newDetailModel(m.api, id)
			initCmd := m.detail.Init()
			sizeCmd := func() tea.Msg {
				return tea.WindowSizeMsg{Width: m.width, Height: m.height}
			}
			return m, tea.Batch(initCmd, sizeCmd)
		}
		return m, cmd

	case viewDetail:
		var cmd tea.Cmd
		m.detail, cmd = m.detail.Update(msg)
		if m.detail.goBack {
			m.state = viewList
			return m, tea.Batch(
				m.api.fetchInvocations(m.f),
				autoRefreshCmd(),
			)
		}
		return m, cmd
	}

	return m, nil
}

func (m rootModel) View() string {
	switch m.state {
	case viewOrgPicker:
		return m.orgPicker.View()
	case viewDetail:
		return m.detail.View()
	default:
		return m.list.View()
	}
}
