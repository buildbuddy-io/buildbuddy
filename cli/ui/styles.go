package ui

import "github.com/charmbracelet/lipgloss"

var (
	// Colors
	colorGreen  = lipgloss.Color("#00cc66")
	colorRed    = lipgloss.Color("#ff3333")
	colorYellow = lipgloss.Color("#ffcc00")
	colorGray   = lipgloss.Color("#888888")
	colorCyan   = lipgloss.Color("#00cccc")
	colorWhite  = lipgloss.Color("#ffffff")
	colorDim    = lipgloss.Color("#666666")

	// Muted status colors for list rendering
	colorStatusPassed  = lipgloss.Color("#7fb995")
	colorStatusFailed  = lipgloss.Color("#d58686")
	colorStatusRunning = lipgloss.Color("#c0a96f")
	colorStatusNeutral = lipgloss.Color("#a0a7b0")

	// Title bar
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorWhite).
			Background(lipgloss.Color("#5533cc")).
			Padding(0, 1)

	// Section headers in detail view
	sectionStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorCyan).
			MarginTop(1)

	// Help text at the bottom
	helpStyle = lipgloss.NewStyle().
			Foreground(colorDim)

	// Error messages
	errorStyle = lipgloss.NewStyle().
			Foreground(colorRed).
			Bold(true)

	// Detail view labels
	labelStyle = lipgloss.NewStyle().
			Foreground(colorDim).
			Width(16)

	valueStyle = lipgloss.NewStyle().
			Foreground(colorWhite)

	// Filter chips
	filterStyle = lipgloss.NewStyle().
			Foreground(colorCyan).
			Background(lipgloss.Color("#1a1a2e")).
			Padding(0, 1)

	// List picker styles
	lipglossHighlight = lipgloss.NewStyle().
				Foreground(colorWhite).
				Bold(true)

	lipglossNormal = lipgloss.NewStyle().
			Foreground(colorGray)
)

func statusTextStyle(status string) lipgloss.Style {
	switch status {
	case "Passed":
		return lipgloss.NewStyle().Foreground(colorStatusPassed)
	case "Failed":
		return lipgloss.NewStyle().Foreground(colorStatusFailed)
	case "In progress":
		return lipgloss.NewStyle().Foreground(colorStatusRunning)
	case "Disconnected", "Unknown":
		return lipgloss.NewStyle().Foreground(colorStatusNeutral)
	default:
		return lipgloss.NewStyle()
	}
}
