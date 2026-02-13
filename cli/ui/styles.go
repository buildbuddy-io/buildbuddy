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

	// Status styles
	statusPassedStyle       = lipgloss.NewStyle().Foreground(colorGreen).Bold(true)
	statusFailedStyle       = lipgloss.NewStyle().Foreground(colorRed).Bold(true)
	statusInProgressStyle   = lipgloss.NewStyle().Foreground(colorYellow).Bold(true)
	statusDisconnectedStyle = lipgloss.NewStyle().Foreground(colorGray).Bold(true)
	statusUnknownStyle      = lipgloss.NewStyle().Foreground(colorGray)

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
