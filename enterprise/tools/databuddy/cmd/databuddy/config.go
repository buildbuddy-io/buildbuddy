package main

import (
	"fmt"
	"strings"

	dbpb "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/proto/databuddy"
	"gopkg.in/yaml.v2"
)

// QueryConfig represents the user-defined config in YAML comments at the top of
// a query SQL file.
type QueryConfig struct {
	Name string `json:"name" yaml:"name"`
	// Macro queries cannot be run, only imported.
	Macro   bool          `json:"macro" yaml:"macro"`
	Plugins []string      `json:"plugins" yaml:"plugins"`
	Charts  []ChartConfig `json:"charts" yaml:"charts"`
}

type ChartConfig struct {
	X      string `json:"x" yaml:"x"`
	Y      any    `json:"y" yaml:"y"`           // string | []string
	Series any    `json:"series" yaml:"series"` // string | []string
	Repeat any    `json:"repeat" yaml:"repeat"` // string | []string
}

func ParseYAMLComments(sql string) (*QueryConfig, error) {
	lines := strings.Split(strings.TrimSpace(sql), "\n")
	rawMetadata := ""
	for _, line := range lines {
		if !strings.HasPrefix(line, "-- ") && !strings.HasPrefix(line, "// ") {
			break
		}
		// TODO: return a ParseError if missing a space after // or --
		line = strings.TrimPrefix(line, "-- ")
		line = strings.TrimPrefix(line, "// ")
		rawMetadata += line + "\n"
	}
	config := &QueryConfig{}
	dec := yaml.NewDecoder(strings.NewReader(rawMetadata))
	if err := dec.Decode(config); err != nil {
		return nil, err
	}
	return config, nil
}

func ParseCharts(config *QueryConfig) ([]*dbpb.Chart, error) {
	var charts []*dbpb.Chart
	for i, c := range config.Charts {
		xs, _ := parseAxes(c.X)
		if len(xs) != 1 {
			return nil, fmt.Errorf("charts[%d]: missing or invalid 'x' axis", i)
		}
		ys, _ := parseAxes(c.Y)
		if len(ys) == 0 {
			return nil, fmt.Errorf("charts[%d]: missing or invalid 'y' axis", i)
		}
		repeat, _ := parseStringSlice(c.Repeat)
		series, _ := parseStringSlice(c.Series)
		charts = append(charts, &dbpb.Chart{
			X:      xs[0],
			Y:      ys,
			Series: series,
			Repeat: repeat,
		})
	}
	return charts, nil
}

func parseAxes(cols any) ([]*dbpb.Axis, bool) {
	if strs, ok := parseStringSlice(cols); ok {
		axes := make([]*dbpb.Axis, 0, len(strs))
		for _, s := range strs {
			axes = append(axes, &dbpb.Axis{Column: s})
		}
		return axes, true
	}
	return nil, false
}

func parseStringSlice(cols any) ([]string, bool) {
	if col, ok := cols.(string); ok {
		return []string{col}, true
	}
	if cols, ok := cols.([]string); ok {
		return cols, true
	}
	if cols, ok := cols.([]any); ok {
		out := make([]string, 0, len(cols))
		for _, v := range cols {
			if s, ok := v.(string); ok {
				out = append(out, s)
			} else {
				return nil, false
			}
		}
		return out, true
	}
	return nil, false
}
