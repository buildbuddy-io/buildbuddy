// generate_docs parses metrics.go and generates markdown documentation.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	inputPath    = flag.String("input_path", "", "Path to metrics.go")
	outputPath   = flag.String("output_path", "", "Path to generated markdown file")
	prettierPath = flag.String("prettier_path", "", "Path to prettier")
)

type labelConstant struct {
	value    string
	comments []string
}

type metricLabel struct {
	name     string
	comments []string
}

type metric struct {
	namespace  string
	subsystem  string
	name       string
	help       string
	metricType string
	labels     []metricLabel
}

type docsGenerator struct {
	metricsGoPath string

	// Current parser state (string).
	state string
	// Cumulative output to be printed at the end.
	outputLines []string

	// Parsed LABEL_CONSTANTS section.
	// Maps label constant token name to {comments: [...], value: ...}.
	labelConstants map[string]labelConstant

	// Fields for METRIC and METRIC.LABELS state.
	metric *metric

	// Fields for LABEL_CONSTANTS state.
	labelConstantComments []string

	// Fields for METRIC.LABELS state.
	labelComments []string
}

func newDocsGenerator(metricsGoPath string) *docsGenerator {
	return &docsGenerator{
		metricsGoPath:  metricsGoPath,
		labelConstants: make(map[string]labelConstant),
	}
}

var (
	labelConstantCommentRE = regexp.MustCompile(`^//\s*(.*)`)
	labelConstantValueRE   = regexp.MustCompile(`^(\w+)\s*=\s*"(.*)"`)
	metricAttrRE           = regexp.MustCompile(`^\s*(Namespace|Subsystem|Name|Help):\s+"(.*)"`)
	newMetricRE            = regexp.MustCompile(`^\w+ = promauto\.New(\w+)`)
	labelTokenRE           = regexp.MustCompile(`^\w+`)
	markdownCommentRE      = regexp.MustCompile(`^//\s*(.*)`)
)

func (g *docsGenerator) parse() ([]string, error) {
	f, err := os.Open(g.metricsGoPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineIndex := 0
	for scanner.Scan() {
		line := scanner.Text()
		if err := g.processLine(lineIndex, line); err != nil {
			return nil, err
		}
		lineIndex++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	g.outputLines = append(g.outputLines, "")
	return g.outputLines, nil
}

func (g *docsGenerator) processLine(lineIndex int, line string) error {
	line = strings.TrimSpace(line)

	if line == "const (" {
		g.state = "LABEL_CONSTANTS"
		return nil
	}
	if g.state == "LABEL_CONSTANTS" {
		if line == ")" {
			g.state = ""
			return nil
		}
		if strings.HasPrefix(line, "//") {
			if m := labelConstantCommentRE.FindStringSubmatch(line); m != nil {
				g.labelConstantComments = append(g.labelConstantComments, m[1])
			} else {
				g.labelConstantComments = nil
			}
			return nil
		}
		if line == "" {
			g.labelConstantComments = nil
			return nil
		}
		if m := labelConstantValueRE.FindStringSubmatch(line); m != nil {
			g.labelConstants[m[1]] = labelConstant{
				value:    m[2],
				comments: g.labelConstantComments,
			}
			g.labelConstantComments = nil
		}
		return nil
	}

	if g.state == "METRIC" {
		// We're inside a metric definition.
		if line == "})" {
			// End metric declaration.
			g.state = ""
			g.flushMetric()
			return nil
		}

		if strings.Contains(line, "[]string{") {
			// Begin declaration of metric labels.
			g.state = "METRIC.LABELS"
			return nil
		}

		// Parse string attributes (Name:, Help:, etc.)
		if m := metricAttrRE.FindStringSubmatch(line); m != nil {
			g.setMetricAttr(strings.ToLower(m[1]), m[2])
			return nil
		}
	}

	// Inside a metric label list ([]string{ ... })
	if g.state == "METRIC.LABELS" {
		// We're inside the labels list of a metric vector definition.
		if line == "})" {
			// End metric and metric label declaration.
			g.state = ""
			g.flushMetric()
			return nil
		}

		if m := labelTokenRE.FindString(line); m != "" {
			constInfo, ok := g.labelConstants[m]
			if ok {
				g.labelComments = constInfo.comments
				g.addMetricLabel(lineIndex, constInfo.value)
			}
		}
	}

	// Check for a new metric definition.
	if m := newMetricRE.FindStringSubmatch(line); m != nil {
		metricType := strings.TrimSuffix(m[1], "Vec")
		g.setMetricAttr("type", metricType)
		g.state = "METRIC"
		return nil
	}

	// Process markdown blocks.
	if strings.HasPrefix(line, "// #") {
		g.state = "MARKDOWN_BLOCK"
	}

	if g.state == "MARKDOWN_BLOCK" {
		if m := markdownCommentRE.FindStringSubmatch(line); m != nil {
			g.outputLines = append(g.outputLines, m[1])
		} else {
			g.state = ""
		}
		return nil
	}

	return nil
}

func (g *docsGenerator) flushMetric() {
	m := g.metric
	if m == nil {
		return
	}
	if strings.HasSuffix(m.name, "_exported") {
		// Skip exported metrics.
		g.metric = nil
		return
	}

	helpParts := strings.Split(m.help, ". ")
	helpMain := helpParts[0]
	helpDetails := helpParts[1:]

	var nameParts []string
	ns := m.namespace
	if ns == "" {
		ns = "buildbuddy"
	}
	nameParts = append(nameParts, ns)
	if m.subsystem != "" {
		nameParts = append(nameParts, m.subsystem)
	}
	if m.name != "" {
		nameParts = append(nameParts, m.name)
	}
	metricName := strings.Join(nameParts, "_")

	helpMainSuffix := ""
	if len(helpDetails) > 0 {
		helpMainSuffix = "."
	}

	g.outputLines = append(g.outputLines,
		"",
		fmt.Sprintf("### **`%s`** (%s)", metricName, m.metricType),
		"",
		fmt.Sprintf("%s%s", helpMain, helpMainSuffix),
	)

	if len(helpDetails) > 0 {
		g.outputLines = append(g.outputLines, "", strings.Join(helpDetails, ". "))
	}

	if len(m.labels) > 0 {
		g.outputLines = append(g.outputLines, "", "#### Labels", "")
		for _, label := range m.labels {
			g.outputLines = append(g.outputLines,
				fmt.Sprintf("- **%s**: %s", label.name, strings.Join(label.comments, " ")),
			)
		}
		g.outputLines = append(g.outputLines, "")
	}

	g.metric = nil
}

func (g *docsGenerator) addMetricLabel(lineIndex int, labelName string) {
	if g.metric == nil {
		log.Fatalf("ERROR (Line %d): Bad state: found metric label documentation outside a metric label.", lineIndex+1)
	}
	g.metric.labels = append(g.metric.labels, metricLabel{
		name:     labelName,
		comments: g.labelComments,
	})
	g.labelComments = nil
}

func (g *docsGenerator) setMetricAttr(name, value string) {
	if g.metric == nil {
		g.metric = &metric{}
	}
	switch name {
	case "namespace":
		g.metric.namespace = value
	case "subsystem":
		g.metric.subsystem = value
	case "name":
		g.metric.name = value
	case "help":
		g.metric.help = value
	case "type":
		g.metric.metricType = value
	}
}

func main() {
	flag.Parse()

	if *inputPath == "" || *outputPath == "" || *prettierPath == "" {
		log.Fatal("--input_path, --output_path, and --prettier_path are required")
	}

	gen := newDocsGenerator(*inputPath)
	lines, err := gen.parse()
	if err != nil {
		log.Fatalf("Error parsing: %v", err)
	}

	// Get absolute path for the output file to ensure prettier can find it.
	outputAbs, err := filepath.Abs(*outputPath)
	if err != nil {
		log.Fatalf("Error getting absolute path: %v", err)
	}

	if err := os.WriteFile(outputAbs, []byte(strings.Join(lines, "\n")), 0644); err != nil {
		log.Fatalf("Error writing output: %v", err)
	}

	cmd := exec.Command(*prettierPath, "--parser=markdown", "--write", outputAbs)
	cmd.Env = os.Environ()
	if output, err := cmd.CombinedOutput(); err != nil {
		log.Fatalf("Error running prettier: %v\nOutput: %s", err, string(output))
	}
}
