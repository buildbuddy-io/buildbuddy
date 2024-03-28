package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/prom"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	outputPath = flag.String("output_path", "", "Path to generated markdown file")
	labelFile  = flag.String("label_file", "", "Path to the file defining labels")
)

const docTemplateContents = "\n" +
	"### **`{{.Name}}`** ({{.Type}})\n" +
	`{{.Help}}
{{with .LabelNames}}
#### Labels
{{range .}}
- **{{.}}**
{{end}}
{{end}}
{{with .Examples}}
#### Examples

` + "```promql\n{{.}}\n```\n{{end}}"

var docTemplate = template.Must(template.New("docs").Parse(docTemplateContents))

func generateDoc(w io.Writer, m *prom.MetricConfig) error {
	caser := cases.Title(language.English)

	data := struct {
		Name       string
		Type       string
		Help       string
		LabelNames []string
		Examples   string
	}{
		Name:       m.ExportedFamily.GetName(),
		Type:       caser.String(strings.ToLower(m.ExportedFamily.GetType().String())),
		Help:       m.ExportedFamily.GetHelp(),
		LabelNames: m.LabelNames,
		Examples:   m.Examples,
	}

	return docTemplate.Execute(w, data)
}

func main() {
	flag.Parse()

	f, err := os.Create(*outputPath)
	if err != nil {
		fmt.Printf("failed to create file %q %s\n", *outputPath, err)
		return
	}
	defer f.Close()
	for _, m := range prom.MetricConfigs {
		generateDoc(f, m)
	}
}
