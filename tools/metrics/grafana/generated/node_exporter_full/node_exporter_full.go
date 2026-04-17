// Generates an alternative  version of the Node Exporter dashboard.
//
// The generated dashboard injects additional BB-specific filters on top
// of the upstream dashboard.
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

//go:embed dashboard.json
var rawJSON []byte

const (
	altUID      = "node-exporter-full-bb"
	altTitle    = "Node Exporter Full (BuildBuddy)"
	altFileName = "node-exporter-full-bb.json"
)

func main() {
	var d map[string]any
	if err := json.Unmarshal(rawJSON, &d); err != nil {
		fmt.Fprintf(os.Stderr, "parse: %v\n", err)
		os.Exit(1)
	}
	applyTransforms(d)
	out, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}

// applyTransforms is the main customization seam. Each helper below is a
// pure function over the dashboard map; add or remove calls here to shape
// the alternative dashboard.
func applyTransforms(d map[string]any) {
	rebrand(d)
	const bbLabels = `region=~"$region",rack=~"$rack",virt=~"$virt",pool=~"$pool"`
	injectLabelFilters(d, bbLabels)
	addBBVariables(d)
	// Make the nodename/instance dropdowns respect the bb filters too —
	// otherwise they'd list every machine across all regions/racks/pools.
	filterVariableQueries(d, []string{"nodename", "node"}, bbLabels)
}

// rebrand renames the dashboard so it can coexist with the upstream copy.
func rebrand(d map[string]any) {
	d["title"] = altTitle
	d["uid"] = altUID
	if tags, ok := d["tags"].([]any); ok {
		for i, t := range tags {
			if s, ok := t.(string); ok && strings.HasPrefix(s, "file:") {
				tags[i] = "file:" + altFileName
			}
		}
	}
}

// labelSelectorRe matches a PromQL label-selector block `{...}`. Nested
// braces are not supported by PromQL itself, so a flat regex is fine.
var labelSelectorRe = regexp.MustCompile(`\{([^{}]*)\}`)

// appendToSelectors appends `extra` to every PromQL label selector
// `{...}` in expr. Empty selectors `{}` become `{extra}`.
func appendToSelectors(expr, extra string) string {
	return labelSelectorRe.ReplaceAllStringFunc(expr, func(sel string) string {
		inner := strings.TrimSpace(sel[1 : len(sel)-1])
		if inner == "" {
			return "{" + extra + "}"
		}
		return "{" + inner + "," + extra + "}"
	})
}

// injectLabelFilters appends `extra` to every PromQL label selector in
// every panel target's `expr`. `extra` is a comma-separated list of
// matchers, e.g. `region=~"$region",rack=~"$rack"`.
//
// Note: this is a textual transform — it does not understand PromQL. If a
// query uses computed label sets via `label_replace`, this will still
// inject into any literal `{...}` selector but won't reach into the
// computed labels. For our purposes (filtering by infrastructure labels
// that exist on the source metrics) the textual approach is sufficient.
func injectLabelFilters(d map[string]any, extra string) {
	walkTargets(d, func(t map[string]any) {
		expr, _ := t["expr"].(string)
		if expr == "" {
			return
		}
		t["expr"] = appendToSelectors(expr, extra)
	})
}

// filterVariableQueries appends `extra` to the label selectors in the
// queries of the named template variables. Used to make the nodename /
// instance dropdowns respect upstream filter variables.
func filterVariableQueries(d map[string]any, names []string, extra string) {
	tpl, _ := d["templating"].(map[string]any)
	if tpl == nil {
		return
	}
	list, _ := tpl["list"].([]any)
	want := make(map[string]bool, len(names))
	for _, n := range names {
		want[n] = true
	}
	for _, v := range list {
		vm, _ := v.(map[string]any)
		if vm == nil {
			continue
		}
		name, _ := vm["name"].(string)
		if !want[name] {
			continue
		}
		if q, ok := vm["query"].(map[string]any); ok {
			if expr, ok := q["query"].(string); ok {
				q["query"] = appendToSelectors(expr, extra)
			}
		}
		if def, _ := vm["definition"].(string); def != "" {
			vm["definition"] = appendToSelectors(def, extra)
		}
	}
}

// addBBVariables inserts bb-specific label-filter variables (region,
// rack, pool, virt) into the dashboard's templating list, immediately after the
// existing `job` variable. Each variable's query filters itself against
// the variables defined above it in the chain — selecting a region
// narrows the rack list, etc., and `job` (above) further constrains all
// three.
func addBBVariables(d map[string]any) {
	tpl, _ := d["templating"].(map[string]any)
	if tpl == nil {
		return
	}
	list, _ := tpl["list"].([]any)

	// Default insertion point and datasource ref; refined by the loop below.
	insertAfter := -1
	var dsRef any = map[string]any{"type": "prometheus", "uid": "vm"}
	for i, v := range list {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		switch vm["type"] {
		case "datasource":
			if insertAfter < i {
				insertAfter = i
			}
		case "query":
			if ds, ok := vm["datasource"]; ok {
				dsRef = ds
			}
			if vm["name"] == "job" {
				insertAfter = i
			}
		}
	}

	// Constrain each new variable by the variables above it: rack filters by
	// region, pool by region+rack. The bb dashboards typically also scope
	// these by `job` to avoid mixing prom + node-exporter label spaces.
	bbVars := []any{
		newQueryVar("region", "Region",
			`label_values(node_uname_info{job="$job"}, region)`, dsRef),
		newQueryVar("rack", "Rack",
			`label_values(node_uname_info{job="$job",region=~"$region"}, rack)`, dsRef),
		newQueryVar("virt", "Virt",
			`label_values(node_uname_info{job="$job",region=~"$region",rack=~"$rack"}, virt)`, dsRef),
		newQueryVar("pool", "Pool",
			`label_values(node_uname_info{job="$job",region=~"$region",rack=~"$rack",virt=~"$virt"}, pool)`, dsRef),
	}

	pos := insertAfter + 1
	if pos < 0 || pos > len(list) {
		pos = len(list)
	}
	out := make([]any, 0, len(list)+len(bbVars))
	out = append(out, list[:pos]...)
	out = append(out, bbVars...)
	out = append(out, list[pos:]...)
	tpl["list"] = out
}

func newQueryVar(name, label, query string, dsRef any) map[string]any {
	return map[string]any{
		"name":       name,
		"label":      label,
		"type":       "query",
		"datasource": dsRef,
		// Grafana expects `query` as an object with `query` (the actual
		// PromQL expression) and `refId` (a stable identifier). Passing a
		// bare string parses as legacy schema and the variable definition
		// shows up empty in the UI.
		"query": map[string]any{
			"query": query,
			"refId": "Prometheus-" + name + "-Variable-Query",
		},
		"definition": query,
		"regex":      "",
		"current":    map[string]any{},
		"options":    []any{},
		"refresh":    1,
		"sort":       1,
		"multi":      true,
		"includeAll": true,
		"allValue":   ".*",
	}
}

// walkTargets calls fn for every Prometheus query target in the
// dashboard, recursing into rows.
func walkTargets(d map[string]any, fn func(map[string]any)) {
	panels, _ := d["panels"].([]any)
	walkPanelsTargets(panels, fn)
}

func walkPanelsTargets(panels []any, fn func(map[string]any)) {
	for _, p := range panels {
		panel, _ := p.(map[string]any)
		if panel == nil {
			continue
		}
		if sub, ok := panel["panels"].([]any); ok {
			walkPanelsTargets(sub, fn)
		}
		targets, _ := panel["targets"].([]any)
		for _, t := range targets {
			if target, ok := t.(map[string]any); ok {
				fn(target)
			}
		}
	}
}
