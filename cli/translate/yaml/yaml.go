package yaml

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/add"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/translate/builtins"
	"gopkg.in/yaml.v2"
)

type yamlTranslator struct {
	loadList []string
}

var (
	splitRegex       = regexp.MustCompile("\n---+")
	newLineSeparator = "\n\n"
	commaSeparator   = ", "
)

func New() *yamlTranslator {
	return &yamlTranslator{
		loadList: []string{},
	}
}

func (y *yamlTranslator) Translate(_, input string) (string, error) {
	rules := splitRegex.Split(string(input), -1)
	output := ""
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		m := yaml.MapSlice{}
		err := yaml.Unmarshal([]byte(rule), &m)
		if err != nil {
			return "", err
		}
		s := y.translateRule(m)
		if s == "" {
			continue
		}
		output += s
	}
	return output, nil
}

func (y *yamlTranslator) translateRule(m yaml.MapSlice) string {
	s := ""
	for _, i := range m {
		ruleName := i.Key.(string)
		switch ruleName {
		case "load":
			if load, ok := i.Value.(yaml.MapSlice); ok {
				s = s + y.translateLoad(load) + newLineSeparator
			} else {
				log.Warnf("load: must be a list")
			}
		case "rules":
			if rules, ok := i.Value.([]interface{}); ok {
				for _, rule := range rules {
					s = s + y.translateRule(rule.(yaml.MapSlice)) + newLineSeparator
				}
			} else {
				log.Warnf("rules: must be a list")
			}
		case "deps":
			if load, ok := i.Value.([]interface{}); ok {
				s = s + y.translateDeps(load) + newLineSeparator
			} else {
				log.Warnf("deps: must be a list, instead it was %T", i.Value)
			}
		case "bazel":
			if load, ok := i.Value.(yaml.MapSlice); ok {
				y.translateBazel(load)
			} else {
				log.Warnf("bazel: must be a map, instead it was %T", i.Value)
			}
		case "settings":
			if load, ok := i.Value.(yaml.MapSlice); ok {
				y.translateSettings(load)
			} else {
				log.Warnf("settings: must be a list, instead it was %T", i.Value)
			}
		case "templates":
			if load, ok := i.Value.(yaml.MapSlice); ok {
				y.translateTemplateMap(load)
			} else if load, ok := i.Value.([]interface{}); ok {
				y.translateTemplate(load)
			} else {
				log.Warnf("template: must be a list or a map, instead it was %T", i.Value)
			}
		case "raw":
			if raw, ok := i.Value.(string); ok {
				s = s + raw + newLineSeparator
			} else {
				log.Warnf("raw: value must be a string")
			}
		default:
			translatedValue := y.translateValue(i.Value, ruleName)
			s = s + translatedValue + newLineSeparator
		}
	}

	return s
}

func (y *yamlTranslator) translateMap(m yaml.MapSlice) string {
	values := []string{}
	for _, i := range m {
		values = append(values, i.Key.(string)+" = "+y.translateValue(i.Value, ""))
	}

	return strings.Join(values, commaSeparator)
}

func (y *yamlTranslator) translateList(l []interface{}) string {
	values := []string{}
	for _, i := range l {
		values = append(values, y.translateValue(i, ""))
	}
	return strings.Join(values, commaSeparator)
}

func (y *yamlTranslator) translateValue(v interface{}, ruleName string) string {
	switch i := v.(type) {
	case yaml.MapSlice:
		m := y.translateMap(i)
		if ruleName != "" {
			return fmt.Sprintf("%s(%s)", ruleName, m)
		}
		return fmt.Sprintf("{%s}", m)
	case []interface{}:
		if ruleName != "" {
			s := []string{}
			for _, i := range i {
				s = append(s, y.translateValue(i, ruleName))
			}
			return strings.Join(s, newLineSeparator)
		}
		return fmt.Sprintf("[%s]", y.translateList(i))
	case string:
		for _, g := range builtins.BuildGlobals {
			if strings.HasPrefix(i, fmt.Sprintf("%s(", g)) {
				return i
			}
		}
		for _, l := range y.loadList {
			if l == fmt.Sprintf(`"%s"`, i) {
				return i
			}
		}
		return fmt.Sprintf(`"%s"`, i)
	case bool:
		if i {
			return "True"
		}
		return "False"
	case nil:
		return ""
	default:
		log.Warnf("unknown translateValue type: %T", i)
		return fmt.Sprintf("unknown translateValue type %T", i)
	}
}

func (y *yamlTranslator) translateLoad(m yaml.MapSlice) string {
	values := []string{}
	for _, i := range m {
		value := ""
		switch i := i.Value.(type) {
		case []interface{}:
			value = y.translateList(i)
			y.loadList = append(y.loadList, strings.Split(value, commaSeparator)...)
		case string:
			value = fmt.Sprintf(`"%s"`, i)
			y.loadList = append(y.loadList, value)
		default:
			log.Warnf("unknown translateLoad type: %T", i)
			return fmt.Sprintf("unknown translateLoad type %T", i)
		}
		values = append(values, fmt.Sprintf(`load("%s", %s)`, i.Key.(string), value))
	}

	return strings.Join(values, newLineSeparator)
}

func (y *yamlTranslator) translateDeps(m []interface{}) string {
	output := ""
	for _, dep := range m {
		depString, ok := dep.(string)
		if !ok {
			log.Warnf("unknown dep type: %T", dep)
			continue
		}
		module, resp, err := add.FetchModuleOrDisambiguate(strings.Split(depString, "@")[0])
		if err != nil {
			log.Warnf("error fetching module: %s", err)
			continue
		}
		output += add.GenerateSnippet(module, resp)

	}
	return output
}

func (y *yamlTranslator) translateBazel(m yaml.MapSlice) {
	for _, field := range m {
		key, ok := field.Key.(string)
		if !ok {
			log.Warnf("unknown bazel field: %T", field.Key)
			continue
		}
		value, ok := field.Value.(string)
		if !ok {
			log.Warnf("unknown bazel field: %T", field.Value)
			continue
		}
		if key == "version" {
			err := os.WriteFile(".bazelversion", []byte(value), 0644)
			if err != nil {
				log.Warnf("error writing .bazelversion file: %s", err)
			}
		}
	}
}

func (y *yamlTranslator) translateSettings(m yaml.MapSlice) {
	bazelrc := ""
	for _, s := range m {
		bazelrc += "common --" + s.Key.(string) + "=" + s.Value.(string) + "\n"
	}

	err := os.WriteFile(".bazelrc", []byte(bazelrc), 0644)
	if err != nil {
		log.Warnf("error writing .bazelrc file: %s", err)
	}
}

func (y *yamlTranslator) translateTemplateMap(m yaml.MapSlice) {
	for _, t := range m {
		renderTemplate(t.Value.(string), t.Key.(string))
	}
}

func (y *yamlTranslator) translateTemplate(m []interface{}) {
	for _, s := range m {
		from := ""
		into := ""

		if slice, ok := s.(string); ok {
			from = slice
		}

		if slice, ok := s.(yaml.MapSlice); ok {
			for _, x := range slice {
				key, ok := x.Key.(string)
				if !ok {
					log.Warnf("unknown key type: %T", x.Value)
					continue
				}
				value, ok := x.Value.(string)
				if !ok {
					log.Warnf("unknown value type: %T", x.Value)
					continue
				}
				if key == "from" {
					from = value
				}
				if key == "into" {
					into = value
				}
			}
		}

		if into == "" {
			parts := strings.Split(from, "/")
			into = parts[len(parts)-1]
		}
		renderTemplate(from, into)
	}
}

func renderTemplate(from, into string) {
	if _, err := os.Stat(into); !os.IsNotExist(err) {
		log.Debugf("skipping template %s, directory %q already exists", from, into)
		return
	}
	log.Debugf("grabbing template from %s and putting it into %q", from, into)

	if !strings.HasPrefix(from, "github/") {
		log.Warnf("unknown template %s", from)
	}

	repo := strings.Replace(from, "github/", "https://github.com/", 1)

	cmd := exec.Command("git", "clone", "--depth=1", repo+".git", into)
	err := cmd.Run()
	if err != nil {
		log.Warnf("error cloning repo %s: %s", repo, err)
	}
	err = os.RemoveAll(filepath.Join(into, ".git"))
	if err != nil {
		log.Warnf("error cleaning up git directory for %s: %s", repo, err)
	}
}
