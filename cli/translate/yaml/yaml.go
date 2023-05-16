package yaml

import (
	"fmt"
	"regexp"
	"strings"

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
				log.Printf("load: must be a list")
			}
		case "rules":
			if rules, ok := i.Value.([]interface{}); ok {
				for _, rule := range rules {
					s = s + y.translateRule(rule.(yaml.MapSlice)) + newLineSeparator
				}
			} else {
				log.Printf("rules: must be a list")
			}
		case "raw":
			if raw, ok := i.Value.(string); ok {
				s = s + raw + newLineSeparator
			} else {
				log.Printf("raw: value must be a string")
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
		log.Printf("unknown translateValue type: %T", i)
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
			log.Printf("unknown translateLoad type: %T", i)
			return fmt.Sprintf("unknown translateLoad type %T", i)
		}
		values = append(values, fmt.Sprintf(`load("%s", %s)`, i.Key.(string), value))
	}

	return strings.Join(values, newLineSeparator)
}
