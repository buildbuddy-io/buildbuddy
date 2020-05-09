package terminal

import (
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"strings"
)

const (
	ELEMENT_ITERM_IMAGE = iota
	ELEMENT_IMAGE       = iota
	ELEMENT_LINK        = iota
	ELEMENT_BK          = iota
)

type element struct {
	url         string
	alt         string
	contentType string
	content     string
	height      string
	width       string
	elementType int
	bk          map[string]string
}

var errUnsupportedElementSequence = errors.New("Unsupported element sequence")

func (i *element) asHTML() string {
	if i.elementType == ELEMENT_LINK {
		content := i.content
		if content == "" {
			content = i.url
		}
		return fmt.Sprintf(`<a href="%s">%s</a>`, i.url, content)
	}

	if i.elementType == ELEMENT_BK {
		output := `<?bk`
		for key, value := range i.bk {
			output = output + ` ` + key + `="` + strings.Replace(value, `"`, "&quot;", -1) + `"`
		}
		return output + `?>`
	}

	alt := i.alt
	if alt == "" {
		alt = i.url
	}

	parts := []string{fmt.Sprintf(`alt="%s"`, alt)}

	if i.elementType == ELEMENT_ITERM_IMAGE {
		parts = append(parts, fmt.Sprintf(`src="data:%s;base64,%s"`, i.contentType, i.content))
	} else {
		parts = append(parts, fmt.Sprintf(`src="%s"`, i.url))
	}

	if i.width != "" {
		parts = append(parts, fmt.Sprintf(`width="%s"`, i.width))
	}
	if i.height != "" {
		parts = append(parts, fmt.Sprintf(`height="%s"`, i.height))
	}
	return fmt.Sprintf(`<img %s>`, strings.Join(parts, " "))
}

func parseElementSequence(sequence string) (*element, error) {
	// Expect 1337;File=name=1.gif;inline=1:BASE64

	args, elementType, content, err := splitAndVerifyElementSequence(sequence)
	if err != nil {
		if err == errUnsupportedElementSequence {
			err = nil
		}
		return nil, err
	}

	args = strings.Map(htmlStripper, args)

	tokens, err := tokenizeString(args, ';', '\\')
	if err != nil {
		return nil, err
	}

	imageInline := false

	elem := &element{content: content, elementType: elementType}

	for _, token := range tokens {
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		val := parts[1]
		switch strings.ToLower(key) {
		case "name":
			nameBytes, err := base64.StdEncoding.DecodeString(val)
			if err != nil {
				return nil, fmt.Errorf("name= value of %q is not valid base64", val)
			}
			elem.url = strings.Map(htmlStripper, string(nameBytes))
			elem.contentType = contentTypeForFile(elem.url)
		case "url":
			elem.url = val
		case "content":
			elem.content = val
		case "inline":
			imageInline = val == "1"
		case "width":
			elem.width = parseImageDimension(val)
		case "height":
			elem.height = parseImageDimension(val)
		case "alt":
			elem.alt = val
		}
	}

	if elem.elementType == ELEMENT_ITERM_IMAGE {
		if elem.url == "" {
			return nil, fmt.Errorf("name= argument not supplied, required to determine content type")
		}
		if elem.contentType == "" {
			return nil, fmt.Errorf("can't determine content type for %q", elem.url)
		}
	} else {
		if elem.url == "" {
			return nil, fmt.Errorf("url= argument not supplied")
		}
	}

	if elem.elementType == ELEMENT_ITERM_IMAGE && !imageInline {
		// in iTerm2, if you don't specify inline=1, the image is merely downloaded
		// and not displayed.
		elem = nil
	}
	return elem, nil
}

func contentTypeForFile(filename string) string {
	dot := strings.LastIndex(filename, ".")
	if dot == -1 {
		return ""
	}
	return mime.TypeByExtension(filename[dot:])
}

func parseImageDimension(s string) string {
	s = strings.ToLower(s)
	if !strings.HasSuffix(s, "px") && !strings.HasSuffix(s, "%") {
		return s + "em"
	} else {
		return s
	}
}

func htmlStripper(r rune) rune {
	switch r {
	case '<', '>', '\'', '"':
		return -1
	default:
		return r
	}
}

func splitAndVerifyElementSequence(s string) (arguments string, elementType int, content string, err error) {
	if strings.HasPrefix(s, "1338;") {
		return s[len("1338;"):], ELEMENT_IMAGE, "", nil
	}
	if strings.HasPrefix(s, "1339;") {
		return s[len("1339;"):], ELEMENT_LINK, "", nil
	}

	prefixLen := len("1337;File=")
	if !strings.HasPrefix(s, "1337;File=") {
		return "", 0, "", errUnsupportedElementSequence
	}
	s = s[prefixLen:]

	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return "", 0, "", fmt.Errorf("expected sequence to have one arguments part and one content part, got %d part(s)", len(parts))
	}

	elementType = ELEMENT_ITERM_IMAGE
	arguments = parts[0]
	content = parts[1]
	if len(content) == 0 {
		return "", 0, "", fmt.Errorf("image content missing")
	}

	_, err = base64.StdEncoding.DecodeString(content)
	if err != nil {
		return "", 0, "", fmt.Errorf("expected content part to be valid Base64")
	}

	return
}

func parseBuildkiteElementSequence(sequence string) (*element, error) {
	// Expect bk;t=123123234234234;llamas=blah

	if !strings.HasPrefix(sequence, "bk;") {
		return nil, nil
	}

	tokens, err := tokenizeString(sequence[3:], ';', '\\')
	if err != nil {
		return nil, err
	}

	params := map[string]string{}

	for _, token := range tokens {
		tokenParts := strings.SplitN(token, "=", 2)
		if len(tokenParts) != 2 {
			return nil, fmt.Errorf("Failed to read key=value from token %q", token)
		}
		params[tokenParts[0]] = tokenParts[1]
	}

	return &element{elementType: ELEMENT_BK, bk: params}, nil
}

func tokenizeString(s string, sep, escape rune) (tokens []string, err error) {
	var runes []rune
	inEscape := false
	for _, r := range s {
		switch {
		case inEscape:
			inEscape = false
			fallthrough
		default:
			runes = append(runes, r)
		case r == escape:
			inEscape = true
		case r == sep:
			tokens = append(tokens, string(runes))
			runes = runes[:0]
		}
	}
	tokens = append(tokens, string(runes))
	if inEscape {
		err = errors.New("invalid terminal escape")
	}
	return tokens, err
}
