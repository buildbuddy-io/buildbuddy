package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	_ "embed"
)

var (
	envProject     = os.Getenv("LOGVIEW_PROJECT")
	envProdCluster = os.Getenv("LOGVIEW_PROD_CLUSTER")
	envDevCluster  = os.Getenv("LOGVIEW_DEV_CLUSTER")
	envLocation    = os.Getenv("LOGVIEW_LOCATION")
	envPort        = os.Getenv("LOGVIEW_PORT")

	queryCache = &Cache{}
)

//go:embed index.html
var html string

//go:embed style.css
var css string

//go:embed main_bundle.js
var js string

//go:embed prompt.md
var prompt string

type TemplateData struct {
	CSS  template.CSS
	JS   template.JS
	Data *QueryResponse
}

type QueryResponse struct {
	Logs  any    `json:"logs"`
	Query string `json:"query"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	port := 0
	if envPort != "" {
		p, err := strconv.Atoi(envPort)
		if err != nil {
			log.Errorf("Invalid port; ignoring")
		}
		port = p
	}

	tmpl, err := template.New("index.html").Funcs(template.FuncMap{"toJSON": toJSON}).Parse(html)
	if err != nil {
		return err
	}

	http.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		nat := req.URL.Query().Get("q")
		q, err := parseNaturalQuery(nat)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}
		logs := []any{}
		if q != "" {
			log.Infof("Getting query result for:\n%s", q)
			v, err := runQuery(req.Context(), q)
			if err != nil {
				log.Errorf("Error: %s", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			logs = v
		}
		data := TemplateData{
			CSS: template.CSS(css),
			JS:  template.JS(js),
			Data: &QueryResponse{
				Logs:  logs,
				Query: q,
			},
		}
		tmpl.Execute(w, data)
	}))
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return err
	}
	port = lis.Addr().(*net.TCPAddr).Port
	log.Infof("Serving on %s", lis.Addr())
	q := strings.Join(os.Args[1:], " ")
	uiURL := fmt.Sprintf("http://localhost:%d/?q=%s", port, url.QueryEscape(q))
	log.Infof("Opening %s", uiURL)
	openBrowserWindow(uiURL)
	http.Serve(lis, nil)
	return nil
}

func openBrowserWindow(url string) {
	for _, open := range []string{"open", "xdg-open"} {
		if err := exec.Command(open, url).Run(); err == nil {
			break
		}
	}
}

type Cache struct{}

var NotFound = fmt.Errorf("key not found in cache")

func (c *Cache) dir() (string, error) {
	d, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	p := filepath.Join(d, "buildbuddy-logview")
	if err := os.MkdirAll(p, 0755); err != nil {
		return "", err
	}
	return p, nil
}

func (c *Cache) path(key string) (string, error) {
	d, err := c.dir()
	if err != nil {
		return "", err
	}
	sha := fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
	return filepath.Join(d, sha), nil
}

func (c *Cache) Get(key string) ([]byte, error) {
	p, err := c.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, NotFound
		}
		return nil, err
	}
	return b, nil
}

func (c *Cache) Set(key string, value []byte) error {
	p, err := c.path(key)
	if err != nil {
		return err
	}
	return os.WriteFile(p, value, 0644)
}

func runQuery(ctx context.Context, query string) ([]any, error) {
	b, err := queryCache.Get(query)
	if err != nil && err != NotFound {
		return nil, err
	}
	buf := &bytes.Buffer{}
	if err == NotFound {
		stderrBuf := &bytes.Buffer{}
		cmd := exec.CommandContext(ctx, "gcloud", "logging", "read", "--format=json", "--project="+envProject, "--limit=10000", query)
		cmd.Stderr = io.MultiWriter(os.Stderr, stderrBuf)
		cmd.Stdout = buf
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("gcloud failed: %w: %s", err, stderrBuf.String())
		}
		queryCache.Set(query, buf.Bytes())
	} else {
		buf = bytes.NewBuffer(b)
	}
	var out []any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func parseNaturalQuery(naturalQuery string) (gcpQuery string, err error) {
	if naturalQuery == "" {
		return "", nil
	}

	now := time.Now()
	// localTimestamp := toLocalTimestamp(t)

	start := now.Add(-3 * time.Hour)
	end := now

	pq := &ParsedQuery{
		Start: toLocalTimestamp(start),
		End:   toLocalTimestamp(end),
	}

	parts := strings.Split(naturalQuery, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)

		// Treat "dev" or "prod" as env
		if p == "dev" || p == "prod" {
			pq.Env = p
			continue
		}
		// Parse env, "env" prefix or suffix
		if strings.HasPrefix(p, "env ") {
			pq.Env = strings.TrimSpace(strings.TrimPrefix(p, "env"))
			continue
		} else if strings.HasSuffix(p, " env") {
			pq.Env = strings.TrimSpace(strings.TrimSuffix(p, "env"))
			continue
		}
		// Parse execution ID
		if strings.Contains(p, "/uploads/") && strings.Contains(p, "blobs/") && !strings.Contains(p, " ") {
			pq.Execution = p
			continue
		}
		// Parse timestamps
		if looksLikeTimespec(p) {
			log.Infof("Parsing %q as timespec", p)
			start, end, err := parseTimespec(p)
			if err == nil {
				pq.Start = toLocalTimestamp(start)
				pq.End = toLocalTimestamp(end)
				continue
			} else {
				log.Warningf("Failed to parse timespec: %s", err)
			}
		}

		// Everything else is treated as a raw match
		pq.Match = p
	}

	return generateQuery(pq)
}

var lookbackRE = regexp.MustCompile(`^last\s+(?P<quantity>\d+(\.\d+)?)\s*(?P<unit>d|h|m|s)$`)

func unitToDuration(unit string) time.Duration {
	switch unit {
	case "d":
		return 24 * time.Hour
	case "h":
		return time.Hour
	case "m":
		return time.Minute
	case "s":
		return time.Second
	default:
		panic("unsupported unit: " + unit)
	}
}

func looksLikeTimespec(s string) bool {
	s = strings.ToLower(s)
	amPmRE := regexp.MustCompile(`(\d|\s)(am|pm)\b`)
	if amPmRE.MatchString(s) {
		return true
	}
	if lookbackRE.MatchString(s) {
		return true
	}
	return false
}

func parseTimespec(s string) (start, end time.Time, err error) {
	var z time.Time

	s = strings.ToLower(s)

	// Parse 'last 3h'
	if m := lookbackRE.FindStringSubmatch(s); m != nil {
		qty, _ := strconv.ParseFloat(m[lookbackRE.SubexpIndex("quantity")], 64)
		unit := m[lookbackRE.SubexpIndex("unit")]
		duration := time.Duration(qty) * unitToDuration(unit)
		now := time.Now()
		return now.Add(-duration), now, nil
	}

	// Parse '30m around Jan 26 8:11PM'
	aroundRE := regexp.MustCompile(`^(?P<window>[^\s]+)\s+around\s+(?P<timestamp>.+)$`)
	if m := aroundRE.FindStringSubmatch(s); m != nil {
		ws := m[aroundRE.SubexpIndex("window")]
		ts := m[aroundRE.SubexpIndex("timestamp")]
		w, err := parseDuration(ws)
		if err != nil {
			return z, z, fmt.Errorf("parse duration in 'around' expr: %w", err)
		}
		t, err := parseTime(ts)
		if err != nil {
			return z, z, fmt.Errorf("parse timestamp %q in 'around' expr: %w", ts, err)
		}
		return t.Add(-w), t.Add(w), nil
	}

	// rangeSeparatorRE := regexp.MustCompile(`\b(to)\b`)
	return z, z, fmt.Errorf("no timespec found")
}

func parseTime(s string) (time.Time, error) {
	s = strings.ToLower(s)
	dateRaw := `(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sept?|oct|nov|dec)\s*(?P<day>\d+)`
	timeRaw := `(?P<hour>\d+)(:(?P<minute>\d+))?\s*(?P<meridiem>am|pm)`
	// dateRE := regexp.MustCompile(dateRaw)
	// timeRE := regexp.MustCompile(timeRaw)
	// optionalDateRaw :=
	// TODO: make date optional
	// TODO: make time optional
	dateTimeRE := regexp.MustCompile(dateRaw + `\s+` + timeRaw)
	if m := dateTimeRE.FindStringSubmatch(s); m != nil {
		year := time.Now().Year() // TODO: allow parsing year
		monthName := m[dateTimeRE.SubexpIndex("month")]
		month := monthNumber(monthName)
		dayRaw := m[dateTimeRE.SubexpIndex("day")]
		day, _ := strconv.Atoi(dayRaw)
		hourRaw := m[dateTimeRE.SubexpIndex("hour")]
		if hourRaw == "" {
			hourRaw = "0"
		}
		hour, _ := strconv.Atoi(hourRaw)
		minuteRaw := m[dateTimeRE.SubexpIndex("minute")]
		minute, _ := strconv.Atoi(minuteRaw)
		meridiem := m[dateTimeRE.SubexpIndex("meridiem")]
		return makeTime(year, month, day, hour, minute, meridiem)
	}
	return time.Time{}, fmt.Errorf("no datetime found")
}

func makeTime(year, month, day, hour, minute int, meridiem string) (time.Time, error) {
	_, offset := time.Now().Zone()
	location := time.FixedZone("", offset)
	return time.ParseInLocation(
		"2006-01-02 3:04PM",
		fmt.Sprintf("%d-%s-%s %d:%s%s", year, doubleDigit(month), doubleDigit(day), hour, doubleDigit(minute), strings.ToUpper(meridiem)),
		location,
	)
}

func doubleDigit(v int) string {
	s := strconv.Itoa(v)
	if len(s) == 1 {
		return "0" + s
	}
	return s
}

func monthNumber(m string) int {
	m = strings.ToLower(m)
	values := map[string]int{
		"jan": 1,
		"feb": 2,
		"mar": 3,
		"apr": 4,
		"may": 5,
		"jun": 6,
		"jul": 7,
		"aug": 8,
		"sep": 9,
		"oct": 10,
		"nov": 11,
		"dec": 12,
	}
	for k, v := range values {
		if strings.HasPrefix(m, k) {
			return v
		}
	}
	return 0
}

func parseDuration(s string) (time.Duration, error) {
	// TODO: more flexible parsing
	return time.ParseDuration(s)
}

func generateQuery(q *ParsedQuery) (string, error) {
	var out []string
	if q.Start != "" {
		out = append(out, fmt.Sprintf("timestamp>=%q", q.Start))
	}
	if q.End != "" {
		out = append(out, fmt.Sprintf("timestamp<=%q", q.End))
	}
	out = append(out, `resource.type="k8s_container"`)
	out = append(out, fmt.Sprintf("resource.labels.location=%q", envLocation))
	out = append(out, fmt.Sprintf("resource.labels.project_id=%q", envProject))
	if q.Env == "dev" {
		out = append(out, fmt.Sprintf("resource.labels.cluster_name=%q", envDevCluster))
	} else if q.Env == "prod" {
		out = append(out, fmt.Sprintf("resource.labels.cluster_name=%q", envProdCluster))
	}
	if q.Execution != "" {
		out = append(out, fmt.Sprintf(`(%q OR "interrupt signal" OR "Hard-stopping")`, q.Execution))
	}
	if q.Match != "" {
		out = append(out, fmt.Sprintf("%q", q.Match))
	}
	return strings.Join(out, "\n"), nil
}

type ParsedQuery struct {
	Start     string `yaml:"start"`
	End       string `yaml:"end"`
	Env       string `yaml:"env"`
	Execution string `yaml:"execution"`
	Match     string `yaml:"match"`
}

// Everything below was generated by GPT -[o.o]-

func toLocalTimestamp(t time.Time) string {
	// Format the time in ISO8601, but without the timezone info.
	isoTime := t.Format("2006-01-02T15:04:05")

	// Get the local timezone offset in seconds east of UTC.
	_, offset := t.Zone()

	// Convert offset seconds to hours and minutes.
	offsetHours := offset / 3600
	offsetMinutes := (offset % 3600) / 60

	// Format the offset as +HHMM or -HHMM.
	offsetStr := fmt.Sprintf("%+.2d%.2d", offsetHours, offsetMinutes)

	// Combine the ISO time with the formatted timezone offset.
	isoTimeWithOffset := isoTime + offsetStr

	return isoTimeWithOffset
}

// toJSON is a custom template function that marshals data to JSON
func toJSON(v any) (template.JS, error) {
	a, err := json.Marshal(v)
	return template.JS(a), err // template.JS is safe to use with <script> tags
}
