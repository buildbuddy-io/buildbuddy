package main

import (
	"bufio"
	"cmp"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/term"
)

var (
	stringPattern = regexp.MustCompile(`".*?"`)
	listPattern   = regexp.MustCompile(`\[.*?\]`)
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	s := bufio.NewScanner(os.Stdin)
	demand := map[string]int{}
	n := 0
	hits := 0
	hitsByKey := map[string]int{}
	termWidth, _, _ := term.GetSize(int(os.Stdout.Fd()))
	for s.Scan() {
		line := s.Text()
		m := listPattern.FindString(line)
		if m == "" {
			continue
		}
		m = m[1 : len(m)-1]
		m = strings.ReplaceAll(m, ",", " ")

		lookKey := stringPattern.FindString(line)
		lookKey = lookKey[1 : len(lookKey)-1]
		demand[lookKey]++
		activeCounts := map[string]int{}
		pausedCounts := map[string]int{}
		activeCounts[lookKey] = 0
		pausedCounts[lookKey] = 0

		runnerDescriptions := strings.Fields(m)
		for _, d := range runnerDescriptions {
			fields := strings.Split(d, ":")
			state := fields[1]
			runnerKey := strings.Join(fields[3:], ":")
			if state == "P" {
				pausedCounts[runnerKey]++
				activeCounts[runnerKey] = max(0, activeCounts[runnerKey])
			} else {
				activeCounts[runnerKey]++
				pausedCounts[runnerKey] = max(0, pausedCounts[runnerKey])
			}
		}
		for k := range demand {
			pausedCounts[k] = max(0, pausedCounts[k])
			activeCounts[k] = max(0, activeCounts[k])
		}
		keys := slices.Collect(maps.Keys(pausedCounts))
		slices.SortFunc(keys, func(a, b string) int {
			c := -cmp.Compare(demand[a], demand[b])
			if c != 0 {
				return c
			}
			return cmp.Compare(a, b)
		})
		screen := "\033[2J\033[H"
		// screen += fmt.Sprintf("%s\n", time.Now())
		lbl := "MISS"
		if pausedCounts[lookKey] > 0 {
			lbl = "HIT"
			hits++
			hitsByKey[lookKey]++
		}
		n++
		if termWidth != 0 {
			w := int(float64(hits) / float64(n) * float64(termWidth))
			hitBar := fmt.Sprintf("\x1b[42m%s\x1b[m", strings.Repeat(" ", w))
			missBar := fmt.Sprintf("\x1b[41m%s\x1b[m", strings.Repeat(" ", termWidth-w))
			screen += fmt.Sprintf("%.3f\n", float64(hits)/float64(n))
			screen += hitBar + missBar + "\n"
		}
		// screen += fmt.Sprintf("? %-40s [%s]\n", lookKey, lbl)
		for i, k := range keys {
			if i >= 60 {
				screen += "...\n"
				break
			}
			if demand[k] == 0 {
				continue
			}
			color := ""
			if k == lookKey && lbl == "MISS" {
				color = "41"
			}
			if k == lookKey && lbl == "HIT" {
				color = "42"
			}
			activeBar := fmt.Sprintf("\x1b[104m%s\x1b[m", strings.Repeat("-", activeCounts[k]))
			pausedBar := fmt.Sprintf("\x1b[107m%s\x1b[m", strings.Repeat("-", pausedCounts[k]))
			screen += fmt.Sprintf("- \x1b[%sm%-40s\x1b[m [%4d \x1b[31m-%4d\x1b[m] %s%s %d\n", color, k, demand[k], demand[k]-hitsByKey[k], activeBar, pausedBar, activeCounts[k]+pausedCounts[k])
		}
		fmt.Print(screen)
		time.Sleep(50 * time.Millisecond)
	}
	if err := s.Err(); err != nil {
		return err
	}

	return nil
}
