package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type logEntry struct {
	Timestamp time.Time
	RawLine   string
}

type fileLogs struct {
	Path    string
	Entries []logEntry
}

var (
	timeLayouts = []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	ansiColors = []string{
		"\x1b[31m",
		"\x1b[32m",
		"\x1b[33m",
		"\x1b[34m",
		"\x1b[35m",
		"\x1b[36m",
		"\x1b[91m",
		"\x1b[92m",
	}
	ansiReset = "\x1b[0m"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("log_viewer", flag.ContinueOnError)
	width := fs.Int("width", 0, "total output width in chars (defaults to terminal width or 180)")
	showPath := fs.Bool("show-path", false, "show full path in headers")

	if err := fs.Parse(args); err != nil {
		return err
	}

	paths := fs.Args()
	if len(paths) < 2 {
		return errors.New("please provide at least 2 log files")
	}

	logs := make([]fileLogs, len(paths))
	for i, path := range paths {
		entries, err := readStructuredLogFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}

		if len(entries) == 0 {
			return fmt.Errorf("file %s has no parseable timestamped logs", path)
		}

		sort.Slice(entries, func(a, b int) bool {
			return entries[a].Timestamp.Before(entries[b].Timestamp)
		})

		logs[i] = fileLogs{Path: path, Entries: entries}
	}

	if os.Getenv("NO_COLOR") != "" {
		ansiColors = []string{""}
		ansiReset = ""
	}

	return renderAligned(logs, *width, *showPath)
}

func readStructuredLogFile(path string) ([]logEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	entries := make([]logEntry, 0, 1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		ts, ok := extractTimestamp(line)
		if !ok {
			continue
		}

		entries = append(entries, logEntry{Timestamp: ts, RawLine: line})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func extractTimestamp(line string) (time.Time, bool) {
	fields := parseLogFmt(line)
	if v, ok := fields["time"]; ok {
		if ts, parsed := parseTimeString(v); parsed {
			return ts, true
		}
	}

	return time.Time{}, false
}

func parseTimeString(value string) (time.Time, bool) {
	v := strings.TrimSpace(value)
	v = strings.Trim(v, `"`)
	if v == "" {
		return time.Time{}, false
	}

	for _, layout := range timeLayouts {
		if ts, err := time.Parse(layout, v); err == nil {
			return ts, true
		}
	}

	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return parseUnixInt(i)
	}

	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return parseUnixNumber(f)
	}

	return time.Time{}, false
}

func parseUnixNumber(v float64) (time.Time, bool) {
	if v <= 0 {
		return time.Time{}, false
	}

	// Heuristics by magnitude to support seconds, milliseconds, microseconds and nanoseconds.
	switch {
	case v > 1e18:
		return time.Unix(0, int64(v)), true
	case v > 1e15:
		return time.Unix(0, int64(v)*int64(time.Microsecond)), true
	case v > 1e12:
		return time.Unix(0, int64(v)*int64(time.Millisecond)), true
	default:
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)
		return time.Unix(sec, nsec), true
	}
}

func parseUnixInt(v int64) (time.Time, bool) {
	if v <= 0 {
		return time.Time{}, false
	}

	switch {
	case v > 1e18:
		return time.Unix(0, v), true
	case v > 1e15:
		return time.Unix(0, v*int64(time.Microsecond)), true
	case v > 1e12:
		return time.Unix(0, v*int64(time.Millisecond)), true
	default:
		return time.Unix(v, 0), true
	}
}

func parseLogFmt(line string) map[string]string {
	fields := make(map[string]string)
	i := 0

	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}

		keyStart := i
		for i < len(line) && line[i] != '=' && line[i] != ' ' {
			i++
		}
		if i >= len(line) || line[i] != '=' {
			for i < len(line) && line[i] != ' ' {
				i++
			}
			continue
		}

		key := line[keyStart:i]
		i++

		if i < len(line) && line[i] == '"' {
			i++
			start := i
			escaped := false
			var b strings.Builder
			for i < len(line) {
				c := line[i]
				if escaped {
					b.WriteByte(c)
					escaped = false
					i++
					continue
				}
				if c == '\\' {
					escaped = true
					i++
					continue
				}
				if c == '"' {
					break
				}
				b.WriteByte(c)
				i++
			}

			if i <= len(line) {
				fields[key] = b.String()
			}

			if i < len(line) && line[i] == '"' {
				i++
			} else {
				fields[key] = line[start:i]
			}
			continue
		}

		valStart := i
		for i < len(line) && line[i] != ' ' {
			i++
		}
		fields[key] = line[valStart:i]
	}

	return fields
}

func renderAligned(logs []fileLogs, width int, showPath bool) error {
	if len(logs) == 0 {
		return errors.New("no logs to render")
	}

	if width <= 0 {
		width = terminalWidth()
	}

	separator := " | "
	colWidth := (width - (len(logs)-1)*len(separator)) / len(logs)
	if colWidth < 24 {
		colWidth = 24
	}

	headers := make([]string, len(logs))
	for i, lf := range logs {
		header := lf.Path
		if !showPath {
			header = filepath.Base(lf.Path)
		}
		headers[i] = truncate(padRight(header, colWidth), colWidth)
	}
	printColoredRow(headers, colWidth, separator)
	printSeparator(colWidth, len(logs), separator)

	indices := make([]int, len(logs))
	for {
		nextTime, ok := earliestCurrentTimestamp(logs, indices)
		if !ok {
			break
		}

		cells := make([]string, len(logs))
		for i := range logs {
			if indices[i] >= len(logs[i].Entries) {
				cells[i] = padRight("", colWidth)
				continue
			}

			entry := logs[i].Entries[indices[i]]
			if entry.Timestamp.Equal(nextTime) {
				cells[i] = formatCell(entry, colWidth)
				indices[i]++
				continue
			}

			cells[i] = padRight("", colWidth)
		}

		printColoredRow(cells, colWidth, separator)
	}

	return nil
}

func earliestCurrentTimestamp(logs []fileLogs, indices []int) (time.Time, bool) {
	var earliest time.Time
	found := false

	for i := range logs {
		if indices[i] >= len(logs[i].Entries) {
			continue
		}

		curr := logs[i].Entries[indices[i]].Timestamp
		if !found || curr.Before(earliest) {
			earliest = curr
			found = true
		}
	}

	return earliest, found
}

func formatCell(entry logEntry, width int) string {
	content := extractDisplayMessage(entry.RawLine)
	return truncate(padRight(content, width), width)
}

func extractDisplayMessage(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}

	fields := parseLogFmt(line)
	if v, ok := fields["msg"]; ok {
		return strings.TrimSpace(v)
	}

	return line
}

func printSeparator(colWidth, cols int, separator string) {
	line := strings.Repeat("-", colWidth)
	parts := make([]string, cols)
	for i := range parts {
		parts[i] = line
	}
	fmt.Println(strings.Join(parts, separator))
}

func printColoredRow(parts []string, colWidth int, separator string) {
	row := make([]string, len(parts))
	for i := range parts {
		cell := truncate(padRight(parts[i], colWidth), colWidth)
		row[i] = colorize(cell, i)
	}
	fmt.Println(strings.Join(row, separator))
}

func colorize(s string, idx int) string {
	if len(ansiColors) == 0 || ansiColors[0] == "" {
		return s
	}
	return ansiColors[idx%len(ansiColors)] + s + ansiReset
}

func padRight(s string, width int) string {
	r := []rune(s)
	if len(r) >= width {
		return string(r[:width])
	}
	return s + strings.Repeat(" ", width-len(r))
}

func truncate(s string, width int) string {
	r := []rune(s)
	if len(r) <= width {
		return s
	}
	if width < 2 {
		return string(r[:width])
	}
	return string(r[:width-1]) + "~"
}

func terminalWidth() int {
	if c := os.Getenv("COLUMNS"); c != "" {
		if n, err := strconv.Atoi(c); err == nil && n > 0 {
			return n
		}
	}
	return 180
}
