package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
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

type viewerServer struct {
	paths    []string
	showPath bool
	tmpl     *template.Template
}

type pageData struct {
	ShowSelector bool
	GeneratedAt  string
	Available    []fileOption
	Selected     []string
	Sources      []string
	Rows         []alignedRow
	Page         int
	TotalPages   int
	TotalRows    int
	HasPrev      bool
	HasNext      bool
	PrevPage     int
	NextPage     int
}

type alignedRow struct {
	Time  string
	Cells []displayCell
}

type displayCell struct {
	Message string
	Class   string
}

type fileOption struct {
	Label    string
	Value    string
	Selected bool
}

const rowsPerPage = 500

var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
}

const pageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Raft Log Viewer</title>
<style>
body {
	margin: 0;
	font-family: Menlo, Consolas, Monaco, monospace;
	background: #faf8f3;
	color: #222;
}
header {
	padding: 12px 16px;
	background: #1f2937;
	color: #f9fafb;
	position: sticky;
	top: 0;
	z-index: 2;
}
header h1 {
	margin: 0;
	font-size: 16px;
}
header p {
	margin: 4px 0 0;
	font-size: 12px;
	color: #cbd5e1;
}
header a {
	color: white;
}
.container {
	padding: 10px;
}
.table-wrap {
	overflow-x: auto;
	border: 1px solid #ddd;
	background: #fff;
}
table {
	border-collapse: collapse;
	width: 100%;
	min-width: 1000px;
}
th, td {
	border-bottom: 1px solid #ececec;
	padding: 8px;
	text-align: left;
	vertical-align: top;
	font-size: 12px;
	line-height: 1.4;
}
thead th {
	position: sticky;
	top: 0;
	background: #f3efe4;
	z-index: 1;
}
th.time-col, td.time-col {
	width: 240px;
	white-space: nowrap;
	color: #6b7280;
	background: #fcfaf4;
}
td.message {
	white-space: pre-wrap;
	word-break: break-word;
}
td.message.kv {
	color: #1d4ed8;
	background: #eff6ff;
}
td.message.err {
	color: #b91c1c;
	background: #fee2e2;
}
tr:nth-child(even) td {
	background: #fffdf8;
}
.empty {
	color: #c3c3c3;
}
</style>
</head>
<body>
<header>
	<h1>Raft Log Viewer</h1>
	{{if .ShowSelector}}
	<p>Choose one or more log files from the provided paths.</p>
	{{else}}
	<p>Generated at {{.GeneratedAt}} | Sources: {{len .Sources}} | Showing rows: {{len .Rows}} / {{.TotalRows}} | Page {{.Page}}/{{.TotalPages}}</p>
	<div style="margin: 0 0 10px; font-size: 12px;">
		{{if .HasPrev}}<a href="{{pageLink .Selected .PrevPage}}">Previous</a>{{end}}
		{{if and .HasPrev .HasNext}} | {{end}}
		{{if .HasNext}}<a href="{{pageLink .Selected .NextPage}}">Next</a>{{end}}
		{{if .Selected}} | <a href="/">Choose files</a>{{end}}
	</div>
	{{end}}
</header>
<div class="container">
	{{if .ShowSelector}}
	<form method="get" action="/" style="background: #fff; border: 1px solid #ddd; padding: 16px;">
		<p style="margin-top: 0; font-size: 13px; color: #4b5563;">Multi-select is enabled. Pick the files you want to align and compare.</p>
		<div style="display: grid; gap: 8px; max-height: 70vh; overflow-y: auto;">
			{{range .Available}}
			<label style="display: flex; align-items: flex-start; gap: 8px; font-size: 13px;">
				<input type="checkbox" name="file" value="{{.Value}}" {{if .Selected}}checked{{end}}>
				<span>{{.Label}}</span>
			</label>
			{{end}}
		</div>
		<div style="margin-top: 16px; display: flex; gap: 12px; align-items: center;">
			<button type="submit">Open selected logs</button>
			<span style="font-size: 12px; color: #6b7280;">{{len .Available}} file(s) found</span>
		</div>
	</form>
	{{else}}
	<div class="table-wrap">
		<table>
			<thead>
				<tr>
					<th class="time-col">time</th>
					{{range .Sources}}<th>{{.}}</th>{{end}}
				</tr>
			</thead>
			<tbody>
				{{range .Rows}}
				<tr>
					<td class="time-col">{{.Time}}</td>
					{{range .Cells}}
						{{if .Message}}
						<td class="message {{.Class}}">{{.Message}}</td>
						{{else}}
						<td class="message empty">&nbsp;</td>
						{{end}}
					{{end}}
				</tr>
				{{end}}
			</tbody>
		</table>
	</div>
	{{end}}
</div>
</body>
</html>`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("log_viewer", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "http listen address")
	showPath := fs.Bool("show-path", false, "show full file path in source headers")

	if err := fs.Parse(args); err != nil {
		return err
	}

	paths := fs.Args()
	if len(paths) < 1 {
		return errors.New("please provide at least 1 log file or directory")
	}

	tmpl, err := template.New("page").Funcs(template.FuncMap{
		"pageLink": buildPageLink,
	}).Parse(pageHTML)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	s := &viewerServer{paths: paths, showPath: *showPath, tmpl: tmpl}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)

	fmt.Printf("log viewer available at http://localhost%s\n", *addr)
	return http.ListenAndServe(*addr, mux)
}

func (s *viewerServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	availableFiles, err := discoverLogFiles(s.paths)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	selected := selectedFiles(r, availableFiles)
	if len(selected) == 0 {
		data := buildSelectorData(availableFiles, nil)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := s.tmpl.Execute(w, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	logs, err := loadLogs(selected)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	page := 1
	if raw := strings.TrimSpace(r.URL.Query().Get("page")); raw != "" {
		p, convErr := strconv.Atoi(raw)
		if convErr == nil && p > 0 {
			page = p
		}
	}

	data := buildPageData(logs, s.showPath, page)
	data.Selected = append([]string(nil), selected...)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func discoverLogFiles(paths []string) ([]string, error) {
	seen := make(map[string]struct{})
	files := make([]string, 0, len(paths))

	for _, root := range paths {
		info, err := os.Stat(root)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", root, err)
		}

		if !info.IsDir() {
			absPath, err := filepath.Abs(root)
			if err != nil {
				return nil, fmt.Errorf("abs %s: %w", root, err)
			}
			if _, found := seen[absPath]; !found {
				seen[absPath] = struct{}{}
				files = append(files, absPath)
			}
			continue
		}

		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			absPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			if _, found := seen[absPath]; found {
				return nil
			}
			seen[absPath] = struct{}{}
			files = append(files, absPath)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk %s: %w", root, err)
		}
	}

	if len(files) == 0 {
		return nil, errors.New("no files found in provided paths")
	}

	sort.Strings(files)
	return files, nil
}

func selectedFiles(r *http.Request, available []string) []string {
	allowed := make(map[string]struct{}, len(available))
	for _, path := range available {
		allowed[path] = struct{}{}
	}

	selected := make([]string, 0, len(available))
	seen := make(map[string]struct{})
	for _, raw := range r.URL.Query()["file"] {
		path := strings.TrimSpace(raw)
		if path == "" {
			continue
		}
		if _, ok := allowed[path]; !ok {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		selected = append(selected, path)
	}

	sort.Strings(selected)
	return selected
}

func buildSelectorData(availableFiles, selected []string) pageData {
	selectedSet := make(map[string]struct{}, len(selected))
	for _, path := range selected {
		selectedSet[path] = struct{}{}
	}

	options := make([]fileOption, 0, len(availableFiles))
	for _, path := range availableFiles {
		_, isSelected := selectedSet[path]
		options = append(options, fileOption{
			Label:    path,
			Value:    path,
			Selected: isSelected,
		})
	}

	return pageData{
		ShowSelector: true,
		Available:    options,
		Selected:     append([]string(nil), selected...),
	}
}

func buildPageLink(selected []string, page int) string {
	if page < 1 {
		page = 1
	}

	parts := make([]string, 0, len(selected)+1)
	for _, path := range selected {
		parts = append(parts, "file="+urlQueryEscape(path))
	}
	parts = append(parts, "page="+strconv.Itoa(page))
	return "/?" + strings.Join(parts, "&")
}

func urlQueryEscape(value string) string {
	replacer := strings.NewReplacer(
		"%", "%25",
		" ", "%20",
		"#", "%23",
		"&", "%26",
		"+", "%2B",
		"?", "%3F",
	)
	return replacer.Replace(value)
}

func loadLogs(paths []string) ([]fileLogs, error) {
	if len(paths) == 1 {
		return readSingleFileSplitByNode(paths[0])
	}

	logs := make([]fileLogs, len(paths))
	for i, path := range paths {
		entries, err := readStructuredLogFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("file %s has no parseable timestamped logs", path)
		}
		sort.Slice(entries, func(a, b int) bool {
			return entries[a].Timestamp.Before(entries[b].Timestamp)
		})
		logs[i] = fileLogs{Path: path, Entries: entries}
	}

	return logs, nil
}

func buildPageData(logs []fileLogs, showPath bool, page int) pageData {
	sources := make([]string, len(logs))
	for i, lf := range logs {
		header := lf.Path
		if !showPath {
			header = filepath.Base(lf.Path)
		}
		sources[i] = header
	}

	allRows := alignRows(logs)
	totalRows := len(allRows)
	totalPages := 1
	if totalRows > 0 {
		totalPages = (totalRows + rowsPerPage - 1) / rowsPerPage
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}

	start := (page - 1) * rowsPerPage
	if start > totalRows {
		start = totalRows
	}
	end := start + rowsPerPage
	if end > totalRows {
		end = totalRows
	}

	rows := allRows[start:end]
	hasPrev := page > 1
	hasNext := page < totalPages

	return pageData{
		GeneratedAt: time.Now().Format(time.RFC3339),
		Sources:     sources,
		Rows:        rows,
		Page:        page,
		TotalPages:  totalPages,
		TotalRows:   totalRows,
		HasPrev:     hasPrev,
		HasNext:     hasNext,
		PrevPage:    page - 1,
		NextPage:    page + 1,
	}
}

func alignRows(logs []fileLogs) []alignedRow {
	rows := make([]alignedRow, 0, 1024)
	indices := make([]int, len(logs))

	for {
		nextTime, ok := earliestCurrentTimestamp(logs, indices)
		if !ok {
			break
		}

		cells := make([]displayCell, len(logs))
		for i := range logs {
			if indices[i] >= len(logs[i].Entries) {
				cells[i] = displayCell{}
				continue
			}

			entry := logs[i].Entries[indices[i]]
			if entry.Timestamp.Equal(nextTime) {
				cells[i] = extractDisplayCell(entry.RawLine)
				indices[i]++
				continue
			}

			cells[i] = displayCell{}
		}

		rows = append(rows, alignedRow{
			Time:  nextTime.Format(time.RFC3339Nano),
			Cells: cells,
		})
	}

	return rows
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

func readSingleFileSplitByNode(path string) ([]fileLogs, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	entriesByNode := make(map[string][]logEntry)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		ts, ok := extractTimestamp(line)
		if !ok {
			continue
		}

		nodeID := extractNodeID(line)
		entriesByNode[nodeID] = append(entriesByNode[nodeID], logEntry{Timestamp: ts, RawLine: line})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(entriesByNode) == 0 {
		return nil, fmt.Errorf("file %s has no parseable timestamped logs", path)
	}

	nodeIDs := make([]string, 0, len(entriesByNode))
	for id := range entriesByNode {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool {
		li, errI := strconv.Atoi(nodeIDs[i])
		lj, errJ := strconv.Atoi(nodeIDs[j])
		if errI == nil && errJ == nil {
			return li < lj
		}
		return nodeIDs[i] < nodeIDs[j]
	})

	logs := make([]fileLogs, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		entries := entriesByNode[id]
		sort.Slice(entries, func(a, b int) bool {
			return entries[a].Timestamp.Before(entries[b].Timestamp)
		})
		logs = append(logs, fileLogs{
			Path:    "node_id=" + id,
			Entries: entries,
		})
	}

	return logs, nil
}

func extractNodeID(line string) string {
	fields := parseLogFmt(line)
	if v, ok := fields["node_id"]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return "unknown"
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

func extractDisplayCell(line string) displayCell {
	line = strings.TrimSpace(line)
	if line == "" {
		return displayCell{}
	}

	message := extractDisplayMessage(line)
	fields := parseLogFmt(line)
	classes := make([]string, 0, 2)

	if isTrueLogField(fields["kv"]) {
		classes = append(classes, "kv")
	}
	if isErrorLevel(fields["level"]) {
		classes = append(classes, "err")
	}

	return displayCell{Message: message, Class: strings.Join(classes, " ")}
}

func isTrueLogField(v string) bool {
	v = strings.TrimSpace(strings.Trim(v, "\""))
	return strings.EqualFold(v, "true")
}

func isErrorLevel(v string) bool {
	v = strings.TrimSpace(strings.Trim(v, "\""))
	return strings.EqualFold(v, "error")
}
