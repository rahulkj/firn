// Package adminui serves a minimal read-only admin page at /.
// It intentionally depends on stdlib templates only.
package adminui

import (
	"html/template"
	"net/http"

	"github.com/rahulkj/eventing/internal/broker"
)

// Handler wires the admin dashboard to the broker.
func Handler(b *broker.Broker) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		topics := []topicRow{}
		for _, t := range b.ListTopics() {
			topics = append(topics, topicRow{Name: t.Name, Partitions: t.Partitions, Mode: t.Mode})
		}
		groupSummaries := []groupSummary{}
		for _, g := range b.Groups().Groups() {
			groupSummaries = append(groupSummaries, groupSummary{
				Name:       g.Name,
				Generation: g.Generation,
				Leader:     g.Leader,
				Members:    len(g.Members),
				Protocol:   g.Protocol,
			})
		}
		render(w, pageTemplate, pageData{Topics: topics, Groups: groupSummaries})
	})
	return mux
}

type groupSummary struct {
	Name       string
	Generation int64
	Leader     string
	Members    int
	Protocol   string
}

type topicRow struct {
	Name       string
	Partitions int
	Mode       string
}

type pageData struct {
	Topics []topicRow
	Groups []groupSummary
}

var pageTemplate = template.Must(template.New("page").Funcs(template.FuncMap{
	"short": func(s string) string {
		const max = 48
		if len(s) > max {
			return s[:max] + "…"
		}
		return s
	},
}).Parse(`<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<title>eventing admin</title>
	<style>
		body{font-family:system-ui,sans-serif;margin:0;padding:2rem;background:#0f172a;color:#e2e8f0}
		.card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:1rem 1.5rem;margin-bottom:1rem}
		h1{margin:0 0 1rem;font-size:1.5rem}
		.sub{color:#94a3b8;font-size:.9rem}
		table{width:100%;border-collapse:collapse;margin-top:.5rem}
		th,td{border-bottom:1px solid #334155;padding:.4rem .6rem;text-align:left;font-size:.9rem}
		th{color:#94a3b8;text-transform:uppercase;letter-spacing:.05em}
	</style>
</head>
<body>
	<h1>eventing admin <span class="sub">/ ui</span></h1>
	<div class="card"><h2>Topics ({{len .Topics}})</h2>
		<table>
			<thead><tr><th>Name</th><th>Partitions</th><th>Mode</th></tr></thead>
			<tbody>
			{{range .Topics}}<tr><td>{{.Name}}</td><td>{{.Partitions}}</td><td>{{.Mode}}</td></tr>{{end}}
			{{if not .Topics}}<tr><td colspan="3" class="sub">no topics yet</td></tr>{{end}}
			</tbody>
		</table>
	</div>
	<div class="card"><h2>Consumer Groups ({{len .Groups}})</h2>
		<table>
			<thead><tr><th>Group</th><th>Generation</th><th>Protocol</th><th>Members</th><th>Leader</th></tr></thead>
			<tbody>
			{{range .Groups}}<tr><td>{{.Name}}</td><td>{{.Generation}}</td><td>{{.Protocol}}</td><td>{{.Members}}</td><td>{{.Leader}}</td></tr>{{end}}
			{{if not .Groups}}<tr><td colspan="5" class="sub">no groups yet</td></tr>{{end}}
			</tbody>
		</table>
	</div>
</body>
</html>
`))

func render(w http.ResponseWriter, t *template.Template, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = t.Execute(w, data)
}
