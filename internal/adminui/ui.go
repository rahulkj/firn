// Package adminui serves a minimal read-only admin page at / and the
// federation page at /federation (ADR-010, slice 3). It intentionally
// depends on stdlib templates only.
package adminui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/rahulkj/firn/internal/broker"
	"github.com/rahulkj/firn/internal/federation"
)

// Handler wires the admin dashboard to the broker. fs may be nil (federation
// unavailable); the /federation page then renders an empty state.
func Handler(b *broker.Broker, fs *federation.Store) http.Handler {
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
		render(w, pageTemplate, pageData{Topics: topics, Groups: groupSummaries, Federation: federationView(fs)})
	})

	// Read-only federation view (ADR-010): peers and cursors, nothing
	// mutable. Registration happens via the admin API / firn-cli only.
	mux.HandleFunc("/federation", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/federation" {
			http.NotFound(w, r)
			return
		}
		render(w, federationTemplate, federationView(fs))
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

type peerRow struct {
	Name      string
	Addr      string
	Direction string
	Mode      string
	Topics    string
	Queues    string
	KeySet    bool
	Updated   int64
}

type cursorRow struct {
	Peer             string
	Topic            string
	Partition        int32
	LastSourceOffset int64
	LastLocalOffset  int64
	Mode             string
	Updated          int64
}

type federationData struct {
	Peers   []peerRow
	Cursors []cursorRow
	Has     bool
}

type pageData struct {
	Topics     []topicRow
	Groups     []groupSummary
	Federation federationData
}

func federationView(fs *federation.Store) federationData {
	out := federationData{}
	if fs == nil {
		return out
	}
	out.Has = true
	for _, p := range fs.ListPeers() {
		topics, queues := []string{}, []string{}
		for _, s := range p.Selectors {
			topics = append(topics, s.Topics...)
			queues = append(queues, s.Queues...)
		}
		if len(topics) == 0 && len(queues) == 0 {
			topics = []string{"(all topics)"}
			queues = []string{"(all queues)"}
		}
		out.Peers = append(out.Peers, peerRow{
			Name:      p.Name,
			Addr:      p.Addr,
			Direction: p.Direction.String(),
			Mode:      p.Mode.String(),
			Topics:    joinOrDash(topics),
			Queues:    joinOrDash(queues),
			KeySet:    p.Key != "",
			Updated:   p.UpdatedUnix,
		})
	}
	for _, c := range fs.ListCursors() {
		out.Cursors = append(out.Cursors, cursorRow{
			Peer:             c.Peer,
			Topic:            c.Topic,
			Partition:        c.Partition,
			LastSourceOffset: c.LastSourceOffset,
			LastLocalOffset:  c.LastLocalOffset,
			Mode:             c.Mode.String(),
			Updated:          c.UpdatedUnix,
		})
	}
	return out
}

func joinOrDash(list []string) string {
	if len(list) == 0 {
		return "—"
	}
	out := ""
	for i, s := range list {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	return out
}

var federationTemplate = template.Must(template.New("federation").Parse(`<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<title>firn admin · federation</title>
	<style>
		body{font-family:system-ui,sans-serif;margin:0;padding:2rem;background:#0f172a;color:#e2e8f0}
		.card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:1rem 1.5rem;margin-bottom:1rem}
		h1{margin:0 0 1rem;font-size:1.5rem}
		h2{font-size:1.1rem}
		.sub{color:#94a3b8;font-size:.9rem}
		table{width:100%;border-collapse:collapse;margin-top:.5rem}
		th,td{border-bottom:1px solid #334155;padding:.4rem .6rem;text-align:left;font-size:.9rem}
		th{color:#94a3b8;text-transform:uppercase;letter-spacing:.05em}
		a{color:#7dd3fc}
	</style>
</head>
<body>
	<h1>firn admin <span class="sub">/ ui / federation</span></h1>
	<p class="sub"><a href="/ui/">← dashboard</a> · read-only</p>
	<div class="card"><h2>Peers ({{len .Peers}})</h2>
		<table>
			<thead><tr><th>Name</th><th>Addr</th><th>Direction</th><th>Mode</th><th>Topics</th><th>Queues</th><th>Key</th><th>Updated</th></tr></thead>
			<tbody>
			{{range .Peers}}<tr><td>{{.Name}}</td><td>{{.Addr}}</td><td>{{.Direction}}</td><td>{{.Mode}}</td><td>{{.Topics}}</td><td>{{.Queues}}</td><td>{{if .KeySet}}set{{else}}—{{end}}</td><td>{{.Updated}}</td></tr>{{end}}
			{{if not .Peers}}<tr><td colspan="8" class="sub">no peers registered</td></tr>{{end}}
			</tbody>
		</table>
	</div>
	<div class="card"><h2>Import cursors ({{len .Cursors}})</h2>
		<table>
			<thead><tr><th>Peer</th><th>Topic</th><th>Partition</th><th>Last source offset</th><th>Last local offset</th><th>Mode</th><th>Updated</th></tr></thead>
			<tbody>
			{{range .Cursors}}<tr><td>{{.Peer}}</td><td>{{.Topic}}</td><td>{{.Partition}}</td><td>{{.LastSourceOffset}}</td><td>{{.LastLocalOffset}}</td><td>{{.Mode}}</td><td>{{.Updated}}</td></tr>{{end}}
			{{if not .Cursors}}<tr><td colspan="7" class="sub">no imports yet</td></tr>{{end}}
			</tbody>
		</table>
		<p class="sub">Updated is unix milli. lag is exported as firn_federation_cursor_lag_seconds.</p>
	</div>
</body>
</html>
`))

var pageTemplate = template.Must(template.New("page").Funcs(template.FuncMap{
	"short": func(s string) string {
		const max = 48
		if len(s) > max {
			return s[:max] + "…"
		}
		return s
	},
	"now": func() int64 { return time.Now().UnixMilli() },
}).Parse(`<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<title>firn admin</title>
	<style>
		body{font-family:system-ui,sans-serif;margin:0;padding:2rem;background:#0f172a;color:#e2e8f0}
		.card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:1rem 1.5rem;margin-bottom:1rem}
		h1{margin:0 0 1rem;font-size:1.5rem}
		.sub{color:#94a3b8;font-size:.9rem}
		table{width:100%;border-collapse:collapse;margin-top:.5rem}
		th,td{border-bottom:1px solid #334155;padding:.4rem .6rem;text-align:left;font-size:.9rem}
		th{color:#94a3b8;text-transform:uppercase;letter-spacing:.05em}
		a{color:#7dd3fc}
	</style>
</head>
<body>
	<h1>firn admin <span class="sub">/ ui</span></h1>
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
	<div class="card"><h2>Federation</h2>
		<p class="sub"><a href="/ui/federation">{{len .Federation.Peers}} peer(s) · {{len .Federation.Cursors}} cursor(s) →</a></p>
	</div>
</body>
</html>
`))

func render(w http.ResponseWriter, t *template.Template, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = t.Execute(w, data)
}
