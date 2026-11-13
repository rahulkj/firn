# Personas and what they use

Firn has exactly four official personas. Each has a documented path; anyone who doesn't fit them probably wants to talk to the platform operator about a key.

| Persona | Mission | UI | CLI | Docs |
|---|---|---|---|---|
| **Platform operator** | deploy, configure, upgrade, secure | `/ui/`, `/ui/federation` | full `firn-cli` | [`docs/operator.md`](operator.md) |
| **SRE / observability** | keep it healthy, page humans when it isn't | `/ui/`, `/metrics` | `firn-cli groups|dlq|federation|list` | [`docs/sre.md`](sre.md) |
| **Application developer** | publish and consume from apps | `/ui/` (read-only, useful) | `firn-cli produce|fetch|groups|list|dlq` | [`docs/developer.md`](developer.md) |
| **Security auditor** | prove keys, audit trail, hardening | `/metrics`, `/ui/` | `firn-cli list-apikeys|revoke-apikey|federation status` | [`docs/adr/011-secure-mode.md`](adr/011-secure-mode.md) |

Extended personas (unofficial but expected):

| Persona | When they appear | What they do |
|---|---|---|
| **Data analyst** | occasional ad-hoc reads | gets a `viewer` key; `fetch` with `--json` piped into their notebook |
| **Audit/compliance** | quarterly review | reads `__audit-0` via `firn-cli` + `awk`/`python`; checks allowlists on production keys |
| **Incident commander** | during an outage | read-only `firn-cli groups|status|dlq list` + `/ui/` — never takes write actions mid-incident without a second pair of eyes |
| **Release manager** | cutting binaries | `make build`, signs tarballs, publishes SHA256SUMS; no production keys at all |

Notes:

- **Nobody gets `admin` unless they need to mint or revoke keys.** That's deliberately tiny: operators rotate keys; everyone else is `viewer` or `operator`.
- **No UI action ever mutates state.** Every mutating call goes through the API surface, which means it's in the audit trail. This is the whole compliance story.
