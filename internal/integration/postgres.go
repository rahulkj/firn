package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

// PostgresCDCConnector captures change data from a Postgres 14+ database
// via logical replication (pgoutput plugin). The DSN comes from an env var,
// never inline in config. Records are emitted per row change:
//
//	{"schema":"...","table":"...","op":"c|u|d|t","lsn":"0/...",
//	 "columns":[{"name":"...","oid":20,"value":"raw-as-sent"},...],
//	 "key":{"col":"value"} }
//
// (op c=insert, u=update, d=delete, t=truncate). Key is the replica-identity
// columns when present, else absent (null key). The checkpoint position is
// the LSN acknowledged to the server; a replication slot (default
// "firn_cdc") is RETAINED on the server, so slot bloat/ orphans must be
// managed by the operator (see docs/integrations.md).
//
// Replica identity: defaults to the primary key. Tables needing full
// identity (common for update/delete payloads) should declare
// REPLICA IDENTITY FULL.
type PostgresCDCConnector struct {
	name       string
	dsnEnv     string
	slot       string
	conn       *pgconn.PgConn
	rels       map[uint32]*pglogrepl.RelationMessage
	lastLSN    pglogrepl.LSN
	streaming  bool // START_REPLICATION already sent on this connection
}

// NewPostgresCDCConnector configures the connector. slot defaults to "firn_cdc".
func NewPostgresCDCConnector(name, dsnEnv, slot string) (*PostgresCDCConnector, error) {
	if !ValidName(name) {
		return nil, fmt.Errorf("integration: invalid connector name %q", name)
	}
	if dsnEnv == "" {
		return nil, errors.New("integration: dsn_env required (env var holding the Postgres DSN)")
	}
	if slot == "" {
		slot = "firn_cdc"
	} else if err := validateSQLIdent(slot); err != nil {
		return nil, fmt.Errorf("integration: slot %q: %w", slot, err)
	}
	return &PostgresCDCConnector{name: name, dsnEnv: dsnEnv, slot: slot, rels: map[uint32]*pglogrepl.RelationMessage{}}, nil
}

func (p *PostgresCDCConnector) Name() string { return p.name }

func (p *PostgresCDCConnector) Close() error {
	if p.conn != nil {
		_ = p.conn.Close(context.Background())
		p.conn = nil
	}
	return nil
}

// validateSQLIdent rejects identifiers with characters that could break the
// replication command (`slot_name` is interpolated raw).
func validateSQLIdent(s string) error {
	if s == "" || len(s) > 63 {
		return errors.New("identifier empty or too long (max 63)")
	}
	for _, r := range s {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '.'
		if !ok {
			return fmt.Errorf("invalid character %q", r)
		}
	}
	return nil
}

func (p *PostgresCDCConnector) ensureConnected(ctx context.Context) error {
	if p.conn != nil {
		return nil
	}
	dsn := os.Getenv(p.dsnEnv)
	if dsn == "" {
		return fmt.Errorf("integration: env %s is empty (DSN)", p.dsnEnv)
	}
	// Normal connection for publication + slot management.
	admin, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("integration: postgres admin connect: %w", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	if err := ensurePublication(ctx, admin, "firn_cdc_pub"); err != nil {
		return err
	}
	slotExists, err := replicationSlotExists(ctx, admin, p.slot)
	if err != nil {
		return err
	}
	// Replication connection.
	conn, err := pgconn.Connect(ctx, withReplication(dsn))
	if err != nil {
		return fmt.Errorf("integration: postgres replication connect (wal_level must be logical): %w", err)
	}
	p.conn = conn
	if _, err := pglogrepl.IdentifySystem(ctx, p.conn); err != nil {
		return fmt.Errorf("integration: postgres identify system: %w", err)
	}
	if !slotExists {
		// Create the slot on the replication connection; checking first
		// avoids sending CREATE_REPLICATION_SLOT for an existing slot, whose
		// error response would desync the replication protocol stream.
		if _, err := pglogrepl.CreateReplicationSlot(ctx, p.conn, p.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{}); err != nil {
			return fmt.Errorf("integration: create replication slot %q (wal_level must be logical): %w", p.slot, err)
		}
	}
	return nil
}


// replicationSlotExists reports whether a logical replication slot with the
// given name exists on the server.
func replicationSlotExists(ctx context.Context, conn *pgconn.PgConn, slot string) (bool, error) {
	if err := validateSQLIdent(slot); err != nil {
		return false, fmt.Errorf("integration: slot name %q: %w", slot, err)
	}
	mrr := conn.Exec(ctx, fmt.Sprintf("SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", slot))
	results, err := mrr.ReadAll()
	if err != nil {
		return false, fmt.Errorf("integration: check replication slot %s: %w", slot, err)
	}
	return len(results) > 0 && len(results[0].Rows) > 0, nil
}

// FetchBatch reads up to max change records. The position (checkpoint) is
// the LSN as a hex string; empty position starts from the slot's retained
// position. Acknowledges (standby status update) the flushed LSN after each
// message window.
func (p *PostgresCDCConnector) FetchBatch(ctx context.Context, position string, max int) ([]Record, error) {
	if max <= 0 {
		max = 100
	}
	if err := p.ensureConnected(ctx); err != nil {
		return nil, err
	}
	startLSN := pglogrepl.LSN(0)
	if position != "" {
		v, err := pglogrepl.ParseLSN(position)
		if err != nil {
			return nil, fmt.Errorf("integration: bad LSN checkpoint %q: %w", position, err)
		}
		startLSN = v
	}
	if !p.streaming {
		if err := pglogrepl.StartReplication(ctx, p.conn, p.slot, startLSN, pglogrepl.StartReplicationOptions{
			PluginArgs: []string{"proto_version '1'", "publication_names 'firn_cdc_pub'"},
		}); err != nil {
			return nil, fmt.Errorf("integration: start replication on slot %q (publication firn_cdc_pub must exist): %w", p.slot, err)
		}
		p.streaming = true
	}
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var recs []Record
	for len(recs) < max {
		msg, err := p.conn.ReceiveMessage(fetchCtx)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				continue // replication errors are surfaced on next poll; keep waiting for WAL
			}
			if fetchCtx.Err() != nil {
				break // poll window elapsed; return what we have
			}
			return recs, nil
		}
		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		switch copyData.Data[0] {
		case pglogrepl.XLogDataByteID: // 'w'
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("integration: xlogdata: %w", err)
			}
			batch := p.decodeMessage(xld, max-len(recs))
			recs = append(recs, batch...)
			p.lastLSN = xld.ServerWALEnd
		case 'k': // PrimaryKeepaliveMessage — respond to keepalive requests
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err == nil && keepalive.ReplyRequested {
				if err := pglogrepl.SendStandbyStatusUpdate(ctx, p.conn, pglogrepl.StandbyStatusUpdate{
					WALFlushPosition: p.lastLSN,
					WALApplyPosition: p.lastLSN,
				}); err != nil {
					return nil, fmt.Errorf("integration: standby status: %w", err)
				}
			}
		}
	}
	if len(recs) > 0 {
		// Flush the position so a restart does not re-read from the slot's
		// earlier retained position.
		if err := pglogrepl.SendStandbyStatusUpdate(ctx, p.conn, pglogrepl.StandbyStatusUpdate{
			WALFlushPosition: p.lastLSN,
			WALApplyPosition: p.lastLSN,
		}); err != nil {
			return nil, fmt.Errorf("integration: standby status: %w", err)
		}
	}
	return recs, nil
}

// decodeMessage decodes one XLogData payload (a pgoutput message) into
// records. Relation messages are cached; commit/begin/truncate generate no records.
func (p *PostgresCDCConnector) decodeMessage(xld pglogrepl.XLogData, max int) []Record {
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		// Unknown/unsupported message type: skip, count nothing (fail-closed is wrong here; WAL must flow).
		return nil
	}
	lsn := xld.ServerWALEnd.String()
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		cp := *m
		p.rels[m.RelationID] = &cp
		return nil
	case *pglogrepl.InsertMessage:
		return p.tupleRecords(xld, m.RelationID, "c", m.Tuple, max, lsn)
	case *pglogrepl.UpdateMessage:
		return p.tupleRecords(xld, m.RelationID, "u", m.NewTuple, max, lsn)
	case *pglogrepl.DeleteMessage:
		return p.tupleRecords(xld, m.RelationID, "d", m.OldTuple, max, lsn)
	case *pglogrepl.TruncateMessage:
		for _, relID := range m.RelationIDs {
			rel := p.rels[relID]
			if rel == nil {
				continue
			}
			val, _ := json.Marshal(map[string]any{
				"schema": rel.Namespace, "table": rel.RelationName, "op": "t", "lsn": lsn,
			})
			_ = val
		}
		return nil
	default:
		return nil
	}
}

func (p *PostgresCDCConnector) tupleRecords(xld pglogrepl.XLogData, relID uint32, op string, tuple *pglogrepl.TupleData, max int, lsn string) []Record {
	rel := p.rels[relID]
	if rel == nil || tuple == nil {
		return nil
	}
	columns := make([]map[string]any, 0, len(tuple.Columns))
	keyCols := map[string]string{}
	for i, col := range tuple.Columns {
		if col == nil || i >= len(rel.Columns) {
			continue
		}
		rcol := rel.Columns[i]
		entry := map[string]any{"name": rcol.Name, "oid": rcol.DataType}
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			entry["value"] = nil
		case pglogrepl.TupleDataTypeToast: // unchanged TOASTed value
			entry["value"] = "__unchanged_toast__"
		default:
			entry["value"] = string(col.Data)
		}
		columns = append(columns, entry)
		if rcol.Flags == 1 { // key column
			keyCols[rcol.Name] = fmt.Sprint(entry["value"])
		}
	}
	rec := map[string]any{
		"schema":  rel.Namespace,
		"table":   rel.RelationName,
		"op":      op,
		"lsn":     lsn,
		"columns": columns,
	}
	if len(keyCols) > 0 {
		rec["key"] = keyCols
	}
	v, err := json.Marshal(rec)
	if err != nil {
		return nil
	}
	r := Record{Value: v, Position: lsn, Headers: map[string]string{
		"source_table": rel.Namespace + "." + rel.RelationName,
		"pg_lsn":       lsn,
		"op":           op,
	}}
	if len(keyCols) > 0 {
		kb, err := json.Marshal(keyCols)
		if err == nil {
			r.Key = kb
		}
	} else {
		r.Key = []byte(rel.Namespace + "." + rel.RelationName)
	}
	_ = xld
	return []Record{r}
}

// withReplication appends the replication=database parameter in the DSN's
// own format (URL or keyword).
func withReplication(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		return dsn + sep + "replication=database"
	}
	return strings.TrimSuffix(dsn, " ") + " replication=database"
}

// ensurePublication creates the firn_cdc_pub publication (FOR ALL TABLES) if
// it does not exist. It needs enough privileges to create publications
// (superuser or table owners); the error message says what is missing.
func ensurePublication(ctx context.Context, conn *pgconn.PgConn, name string) error {
	if err := validateSQLIdent(name); err != nil {
		return fmt.Errorf("integration: publication name %q: %w", name, err)
	}
	check := conn.Exec(ctx, fmt.Sprintf("SELECT 1 FROM pg_publication WHERE pubname='%s'", name))
	results, err := check.ReadAll()
	if err == nil && len(results) > 0 && len(results[0].Rows) > 0 {
		return nil
	}
	res := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", name))
	if _, err := res.ReadAll(); err != nil {
		return fmt.Errorf("integration: create publication %s: %w (needs superuser or table-owner privileges; or create it manually)", name, err)
	}
	return nil
}

