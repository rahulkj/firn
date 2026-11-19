package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

// enrichCache holds loaded reference indexes keyed by source file + match
// field. Entries are invalidated by mtime change, so reference-file updates
// are picked up on the next record after the file changes.
type enrichCache struct {
	mu     sync.Mutex
	byFile map[string]*enrichEntry
}

type enrichEntry struct {
	mtime int64
	index map[string]map[string]any // match value -> row doc
}

var globalEnrichCache = &enrichCache{byFile: map[string]*enrichEntry{}}

func loadEnrichIndex(path, matchField string, max int) (*enrichEntry, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("enrich: stat reference file: %w", err)
	}
	cacheKey := path + "\x00" + matchField
	globalEnrichCache.mu.Lock()
	defer globalEnrichCache.mu.Unlock()
	if e, ok := globalEnrichCache.byFile[cacheKey]; ok && e.mtime == st.ModTime().UnixNano() {
		return e, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("enrich: read reference file: %w", err)
	}
	idx := map[string]map[string]any{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			return nil, fmt.Errorf("enrich: reference file line %d is not JSON: %w", i+1, err)
		}
		v, ok := obj[matchField]
		if !ok {
			return nil, fmt.Errorf("enrich: reference file line %d missing match_field %q", i+1, matchField)
		}
		var key string
		switch x := v.(type) {
		case string:
			key = x
		default:
			key = fmt.Sprintf("%v", x)
		}
		if len(idx) >= max {
			return nil, fmt.Errorf("enrich: reference file exceeds cache_records limit %d", max)
		}
		idx[key] = obj
	}
	e := &enrichEntry{mtime: st.ModTime().UnixNano(), index: idx}
	globalEnrichCache.byFile[cacheKey] = e
	return e, nil
}

// applyEnrich joins the record to a read-only JSONL reference file by key.
// Reference rows are matched where match_field equals the record key at
// spec.KeyPath; configured fields are copied into the record.
func applyEnrich(doc map[string]any, spec *EnrichSpec) (map[string]any, error) {
	if spec == nil {
		return nil, errors.New("enrich: spec required")
	}
	cacheRecs := spec.CacheRecs
	if cacheRecs <= 0 {
		cacheRecs = 100000
	}
	entry, err := loadEnrichIndex(spec.SourceFile, spec.MatchField, cacheRecs)
	if err != nil {
		return nil, err
	}
	keyV, found := getPath(doc, spec.KeyPath)
	if !found {
		if spec.OnMiss == "dlq" {
			return nil, fmt.Errorf("enrich: key path %q missing in record", spec.KeyPath)
		}
		return doc, nil // passthrough
	}
	var keyStr string
	switch v := keyV.(type) {
	case string:
		keyStr = v
	default:
		keyStr = fmt.Sprintf("%v", v)
	}
	match := entry.index[keyStr]
	if match == nil {
		if spec.OnMiss == "dlq" {
			return nil, fmt.Errorf("enrich: no reference row for key %q", keyStr)
		}
		return doc, nil // passthrough
	}
	for refField, outField := range spec.Fields {
		if v, ok := match[refField]; ok {
			setPath(doc, outField, v)
		}
	}
	return doc, nil
}
