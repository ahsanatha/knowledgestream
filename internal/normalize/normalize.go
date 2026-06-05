package normalize

import (
	"encoding/json"
	"strings"
	"time"
)

type KnowledgeChange struct {
	Title        string    `json:"title"`
	Wiki         string    `json:"wiki"`
	Editor       string    `json:"editor"`
	BytesChanged int64     `json:"bytes_changed"`
	Timestamp    time.Time `json:"timestamp"`
	ChangeType   string    `json:"change_type"`
	ServerName   string    `json:"server_name"`
	Namespace    int       `json:"namespace"`
	Reverted     bool      `json:"reverted"`
}

type Normalizer struct{}

func NewNormalizer() *Normalizer {
	return &Normalizer{}
}

func (n *Normalizer) Parse(raw string) *KnowledgeChange {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	var msg map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		return nil
	}

	meta, ok := msg["meta"].(map[string]interface{})
	if ok {
		if domain, _ := meta["domain"].(string); domain == "canary" {
			return nil
		}
	}

	bot, _ := msg["bot"].(bool)
	if bot {
		return nil
	}

	title, _ := msg["title"].(string)
	if title == "" {
		return nil
	}

	// Exclude non-encyclopedic wikis using the raw wiki identifier
	rawWiki, _ := msg["wiki"].(string)
	switch rawWiki {
	case "wikidatawiki", "commonswiki", "metawiki", "mediawikiwiki":
		return nil
	}

	serverName, _ := msg["server_name"].(string)
	if serverName == "" {
		serverName = rawWiki
	}

	wiki := n.extractWiki(serverName)

	editor, _ := msg["user"].(string)
	if editor == "" {
		editor, _ = msg["performer"].(map[string]interface{})["user_text"].(string)
	}

	ns := 0
	if nsVal, ok := msg["namespace"].(float64); ok {
		ns = int(nsVal)
	}
	if ns == 0 {
		if nsVal, ok := msg["page_namespace"].(float64); ok {
			ns = int(nsVal)
		}
	}

	// Main articles only (namespace 0)
	if ns != 0 {
		return nil
	}

	changeType := n.detectType(msg)

	var bytesChanged int64
	if length, ok := msg["length"].(map[string]interface{}); ok {
		nv, _ := length["new"].(float64)
		ov, _ := length["old"].(float64)
		bytesChanged = int64(nv) - int64(ov)
	} else if changeType == "new" {
		bytesChanged = 1
	} else if changeType == "links" {
		bytesChanged = 1
	}

	var ts time.Time
	if dt, ok := msg["meta"].(map[string]interface{})["dt"].(string); ok {
		ts, _ = time.Parse(time.RFC3339Nano, dt)
	}
	if ts.IsZero() {
		if tsStr, ok := msg["timestamp"].(string); ok {
			ts, _ = time.Parse(time.RFC3339Nano, tsStr)
		}
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	reverted := detectRevert(msg)

	return &KnowledgeChange{
		Title:        title,
		Wiki:         wiki,
		Editor:       editor,
		BytesChanged: bytesChanged,
		Timestamp:    ts,
		ChangeType:   changeType,
		ServerName:   serverName,
		Namespace:    ns,
		Reverted:     reverted,
	}
}

// revertCommentMarkers are case-insensitive substrings that, when present in an
// edit summary, mark the edit as undoing prior work. These are the canonical
// volatility signal on Wikipedia — an edit that exists to erase another edit.
var revertCommentMarkers = []string{
	"undid revision",
	"reverted",
	"revert to",
	"rvv",
	"wp:rbi",
	"restored revision",
}

// revertTags are MediaWiki change tags applied to reverting edits.
var revertTags = map[string]struct{}{
	"mw-reverted": {},
	"mw-rollback": {},
	"mw-undo":     {},
	"mw-manual-revert": {},
}

// detectRevert inspects the edit summary and change tags for revert markers.
// A reverted edit is weighted heavily by the volatility engine because reverts —
// not raw edit volume — are what distinguish contested knowledge from routine work.
func detectRevert(msg map[string]interface{}) bool {
	comment := ""
	if c, ok := msg["comment"].(string); ok {
		comment = c
	} else if c, ok := msg["parsedcomment"].(string); ok {
		comment = c
	}
	lc := strings.ToLower(comment)
	for _, m := range revertCommentMarkers {
		if strings.Contains(lc, m) {
			return true
		}
	}
	// "rv " / "rvt " shorthand — require a word boundary so we don't match "serve", etc.
	if strings.HasPrefix(lc, "rv ") || strings.Contains(lc, " rv ") {
		return true
	}
	if tags, ok := msg["tags"].([]interface{}); ok {
		for _, t := range tags {
			if ts, ok := t.(string); ok {
				if _, hit := revertTags[ts]; hit {
					return true
				}
			}
		}
	}
	return false
}

func (n *Normalizer) extractWiki(serverName string) string {
	serverName = strings.TrimSuffix(serverName, ".org")
	serverName = strings.TrimPrefix(serverName, "www.")
	if idx := strings.Index(serverName, ".wikipedia"); idx != -1 {
		return serverName[:idx] + "wiki"
	}
	if idx := strings.Index(serverName, ".wiktionary"); idx != -1 {
		return serverName[:idx] + "wiktionary"
	}
	dotCount := strings.Count(serverName, ".")
	if dotCount >= 1 {
		parts := strings.SplitN(serverName, ".", 2)
		return parts[0]
	}
	return serverName
}

func (n *Normalizer) detectType(msg map[string]interface{}) string {
	t, _ := msg["type"].(string)
	if t != "" {
		return t
	}
	if stream, ok := msg["$schema"].(string); ok {
		switch {
		case strings.Contains(stream, "/page-create"):
			return "new"
		case strings.Contains(stream, "/page-links-change"):
			return "links"
		case strings.Contains(stream, "/recentchange"):
			return "edit"
		case strings.Contains(stream, "/page-delete"):
			return "delete"
		case strings.Contains(stream, "/page-move"):
			return "move"
		}
	}
	return "unknown"
}
