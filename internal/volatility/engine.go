package volatility

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahsanknowledge/knowledgestream/internal/normalize"
	"github.com/ahsanknowledge/knowledgestream/internal/ranking"
	"github.com/axiomhq/hyperloglog"
)

type DayBucket struct {
	EditCount int64
	Magnitude int64 // gross churn: sum of |bytes changed|
	NetBytes  int64 // signed sum of bytes changed — net growth/shrink
	Reverts   int64 // edits that undid prior work
	Editors   *hyperloglog.Sketch
	Wikis     map[string]struct{}
}

type TopicWindow struct {
	Buckets [7]*DayBucket
	LastDay string
}

type VolatilityScore struct {
	Title         string  `json:"title"`
	Wiki          string  `json:"wiki"`
	EditCount     int64   `json:"edit_count"`
	UniqueEditors int64   `json:"unique_editors"`
	Magnitude     int64   `json:"magnitude"`     // gross bytes: sum of |bytes changed|
	NetBytes      int64   `json:"net_bytes"`     // signed sum of bytes changed
	Reverts       int64   `json:"reverts"`
	Churn         float64 `json:"churn"`
	Score         float64 `json:"score"`
	WikiCount     int64   `json:"wiki_count"`
}

type Engine struct {
	mu              sync.RWMutex
	windows         map[string]*TopicWindow
	windowDays      int
	store           *ranking.Store
	eventsProcessed atomic.Int64
}

func NewEngine(windowDays int, store *ranking.Store) *Engine {
	return &Engine{
		windows:    make(map[string]*TopicWindow),
		windowDays: windowDays,
		store:      store,
	}
}

func (e *Engine) Process(change normalize.KnowledgeChange) VolatilityScore {
	if change.Title == "" {
		return VolatilityScore{}
	}
	e.eventsProcessed.Add(1)

	e.mu.Lock()
	defer e.mu.Unlock()

	today := change.Timestamp.Format("2006-01-02")
	tw, ok := e.windows[change.Title]
	if !ok {
		tw = &TopicWindow{}
		for i := range tw.Buckets {
			tw.Buckets[i] = &DayBucket{Editors: hyperloglog.New16(), Wikis: make(map[string]struct{})}
		}
		tw.LastDay = today
		e.windows[change.Title] = tw
	}

	if tw.LastDay != today {
		tw.rotate(today)
	}

	bucket := tw.Buckets[0]
	bucket.EditCount++
	bucket.Magnitude += abs64(change.BytesChanged)
	bucket.NetBytes += change.BytesChanged
	if change.Reverted {
		bucket.Reverts++
	}
	if change.Editor != "" {
		bucket.Editors.Insert([]byte(change.Editor))
	}
	if change.Wiki != "" {
		bucket.Wikis[change.Wiki] = struct{}{}
	}

	score := e.calculateScore(tw)
	score.Title = change.Title
	score.Wiki = change.Wiki
	e.store.UpdateRanking(
		change.Title, change.Wiki,
		score.EditCount, score.UniqueEditors, score.Magnitude, score.NetBytes, score.Reverts,
		score.Churn, score.Score, score.WikiCount,
	)
	return score
}

// Scoring tunables. Volatility is intensity × instability × recency, where
// instability is what separates contested knowledge from routine activity.
const (
	recencyTau    = 3.0 // day-decay constant; weight of day i is exp(-i/tau)
	recencyFloor  = 0.3 // an all-stale topic still keeps 30% of its score
	contentionCap = 8.0 // edits-per-editor ceiling (edit-war signature)
	churnCap      = 12.0 // gross/net ceiling (oscillation-in-place signature)
	churnFloor    = 0.5
	revertWeight  = 2.0 // reverts count extra toward raw intensity
)

// calculateScore turns the 7-day window into a single volatility number.
//
// The old score was log(edits)·log(editors)·log(magnitude) — a pure *activity*
// measure that rewarded broad collaboration and big one-shot rewrites, i.e. the
// opposite of volatility. This version rewards the actual fingerprints of
// unstable knowledge:
//
//   - reverts            — edits that exist to undo other edits
//   - contention         — many edits from few editors (a tug-of-war, not a crowd)
//   - churn              — gross bytes >> |net bytes|, i.e. text oscillating in place
//                          rather than an article simply growing
//   - recency            — activity happening now, not 6 days ago
func (e *Engine) calculateScore(tw *TopicWindow) VolatilityScore {
	var totalEdits, totalMagnitude, totalReverts, netBytes int64
	var recencyEdits float64
	mergedHLL := hyperloglog.New16()
	allWikis := make(map[string]struct{})

	for i, b := range tw.Buckets {
		if b == nil {
			continue
		}
		totalEdits += b.EditCount
		totalMagnitude += b.Magnitude
		totalReverts += b.Reverts
		netBytes += b.NetBytes
		recencyEdits += float64(b.EditCount) * math.Exp(-float64(i)/recencyTau)
		mergedHLL.Merge(b.Editors)
		for w := range b.Wikis {
			allWikis[w] = struct{}{}
		}
	}

	editors := int64(mergedHLL.Estimate())
	if editors < 1 {
		editors = 1
	}

	// intensity: how much is happening, with reverts counted extra.
	intensity := math.Log1p(float64(totalEdits) + revertWeight*float64(totalReverts))

	// contention: few editors driving many edits => tug-of-war.
	contention := clamp(float64(totalEdits)/float64(editors), 1, contentionCap)

	// churn: gross churn far exceeding net change => text oscillating in place.
	// A single +50KB rewrite has churn ~1; 25 reverts of a paragraph have churn >> 1.
	churn := clamp(float64(totalMagnitude)/(1+math.Abs(float64(netBytes))), churnFloor, churnCap)

	// recency: fraction of (weighted) edits that are recent. ~1 if all today.
	recency := recencyFloor
	if totalEdits > 0 {
		recency = recencyFloor + (1-recencyFloor)*(recencyEdits/float64(totalEdits))
	}

	// reverts are the strongest single signal, so they also multiply directly.
	revertBoost := 1 + float64(totalReverts)

	score := intensity * contention * churn * recency * revertBoost

	return VolatilityScore{
		EditCount:     totalEdits,
		UniqueEditors: int64(mergedHLL.Estimate()),
		Magnitude:     totalMagnitude,
		NetBytes:      netBytes,
		Reverts:       totalReverts,
		Churn:         churn,
		Score:         score,
		WikiCount:     int64(len(allWikis)),
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func (tw *TopicWindow) rotate(today string) {
	last := tw.Buckets[6]
	tw.Buckets[6] = nil
	for i := 6; i > 0; i-- {
		tw.Buckets[i] = tw.Buckets[i-1]
	}
	if last != nil {
		last.EditCount = 0
		last.Magnitude = 0
		last.Editors = hyperloglog.New16()
		last.Wikis = make(map[string]struct{})
		tw.Buckets[0] = last
	} else {
		tw.Buckets[0] = &DayBucket{Editors: hyperloglog.New16(), Wikis: make(map[string]struct{})}
	}
	tw.LastDay = today
}

func (e *Engine) Maintenance(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.cleanOldTopics()
		}
	}
}

func (e *Engine) cleanOldTopics() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for title, tw := range e.windows {
		var totalEdits int64
		for _, b := range tw.Buckets {
			if b != nil {
				totalEdits += b.EditCount
			}
		}
		if totalEdits == 0 {
			delete(e.windows, title)
		}
	}

	log.Printf("active topics: %d", len(e.windows))
}

func (e *Engine) GetTopicStats(title string) (*VolatilityScore, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tw, ok := e.windows[title]
	if !ok {
		return nil, fmt.Errorf("topic not found: %s", title)
	}

	score := e.calculateScore(tw)
	score.Title = title
	return &score, nil
}

func (e *Engine) ListTopics() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	topics := make([]string, 0, len(e.windows))
	for title := range e.windows {
		topics = append(topics, title)
	}
	sort.Strings(topics)
	return topics
}

func (e *Engine) HandleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := e.ListTopics()
	writeJSON(w, http.StatusOK, topics)
}

func (e *Engine) HandleStats(w http.ResponseWriter, r *http.Request) {
	e.mu.RLock()
	activeTopics := len(e.windows)
	e.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]int64{
		"events_processed": e.eventsProcessed.Load(),
		"active_topics":    int64(activeTopics),
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
