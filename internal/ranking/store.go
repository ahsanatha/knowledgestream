package ranking

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

const rankingsKey = "volatility:rankings"

type TopicRank struct {
	Title         string  `json:"title"`
	Wiki          string  `json:"wiki"`
	EditCount     int64   `json:"edit_count"`
	UniqueEditors int64   `json:"unique_editors"`
	Magnitude     int64   `json:"magnitude"` // gross bytes
	NetBytes      int64   `json:"net_bytes"`
	Reverts       int64   `json:"reverts"`
	Churn         float64 `json:"churn"`
	Score         float64 `json:"score"`
	WikiCount     int64   `json:"wiki_count"`
}

type Store struct {
	rdb *redis.Client
}

func NewStore(addr string) (*Store, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis connect: %w", err)
	}
	return &Store{rdb: rdb}, nil
}

func (s *Store) Close() error {
	return s.rdb.Close()
}

func (s *Store) UpdateRanking(title, wiki string, edits, editors, magnitude, netBytes, reverts int64, churn, score float64, wikiCount int64) {
	ctx := context.Background()
	data := TopicRank{
		Title:         title,
		Wiki:          wiki,
		EditCount:     edits,
		UniqueEditors: editors,
		Magnitude:     magnitude,
		NetBytes:      netBytes,
		Reverts:       reverts,
		Churn:         churn,
		Score:         score,
		WikiCount:     wikiCount,
	}
	payload, _ := json.Marshal(data)

	pipe := s.rdb.Pipeline()
	pipe.ZAdd(ctx, rankingsKey, redis.Z{Score: score, Member: title})
	pipe.Set(ctx, fmt.Sprintf("topic:%s", title), payload, 0)
	pipe.Exec(ctx)
}

func (s *Store) TopN(ctx context.Context, n int) ([]TopicRank, error) {
	results, err := s.rdb.ZRevRangeWithScores(ctx, rankingsKey, 0, int64(n-1)).Result()
	if err != nil {
		return nil, err
	}

	ranks := make([]TopicRank, 0, len(results))
	for _, z := range results {
		title := z.Member.(string)
		rank, err := s.GetTopic(ctx, title)
		if err != nil {
			continue
		}
		ranks = append(ranks, rank)
	}
	return ranks, nil
}

func (s *Store) GetTopic(ctx context.Context, title string) (TopicRank, error) {
	data, err := s.rdb.Get(ctx, fmt.Sprintf("topic:%s", title)).Bytes()
	if err != nil {
		return TopicRank{}, err
	}
	var rank TopicRank
	if err := json.Unmarshal(data, &rank); err != nil {
		return TopicRank{}, err
	}
	return rank, nil
}

func (s *Store) HandleRankings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	n := 10
	if nStr := r.URL.Query().Get("n"); nStr != "" {
		if parsed, err := strconv.Atoi(nStr); err == nil && parsed > 0 && parsed <= 100 {
			n = parsed
		}
	}

	ranks, err := s.TopN(r.Context(), n)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, ranks)
}

func (s *Store) HandleTopicStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	title := strings.TrimPrefix(r.URL.Path, "/api/topics/")
	title = strings.TrimSpace(title)
	if title == "" {
		http.Error(w, "missing topic title", http.StatusBadRequest)
		return
	}

	rank, err := s.GetTopic(r.Context(), title)
	if err != nil {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, rank)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
