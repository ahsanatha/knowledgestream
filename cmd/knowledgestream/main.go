package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ahsanknowledge/knowledgestream/internal/fanout"
	"github.com/ahsanknowledge/knowledgestream/internal/gateway"
	"github.com/ahsanknowledge/knowledgestream/internal/ingest"
	"github.com/ahsanknowledge/knowledgestream/internal/normalize"
	"github.com/ahsanknowledge/knowledgestream/internal/ranking"
	"github.com/ahsanknowledge/knowledgestream/internal/volatility"
)

func main() {
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	sseURL := env("SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange,page-create,page-links-change")
	addr := env("LISTEN_ADDR", ":8080")

	rankStore, err := ranking.NewStore(redisAddr)
	if err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}
	defer rankStore.Close()

	eng := volatility.NewEngine(7, rankStore)
	fr := fanout.NewRouter()
	gw := gateway.NewGateway(fr)
	norm := normalize.NewNormalizer()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	changes := make(chan normalize.KnowledgeChange, 10000)

	go func() {
		for {
			if err := ingest.WikimediaStream(ctx, sseURL, norm, changes); err != nil {
				if ctx.Err() != nil {
					return // clean shutdown
				}
				log.Printf("ingest error: %v — reconnecting in 5s", err)
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	go func() {
		for change := range changes {
			go func(c normalize.KnowledgeChange) {
				score := eng.Process(c)
				fr.Route(fanout.RouteMessage{Change: c, Score: score})
				gw.Broadcast(c, score.Score, score.WikiCount)
			}(change)
		}
	}()

	go eng.Maintenance(ctx, 1*time.Minute)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", gw.HandleWebSocket)
	mux.HandleFunc("/api/rankings", rankStore.HandleRankings)
	mux.HandleFunc("/api/topics/", rankStore.HandleTopicStats)
	mux.HandleFunc("/api/topics", eng.HandleListTopics)
	mux.HandleFunc("/api/stats", eng.HandleStats)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{Addr: addr, Handler: corsMiddleware(mux)}

	go func() {
		log.Printf("KnowledgeStream listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	srv.Shutdown(context.Background())
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
