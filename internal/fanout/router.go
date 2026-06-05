package fanout

import (
	"log"
	"sync"

	"github.com/ahsanknowledge/knowledgestream/internal/normalize"
	"github.com/ahsanknowledge/knowledgestream/internal/volatility"
)

type RouteMessage struct {
	Change normalize.KnowledgeChange
	Score  volatility.VolatilityScore
}

type Router struct {
	mu  sync.RWMutex
	sub map[string]map[string]func(msg RouteMessage)
}

func NewRouter() *Router {
	return &Router{
		sub: make(map[string]map[string]func(msg RouteMessage)),
	}
}

func (r *Router) Subscribe(clientID, topic string, push func(msg RouteMessage)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.sub[topic] == nil {
		r.sub[topic] = make(map[string]func(msg RouteMessage))
	}
	r.sub[topic][clientID] = push
	log.Printf("SUB: client %s -> topic %s", clientID, topic)
}

func (r *Router) Unsubscribe(clientID, topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.sub[topic] != nil {
		delete(r.sub[topic], clientID)
		if len(r.sub[topic]) == 0 {
			delete(r.sub, topic)
		}
	}
}

func (r *Router) UnsubscribeAll(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for topic, clients := range r.sub {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(r.sub, topic)
		}
	}
}

func (r *Router) Route(msg RouteMessage) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topic := msg.Change.Title
	if clients, ok := r.sub[topic]; ok {
		for clientID, push := range clients {
			go func(cid string, fn func(RouteMessage)) {
				fn(msg)
			}(clientID, push)
		}
	}
}

func (r *Router) Stats() (int, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topics := len(r.sub)
	totalSubs := 0
	for _, clients := range r.sub {
		totalSubs += len(clients)
	}
	return topics, totalSubs
}
