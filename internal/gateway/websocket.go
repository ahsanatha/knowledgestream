package gateway

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ahsanatha/knowledgestream/internal/fanout"
	"github.com/ahsanatha/knowledgestream/internal/normalize"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type ClientMessage struct {
	Type       string   `json:"type"`
	Topics     []string `json:"topics,omitempty"`
	Langs      []string `json:"langs,omitempty"`
}

type ServerMessage struct {
	Type      string                     `json:"type"`
	Page      string                     `json:"page,omitempty"`
	Wiki      string                     `json:"wiki,omitempty"`
	Editor    string                     `json:"editor,omitempty"`
	Bytes     int64                      `json:"bytes,omitempty"`
	Score     float64                    `json:"score,omitempty"`
	WikiCount int64                      `json:"wiki_count,omitempty"`
	Change    *normalize.KnowledgeChange `json:"change,omitempty"`
}

type Client struct {
	ID    string
	conn  *websocket.Conn
	mu    sync.Mutex
	topics map[string]struct{}
}

type Gateway struct {
	router *fanout.Router
	mu     sync.RWMutex
	clients map[string]*Client
	nextID  int
}

func NewGateway(router *fanout.Router) *Gateway {
	return &Gateway{
		router:  router,
		clients: make(map[string]*Client),
	}
}

func (g *Gateway) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade error: %v", err)
		return
	}

	g.mu.Lock()
	g.nextID++
	clientID := fmt.Sprintf("client-%d", g.nextID)
	client := &Client{
		ID:     clientID,
		conn:   conn,
		topics: make(map[string]struct{}),
	}
	g.clients[clientID] = client
	g.mu.Unlock()

	log.Printf("client connected: %s", clientID)
	defer g.cleanupClient(clientID)

	go g.heartbeat(client)

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Printf("client %s read error: %v", clientID, err)
			return
		}

		var msg ClientMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("client %s bad message: %v", clientID, err)
			continue
		}

		switch msg.Type {
		case "subscribe":
			g.handleSubscribe(clientID, msg.Topics)
		case "unsubscribe":
			g.handleUnsubscribe(clientID, msg.Topics)
		default:
			log.Printf("client %s unknown message type: %s", clientID, msg.Type)
		}
	}
}

func (g *Gateway) handleSubscribe(clientID string, topics []string) {
	g.mu.RLock()
	client, ok := g.clients[clientID]
	g.mu.RUnlock()
	if !ok {
		return
	}

	push := func(msg fanout.RouteMessage) {
		change := msg.Change
		score := msg.Score.Score
		serverMsg := ServerMessage{
			Type:   "change",
			Page:   change.Title,
			Wiki:   change.Wiki,
			Editor: change.Editor,
			Bytes:  change.BytesChanged,
			Change: &change,
			Score:  score,
		}
		client.mu.Lock()
		defer client.mu.Unlock()
		if err := client.conn.WriteJSON(serverMsg); err != nil {
			log.Printf("client %s write error: %v", clientID, err)
		}
	}

	for _, topic := range topics {
		client.mu.Lock()
		if _, exists := client.topics[topic]; exists {
			client.mu.Unlock()
			continue
		}
		client.topics[topic] = struct{}{}
		client.mu.Unlock()
		g.router.Subscribe(clientID, topic, push)
	}

	log.Printf("client %s subscribed to %d topics", clientID, len(topics))
}

func (g *Gateway) handleUnsubscribe(clientID string, topics []string) {
	for _, topic := range topics {
		g.router.Unsubscribe(clientID, topic)
	}

	g.mu.RLock()
	client, ok := g.clients[clientID]
	g.mu.RUnlock()
	if ok {
		client.mu.Lock()
		for _, topic := range topics {
			delete(client.topics, topic)
		}
		client.mu.Unlock()
	}

	log.Printf("client %s unsubscribed from %v", clientID, topics)
}

func (g *Gateway) heartbeat(client *Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		client.mu.Lock()
		err := client.conn.WriteJSON(ServerMessage{Type: "heartbeat"})
		client.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (g *Gateway) Broadcast(change normalize.KnowledgeChange, score float64, wikiCount int64) {
	msg := ServerMessage{
		Type:      "change",
		Page:      change.Title,
		Wiki:      change.Wiki,
		Editor:    change.Editor,
		Bytes:     change.BytesChanged,
		Score:     score,
		WikiCount: wikiCount,
		Change:    &change,
	}

	g.mu.RLock()
	clients := make([]*Client, 0, len(g.clients))
	for _, c := range g.clients {
		clients = append(clients, c)
	}
	g.mu.RUnlock()

	if len(clients) == 0 {
		return
	}

	for _, client := range clients {
		go func(c *Client) {
			c.mu.Lock()
			defer c.mu.Unlock()
			if err := c.conn.WriteJSON(msg); err != nil {
				log.Printf("broadcast write error for %s: %v", c.ID, err)
			}
		}(client)
	}
	log.Printf("broadcast: sent change %q to %d clients", change.Title, len(clients))
}

func (g *Gateway) cleanupClient(clientID string) {
	g.router.UnsubscribeAll(clientID)
	g.mu.Lock()
	delete(g.clients, clientID)
	g.mu.Unlock()
	log.Printf("client disconnected: %s", clientID)
}
