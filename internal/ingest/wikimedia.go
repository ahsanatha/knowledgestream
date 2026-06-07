package ingest

import (
	"context"
	"fmt"
	"log"

	"github.com/ahsanatha/knowledgestream/internal/normalize"
	"github.com/r3labs/sse/v2"
)

func WikimediaStream(ctx context.Context, url string, norm *normalize.Normalizer, out chan<- normalize.KnowledgeChange) error {
	client := sse.NewClient(url)
	client.Headers = map[string]string{
		"User-Agent": "KnowledgeStream/1.0 (https://github.com/ahsanatha/knowledgestream; demo)",
	}

	events := make(chan *sse.Event)
	errCh := make(chan error, 1)

	go func() {
		if err := client.SubscribeChan("", events); err != nil {
			errCh <- err
		}
	}()

	for {
		select {
		case <-ctx.Done():
			client.Unsubscribe(events)
			return ctx.Err()
		case err := <-errCh:
			return fmt.Errorf("sse error: %w", err)
		case event, ok := <-events:
			if !ok {
				return fmt.Errorf("event channel closed")
			}
			data := string(event.Data)
			change := norm.Parse(data)
			if change == nil {
				continue
			}
			select {
			case out <- *change:
			case <-ctx.Done():
				return ctx.Err()
			default:
				log.Println("WARNING: output channel full, dropping event")
			}
		}
	}
}
