package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/fiatjaf/eventstore/lmdb"
	"github.com/gzuuus/onRelay/atomic"
	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/rely"
)

var (
	db     lmdb.LMDBBackend
	buffer *atomic.AtomicCircularBuffer
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rely.HandleSignals(cancel)
	db = lmdb.LMDBBackend{Path: "./db/"}
	os.MkdirAll(db.Path, 0o755)
	if err := db.Init(); err != nil {
		panic(err)
	}

	buffer = atomic.NewAtomicCircularBuffer(500)
	relay := rely.NewRelay()
	relay.OnEvent = Save
	relay.OnReq = Query
	addr := "localhost:3334"
	log.Printf("[INFO] Running relay on %s", addr)

	if err := relay.StartAndServe(ctx, addr); err != nil {
		panic(err)
	}
}

func Save(c rely.Client, e *nostr.Event) error {
	log.Printf("[INFO] Received event (ID: %s, Kind: %d)", e.ID, e.Kind)
	ctx := context.Background()
	switch {
	case nostr.IsEphemeralKind(e.Kind):
		err := buffer.SaveEvent(ctx, e)
		if err != nil {
			log.Printf("[ERROR] storing ephemeral event: %v", err)
			return err
		}
		log.Printf("[INFO] Ephemeral event stored: %s", e.ID)
		return nil

	case nostr.IsReplaceableKind(e.Kind), nostr.IsAddressableKind(e.Kind):
		return saveReplaceableEvent(ctx, e)

	default:
		return fmt.Errorf("unhandled event kind: %d", e.Kind)
	}
}

func saveReplaceableEvent(ctx context.Context, e *nostr.Event) error {
	err := db.ReplaceEvent(ctx, e)
	if err != nil {
		log.Printf("[ERROR] saving replaceable/addressable event: %v", err)
		return err
	}
	log.Printf("[INFO] Replaceable event saved: %s", e.ID)
	return nil
}

func Query(ctx context.Context, c rely.Client, f nostr.Filters) ([]nostr.Event, error) {
	log.Printf("[INFO] Received query with filters: %v", f)
	result := make([]nostr.Event, 0)

	for _, f := range f {
		events, err := buffer.QueryEvents(ctx, f)
		if err != nil {
			log.Printf("[ERROR] querying ephemeral events: %v", err)
		} else {
			for _, event := range events {
				if event != nil {
					result = append(result, *event)
				}
			}
		}
	}
	return result, nil
}
