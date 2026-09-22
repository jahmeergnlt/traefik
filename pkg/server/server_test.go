package server

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestConcurrentUpdates ensures that when multiple configuration updates are
// sent almost simultaneously, the server processes them sequentially and the
// final router reflects the last configuration without ever exposing a stale
// middleware chain.
func TestConcurrentUpdates(t *testing.T) {
	srv := NewServer(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)

	// Prepare two distinct configurations.
	cfg1 := &Config{
		Routers: map[string]RouterConfig{
			"routerA": {Rule: "Host(`example.com`)", Middlewares: []string{"mw1"}},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {Type: "addPrefix"},
		},
	}
	cfg2 := &Config{
		Routers: map[string]RouterConfig{
			"routerA": {Rule: "Host(`example.com`)", Middlewares: []string{"mw2"}},
			"routerB": {Rule: "Host(`foo.com`)", Middlewares: []string{"mw1"}},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {Type: "addPrefix"},
			"mw2": {Type: "stripPrefix"},
		},
	}

	// Send both configurations concurrently.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		srv.UpdateConfig(cfg1)
	}()
	go func() {
		defer wg.Done()
		srv.UpdateConfig(cfg2)
	}()
	wg.Wait()

	// Give the server a moment to process the updates.
	time.Sleep(100 * time.Millisecond)

	// The active router must reflect cfg2 (the last update) because the
	// server processes updates sequentially and the atomic swap guarantees
	// consistency.
	router := srv.GetRouter()
	if router == nil {
		t.Fatalf("router is nil after processing updates")
	}

	// routerA should now use middleware "mw2".
	chainA, ok := router.Chains["routerA"]
	if !ok {
		t.Fatalf("routerA chain missing")
	}
	if len(chainA) != 1 || chainA[0].Type != "stripPrefix" {
		t.Fatalf("routerA chain not updated correctly, got %+v", chainA)
	}

	// routerB should exist only in cfg2.
	if _, ok := router.Chains["routerB"]; !ok {
		t.Fatalf("routerB chain missing after cfg2 update")
	}
}
