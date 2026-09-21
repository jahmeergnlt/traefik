package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestConcurrentConfigUpdates ensures that rapid, concurrent configuration
// submissions do not leave the server in a partially‑updated state.
func TestConcurrentConfigUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewServer(ctx, "127.0.0.1:0") // use an OS‑assigned port
	// Replace the real router builder with a test‑aware version.
	originalBuilder := buildRouterFromConfig
	defer func() { buildRouterFromConfig = originalBuilder }()

	var routerMux sync.Mutex
	buildRouterFromConfig = func(cfg interface{}) *Router {
		// Simulate a small build delay to increase race window.
		time.Sleep(5 * time.Millisecond)

		routerMux.Lock()
		defer routerMux.Unlock()
		// Attach the config payload as a header value for verification.
		r := newRouter()
		r.ServeHTTP = func(w http.ResponseWriter, req *http.Request) {
			if payload, ok := cfg.(string); ok {
				w.Header().Set("X-Payload", payload)
			}
			http.NotFound(w, req)
		}
		return r
	}

	// Start the server in a goroutine.
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			t.Fatalf("server start failed: %v", err)
		}
	}()

	// Wait for the server to be listening.
	time.Sleep(50 * time.Millisecond)

	// Fire a burst of concurrent configuration updates.
	const updates = 20
	var wg sync.WaitGroup
	for i := 0; i < updates; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			srv.SubmitConfig(ConfigMessage{Payload: "cfg-" + string(rune('A'+i))})
		}(i)
	}
	wg.Wait()

	// Give the server a moment to process the last update.
	time.Sleep(100 * time.Millisecond)

	// Issue a request and verify that the response carries the *latest* payload.
	// The latest payload is deterministic because the channel is FIFO.
	expectedPayload := "cfg-" + string(rune('A'+updates-1))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rr := httptest.NewRecorder()
	srv.handleRequest(rr, req)

	if got := rr.Header().Get("X-Payload"); got != expectedPayload {
		t.Fatalf("expected latest payload %q, got %q", expectedPayload, got)
	}
}
