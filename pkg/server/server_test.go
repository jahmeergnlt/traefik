package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestConcurrentConfigurationUpdates(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	// Initial configuration: route1 uses mw1 (Old)
	initialConfig := Configuration{
		Routers: map[string]RouterConfig{
			"route1": {
				Path:         "/test",
				Middleware:   "mw1",
				ResponseText: "Old Router",
			},
		},
		Middlewares: map[string]MiddlewareConfig{
			"mw1": {
				HeaderName:  "X-Test-Header",
				HeaderValue: "Old",
			},
		},
	}
	s.GetConfigurationChan() <- initialConfig
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Provider A: updates route1 to use mw2 (New)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				configA := Configuration{
					Routers: map[string]RouterConfig{
						"route1": {
							Path:         "/test",
							Middleware:   "mw2",
							ResponseText: "New Router",
						},
					},
					Middlewares: map[string]MiddlewareConfig{
						"mw1": {
							HeaderName:  "X-Test-Header",
							HeaderValue: "Old",
						},
						"mw2": {
							HeaderName:  "X-Test-Header",
							HeaderValue: "New",
						},
					},
				}
				s.GetConfigurationChan() <- configA
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Provider B: updates an unrelated router, but keeps route1 using mw1 (Old)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				configB := Configuration{
					Routers: map[string]RouterConfig{
						"route1": {
							Path:         "/test",
							Middleware:   "mw1",
							ResponseText: "Old Router",
						},
						"unrelated": {
							Path:         "/unrelated",
							Middleware:   "",
							ResponseText: "Unrelated Router",
						},
					},
					Middlewares: map[string]MiddlewareConfig{
						"mw1": {
							HeaderName:  "X-Test-Header",
							HeaderValue: "Old",
						},
					},
				}
				s.GetConfigurationChan() <- configB
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Send a continuous stream of HTTP requests to the router to verify consistency
	var reqWg sync.WaitGroup
	reqStopChan := make(chan struct{})
	
	for i := 0; i < 10; i++ {
		reqWg.Add(1)
		go func() {
			defer reqWg.Done()
			for {
				select {
				case <-reqStopChan:
					return
				default:
					req := httptest.NewRequest(http.MethodGet, "/test", nil)
					rec := httptest.NewRecorder()
					
					s.entryPoints["web"].ServeHTTP(rec, req)

					headerVal := rec.Header().Get("X-Test-Header")
					body := rec.Body.String()

					// Consistency check: If response text is "Old Router", header must be "Old".
					// If response text is "New Router", header must be "New".
					if body == "Old Router" && headerVal != "Old" && headerVal != "" {
						t.Errorf("Race condition detected: Body is 'Old Router' but header value is %q", headerVal)
					}
					if body == "New Router" && headerVal != "New" {
						t.Errorf("Race condition detected: Body is 'New Router' but header value is %q", headerVal)
					}
					
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Let the test run concurrently for a short duration
	time.Sleep(200 * time.Millisecond)

	// Stop workers
	close(stopChan)
	wg.Wait()

	close(reqStopChan)
	reqWg.Wait()
}