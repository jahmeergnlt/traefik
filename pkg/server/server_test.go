package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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

	// Send a continuous stream of HTTP requests to the router
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep := s.GetEntryPoint("web")
		for i := 0; i < 1000; i++ {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()
			ep.ServeHTTP(rec, req)

			if rec.Code == http.StatusOK {
				body := rec.Body.String()
				headerVal := rec.Header().Get("X-Test-Header")

				if body == "New Router" {
					if headerVal != "New" {
						t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
					}
				} else if body == "Old Router" {
					if headerVal != "Old" {
						t.Errorf("Consistency violation: response body is %q but header is %q", body, headerVal)
					}
				} else {
					t.Errorf("Unexpected response body: %q", body)
				}
			}
			time.Sleep(500 * time.Microsecond)
		}
	}()

	// Run the test for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

func TestConcurrentGetConfigAndSwitch(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Writer goroutine pushing configs
	wg.Add(1)
	go func() {
		defer wg.Done()
		counter := 0
		for {
			select {
			case <-stopChan:
				return
			default:
				counter++
				cfg := Configuration{
					Routers: map[string]RouterConfig{
						"dynamic": {
							Path:         fmt.Sprintf("/path/%d", counter),
							ResponseText: fmt.Sprintf("Response %d", counter),
						},
					},
					Middlewares: map[string]MiddlewareConfig{
						"mw": {
							HeaderName:  "X-Counter",
							HeaderValue: fmt.Sprintf("%d", counter),
						},
					},
				}
				s.GetConfigurationChan() <- cfg
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Reader goroutines reading config concurrently
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopChan:
					return
				default:
					cfg := s.GetConfig()
					// Read from map to ensure no concurrent read/write panic
					for k := range cfg.Routers {
						_ = k
					}
					for k := range cfg.Middlewares {
						_ = k
					}
					time.Sleep(500 * time.Microsecond)
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stopChan)
	wg.Wait()
}

func TestMultipleProvidersConflictingBursts(t *testing.T) {
	s := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	var totalRequests atomic.Int64
	var totalSuccess atomic.Int64

	// 3 Providers continuously publishing conflicting updates
	for p := 1; p <= 3; p++ {
		providerID := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			pName := fmt.Sprintf("P%d", providerID)
			for {
				select {
				case <-stopChan:
					return
				default:
					cfg := Configuration{
						Routers: map[string]RouterConfig{
							"main": {
								Path:         "/api",
								Middleware:   "auth",
								ResponseText: fmt.Sprintf("Body-%s", pName),
							},
						},
						Middlewares: map[string]MiddlewareConfig{
							"auth": {
								HeaderName:  "X-Provider",
								HeaderValue: fmt.Sprintf("Header-%s", pName),
							},
						},
					}
					s.GetConfigurationChan() <- cfg
					time.Sleep(time.Duration(providerID) * time.Millisecond)
				}
			}
		}()
	}

	// 5 Client goroutines continuously firing HTTP requests
	for c := 0; c < 5; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep := s.GetEntryPoint("web")
			for {
				select {
				case <-stopChan:
					return
				default:
					req := httptest.NewRequest(http.MethodGet, "/api", nil)
					rec := httptest.NewRecorder()
					ep.ServeHTTP(rec, req)
					totalRequests.Add(1)

					if rec.Code == http.StatusOK {
						totalSuccess.Add(1)
						body := rec.Body.String()
						header := rec.Header().Get("X-Provider")

						// Strict atomicity assertion: Body-PX MUST match Header-PX
						var expectedHeader string
						switch body {
						case "Body-P1":
							expectedHeader = "Header-P1"
						case "Body-P2":
							expectedHeader = "Header-P2"
						case "Body-P3":
							expectedHeader = "Header-P3"
						default:
							t.Errorf("Unexpected body format: %s", body)
						}

						if header != expectedHeader {
							t.Errorf("Atomic sync failure! Body was %q but Header was %q (expected %q)", body, header, expectedHeader)
						}
					}
					time.Sleep(200 * time.Microsecond)
				}
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	t.Logf("Completed burst test: %d total requests, %d successful requests", totalRequests.Load(), totalSuccess.Load())
}
