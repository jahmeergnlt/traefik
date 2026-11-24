map[string]RouterConfig{
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

	// Send a continuous stream of HTTP requests to verify consistency
	var requestCount int64
	var inconsistentCount int64

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(1 * time.Millisecond)
			defer ticker.Stop()

			timeout := time.After(200 * time.Millisecond)
			for {
				select {
				case <-timeout:
					return
				case <-ticker.C:
					rec := httptest.NewRecorder()
					req := httptest.NewRequest("GET", "/test", nil)
					
					s.entryPoints["web"].ServeHTTP(rec, req)
					atomic.AddInt64(&requestCount, 1)

					body := rec.Body.String()
					header := rec.Header().Get("X-Test-Header")

					// Inconsistent states to check:
					// 1. Response says "New Router" but header is "Old" (or vice versa)
					if body == "New Router" && header != "New" {
						atomic.AddInt64(&inconsistentCount, 1)
					}
					if body == "Old Router" && header != "Old" {
						atomic.AddInt64(&inconsistentCount, 1)
					}
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	t.Logf("Total requests: %d, Inconsistent responses: %d", atomic.LoadInt64(&requestCount), atomic.LoadInt64(&inconsistentCount))
	if atomic.LoadInt64(&inconsistentCount) > 0 {
		t.Fatalf("Detected %d inconsistent responses where router configuration and middleware configuration were mismatched!", atomic.LoadInt64(&inconsistentCount))
	}
}