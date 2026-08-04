package fix

import "sync"

// FixRaceCondition adds proper synchronization to prevent data races.
// This fix uses sync.RWMutex to protect concurrent access to shared state.
type SafeState struct {
    mu    sync.RWMutex
    value interface{}
}

func (s *SafeState) Get() interface{} {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.value
}

func (s *SafeState) Set(v interface{}) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.value = v
}
